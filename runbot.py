#!/bin/env python3

import os
import re
import sys
import subprocess
import uuid
import cs_db
from cs_environment import current_user_is_production
from cs_logging import logmsg, logerr
import cs_jira_requests


def is_prod_run():
    is_prod_run = False
    if os.getenv('CS_PROD') == "P" and current_user_is_production():
        is_prod_run = True
        logmsg("Production Run -- using {} server".format(os.getenv('CS_MSSQLSVR').split(".")[0].upper()))
    else:
        logmsg("Non-production run -- using {} server".format(os.getenv('CS_MSSQLSVR_DEV').split(".")[0].upper()))
    return is_prod_run


# OUTPUT: Returns the column values that runbot.py will utilize from the row {}
def get_required_row_values(row_to_execute):
    id = row_to_execute.get('id')
    jira_issue_id = row_to_execute.get('jira_issue_id')
    job_name = row_to_execute.get('job_nm')
    job_type = row_to_execute.get('job_type')
    runjob_cmd = row_to_execute.get('runjob_cmd')
    artifact_id = row_to_execute.get('artifact_id')
    log_run_id = row_to_execute.get('run_id')
    error_snippet = row_to_execute.get('error_snippit_tx')
    return id, jira_issue_id, job_name, runjob_cmd, artifact_id, log_run_id, error_snippet, job_type

# Function to get the next status_cd = 'NEW' (unrun) item in mis_reports.jobs.RUNJOB_REQUEST_T
# OUTPUT: Returns dictionary of a single row
def get_next_row_dict(runbot_id = None):
    logmsg("Accessing DB to find next open request...")
    # This sproc returns 1 row for execution and marks the row as 'QUEUED'
    sql = """
        EXEC MIS_Reports.jobs.RUNJOB_REQUEST_GET @IS_AUTOMATION = 1;
        """
    if runbot_id:
        sql = """
        EXEC MIS_Reports.jobs.RUNJOB_REQUEST_GET @RUNJOB_ID = {};
        """.format(runbot_id)
        
    rows_list = cs_db.DataBase.mssql_query(sql)
    if rows_list:
        row_to_execute = rows_list[0]
    else:
        return False
    row_to_execute =  {k.lower(): v for k, v in row_to_execute.items()}
    return row_to_execute
                                        

# OUTPUT: rc. If rc != 0, there was a failure.
def execute_runjob(runjob_cmd, jira_issue_id, id):
    if ".ctl" not in runjob_cmd:
        if current_user_is_production() and "-request" not in runjob_cmd:
            logmsg("Jira Request parameter required in prod; appending '-request {}' to runjob command".format(jira_issue_id))
            runjob_cmd += " -request {}".format(jira_issue_id)
        if "-runbot_id" not in runjob_cmd:
            logmsg("Appending '-runbot_id {}' to runjob command".format(id))
            runjob_cmd += " -runbot_id {}".format(id)
    else:
        logmsg("Request type: CTL")

    logmsg("Executing: {}".format(runjob_cmd))
    proc = subprocess.Popen(runjob_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    return proc


# NEED: 'id', 'status_cd', 'run_id'
# BEFORE THIS METHOD RUNS, 'status_cd' will always be 'QUEUED' (by previous sproc)
# OUTPUT: Boolean, depending on whether the status_cd was successfully updated
def update_status_cd_for_row(id, new_status):
    logmsg("Updating 'status_cd' in mis_reports.jobs.RUNJOB_REQUEST_T to {}".format(new_status))
    sql = """
        EXEC  MIS_Reports.jobs.RUNJOB_REQUEST_UPDATE 
        @ID = {},
        @STATUS = '{}';
        """.format(id, new_status)
    return cs_db.DataBase.mssql_update(sql)

def log_output_to_database(job_command, log_file_name, log_text, is_failure, artifact_id, runbot_id):
    logmsg("Logging output to database")
    if not log_text:
        log_text = "Nothing to log"

    log_text = log_text.replace("'", "").replace('\\n', '\n').replace('\\t', '\t')

    params = "'{}', '{}', '{}', {}, {}, {}".format(job_command, log_file_name, log_text, is_failure, artifact_id, runbot_id)    
    mod_to_call = "if (! MIS::Jobs::log_content_to_database(" + params + ")) {exit 1;} else {exit 0;}"
    rc = subprocess.call(["perl", "-mMIS::Jobs", "-e", mod_to_call])

# function to query existing, non-completed Jira release stories and update the runjob table in MSSQL
# with that stories current status.  Runbot will look for 'Ready for Implementation' to execute a request
def update_jira_status_for_outstanding_requests(jira_issue_id = None):
    # get Jira ids from the database
    if jira_issue_id == None:
        sql = """
            SELECT  JIRA_ISSUE_ID
                FROM     mis_reports.jobs.RUNJOB_REQUEST_T
            WHERE    (JIRA_STATUS_TX IS NULL OR JIRA_STATUS_TX <> 'Done')
                AND EXECUTION_END_TS IS NULL
                AND QUEUE_TS > getDate() - 7
            ORDER BY ID;
            """
        rows_list = cs_db.DataBase.mssql_query(sql)
        issues_csv = ""
        if rows_list == None:
            return
        
        for iss in rows_list:
            issues_csv += str(iss["JIRA_ISSUE_ID"]) + ","
        
        issues_csv = issues_csv[:-1]
        logmsg("Found {0} issues in update_jira_status_for_outstanding_requests".format(len(rows_list)))
    else:
        issues_csv = jira_issue_id

    # call ws for each status
    jira = cs_jira_requests.JiraRequest()
    issues = jira.query_multipe_issues(issues_csv)
    
    for issue in issues["result"]["issues"]:
        # update Jira status in runjob database
        logmsg("Updating {0} with {1}".format(issue["key"], issue["fields"]["status"]["name"]))
        try:
            sql = """
                UPDATE mis_reports.jobs.RUNJOB_REQUEST_T
                SET JIRA_STATUS_TX = '{0}'
                WHERE    JIRA_ISSUE_ID = '{1}';
                """.format(issue["fields"]["status"]["name"], issue["key"])
            mssql_resultset = cs_db.DataBase.mssql_update(sql)
        except:
            logmsg("WARNING: unable to update {0}".format(issue["key"]))
            continue  
    logmsg("mis_reports.jobs.RUNJOB_REQUEST_T Jira status complete")
    logmsg("------------------------------------------------------")
    

######################### MAIN APPLICATION #############################

#!/bin/env python3

########################################################################
# Program: runbot.py                                                   #
# Author:  Jeremy Ulfohn                                               #
# Date:    10/05/2023                                                  #
# Purpose: Automates DAIPS rerun/runjob requests in full. Can be run   #
#          manually or via Control-M job (mis_ha02_00_c).              #
########################################################################

import os
import sys
import cs_logging
from cs_logging import logmsg, logerr
import cs_jira_requests
import cs_runbot

jira = cs_jira_requests.JiraRequest()
final_status_cd = 'COMPLETE'

# Set defaults
logmsg("Execution begins")

logmsg("Updating existing Jira status in mis_reports.jobs.RUNJOB_REQUEST_T")
cs_runbot.update_jira_status_for_outstanding_requests()

is_prod_run = cs_runbot.is_prod_run()    
    
##### Step 1: Get queue from mis_reports.jobs.RUNJOB_REQUEST_T #####
row_to_execute = cs_runbot.get_next_row_dict()
if not row_to_execute:
    logmsg("========= No runjob items found =========")
    logmsg("\tMIS_Reports.jobs.RUNJOB_REQUEST_T has no outstanding rows with status_cd = 'NEW*'.")
    sys.exit(0)
    
runbot_id, jira_issue_id, job_name, runjob_cmd, artifact_id, log_run_id, error_snippet, job_type = cs_runbot.get_required_row_values(row_to_execute)

if not runjob_cmd:
    logerr("ERROR: runjob_cmd not found in DB row. Please check data and retry.")
    sys.exit(0)
    
# Check that Jira Issue ID is valid, then assign to executor
logmsg("Validating that {} is a valid Jira key...".format(jira_issue_id))
if not jira.is_valid_issue_key(jira_issue_id):
    cs_logging.logmsg("ERROR: {} is not a valid JIRA Story. Please re-run with a valid JIRA ID.".format(jira_issue_id))
    sys.exit(0)

logmsg("Assigning {0}".format(os.environ.get("WINDOWS_USERID").split(",")[0]));
jira.assign_story_to_user(jira_issue_id, os.environ.get("WINDOWS_USERID").split(",")[0])
   
# Add comment to Jira that the runjob is now executing. This will be used to determine =run duration
logmsg("Commenting BEGIN notice on {}".format(jira_issue_id))
jira.add_comment(jira_issue_id, "Runbot execution of '{0}' ({1}) begins.".format(runjob_cmd, job_name))
jira.update_story_status(jira_issue_id, "IN PROGRESS")


##### Step 2: Change DB and Jira status to 'RUNNING' #####
change_status_to_running = cs_runbot.update_status_cd_for_row(runbot_id, 'RUNNING')

##### Step 3: Execute the 'runjob_cmd' at system level #####
runjob_result = cs_runbot.execute_runjob(runjob_cmd, jira_issue_id, runbot_id)
job_output, job_errors = runjob_result.communicate()

std_out = str(runjob_result.communicate()[0])
if runjob_result.returncode != 0:
     logerr(("Command failed '{}' - please see log for details.").format(runjob_cmd))
     final_status_cd = 'ERROR'

##### Log CTM output file if executing .ctl file (not runjob command)
if "runjob" not in runjob_cmd:
    is_failure = 0 if runjob_result.returncode == 0 else 1
    cs_runbot.log_output_to_database(runjob_cmd, runjob_cmd, std_out, is_failure, 0, runbot_id)

##### Step 4: Call update sproc to update 'status_cd' to 'COMPLETE' or 'ERROR' #####
status_change_endstate = cs_runbot.update_status_cd_for_row(runbot_id, final_status_cd)

##### Getting updated runbot row to comment the log id
runjob_row = cs_runbot.get_next_row_dict(runbot_id)
runbot_id, jira_issue_id, job_name, runjob_cmd, artifact_id, log_run_id, error_snippet = cs_runbot.get_required_row_values(runjob_row)

##### Step 5: Comment on Jira story with run results #####
logmsg("Commenting END notice on {}".format(jira_issue_id))
jira.add_comment(jira_issue_id, "Runbot execution of '{0}' ({1}) ends. \nStatus: *{2}*".format(runjob_cmd, job_name, final_status_cd))

# noting log location based on job type
if job_type == "RERUN_REPORT" or job_type == "RELOAD_TABLE":
    jira.add_comment(jira_issue_id, "Log Location: https://artifacttracker.schwab.com/artifacts/summary?id={0}&tab=Logs&runId={1}&lineCount=10".format(artifact_id, log_run_id))

if job_type == "":
    jira.add_comment(jira_issue_id, "Full log file: https://gdtws.schwab.com/ReleaseIntake/RunbotQueueMonitor")

if error_snippet:
    jira.add_comment(jira_issue_id, "{{color:red}}Error Message: {}{{color}}".format(error_snippet))

if final_status_cd == "ERROR":
    jira.update_story_status(jira_issue_id, "Failed")
else:
    jira.update_story_status(jira_issue_id, "Done")

# update the RUNJOB_REQUEST_T table with the new Jira status
cs_runbot.update_jira_status_for_outstanding_requests(jira_issue_id)

sys.exit(0)    