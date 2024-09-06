""" Purpose: Library Functions for RunBot.py """
#!/bin/env python3

import os
import re
import sys
import subprocess
import uuid
import fileinput
import shutil

import cs_db
import cs_jira_requests
import unix_utility
from cs_environment import current_user_is_production, get_mssql_instance, is_full_production_run
from cs_logging import logmsg, logerr, logwarning, logsuccess

script_arrow = str(os.path.basename(__file__)) + " ->"

class RunbotCommand:
    """
    RunbotCommand object defaults to is_runjob=True, is_successful=None
    """
    def __init__(self, command, runbot_id, command_type=None) -> None:
        self.command = command
        self.runbot_id = runbot_id
        self.type = command_type
        self.jira_id = os.getenv('WORKING_JIRA_ID')
        self.is_runjob = True if "runjob" in self.command else False
        self.is_successful = None
        
        
    def __execute_cmd(self):
        logmsg(f"{script_arrow} Executing: {self.command}")
        # run_command_python() returns True if there was a failure
        if unix_utility.run_command_python(self.command, pipe_output=False):
            self.is_successful = False
            if not self.is_runjob:
                # If CTL, log what specific command failed. Commands located in runjobs_list[]
                logerr(f"{script_arrow} Execution failed on 1 or more steps; check {os.getenv('RUNBOT_LOG')} for details")
            else:
                logerr(f"{script_arrow} Runjob failed: {self.command}")
        else:
            self.is_successful = True
            logsuccess(f"{script_arrow} Completed execution of '{self.command}'")
    
    
    def __format_runjob_cmd(self, run=False):
        """
        INPUT: self.runjob_cmd (str), run (boolean, optional)
        Function: formats runjob command and optionally executes the command
        """
        if current_user_is_production() and "-request" not in self.command:
            if self.is_runjob:
                logmsg(f"{script_arrow} Jira Request parameter required in prod; appending '-request {self.jira_id}' to runjob command")
            self.command += f" -request {self.jira_id}"
        if "-runbot_id" not in self.command:
            if self.is_runjob:
                logmsg(f"{script_arrow} Appending '-runbot_id {self.runbot_id}' to runjob command")
            self.command += f" -runbot_id {self.runbot_id}"
        
        # Optional parameter [run] will be set if is_runjob=True
        if run:
            self.__execute_cmd()
    
    
    def __process_non_runjob(self):
        if ".ctl" in self.command:
            self.type = "CTL"
            ctl = self.command.split()[0]
            logmsg(f"{script_arrow} Request type: CTL. Scrubbing for runjob commands...")
            scrubbed_ctl = "/NAS/mis/tmp/scrubbed_" + ctl.split('/')[-1]
            shutil.copyfile(ctl, scrubbed_ctl)
            os.chmod(scrubbed_ctl, 755)
            for line in fileinput.input(scrubbed_ctl, inplace=True):
                line = line.strip()
                if "runjob" in line and not line.startswith('#'):
                    self.command = line # Set command attribute to current line
                    self.__format_runjob_cmd()
                    print(self.command)
                else:
                    print(line)

            logmsg(f"{script_arrow} Scrubbed CTL Contents: ")
            with open(scrubbed_ctl, 'r') as f:
                for l in f.readlines():
                    print(l)
            
            self.command = scrubbed_ctl # Set command equal to new scrubbed CTL file
        self.__execute_cmd()
    
    
    def process_runbot_command(self):
        """
        MAIN METHOD - this public method calls every other method within class RunbotCommand
        Purpose: Scrubs and executes the command, based on whether the command is a runjob or not
        """
        if self.is_runjob:
            self.__format_runjob_cmd(run=True)
        else:
            self.__process_non_runjob()


def is_prod_run():
    is_prod = is_full_production_run()
    logmsg(f"{script_arrow} {'Production' if is_prod else 'Non-production'} run -- using {get_mssql_instance()} server")
    return is_prod


def get_required_row_values(row_to_execute):
    """
    Returns the column values that runbot.py will utilize from the row {}
    """
    id = row_to_execute.get('id')
    jira_issue_id = row_to_execute.get('jira_issue_id')
    job_name = row_to_execute.get('job_nm')
    job_type = row_to_execute.get('job_type')
    runjob_cmd = row_to_execute.get('runjob_cmd')
    artifact_id = row_to_execute.get('artifact_id')
    log_run_id = row_to_execute.get('run_id')
    error_snippet = row_to_execute.get('error_snippit_tx')
    return id, jira_issue_id, job_name, runjob_cmd, artifact_id, log_run_id, error_snippet, job_type


def get_next_row_dict(runbot_id=None):
    """
    Function to get the next status_cd = 'NEW' (unrun) item in mis_reports.jobs.RUNJOB_REQUEST_T
    OUTPUT: Returns dictionary of a single row
    """
    logmsg(f"{script_arrow} Accessing DB to find next open request...")
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
      
            
def update_status_cd_for_row(id, new_status):
    """
    NOTE: Before this method runs, 'status_cd' will always be 'QUEUED' (by previous sproc)
    INPUT: 'id', 'status_cd', 'run_id'
    OUTPUT: is_success (Boolean)
    """
    logmsg(f"{script_arrow} Updating 'status_cd' in mis_reports.jobs.RUNJOB_REQUEST_T to {new_status}")
    sql = """
        EXEC  MIS_Reports.jobs.RUNJOB_REQUEST_UPDATE 
        @ID = {},
        @STATUS = '{}';
        """.format(id, new_status)
    return cs_db.DataBase.mssql_update(sql)


def log_output_to_database(job_command, log_file_name, log_text, is_failure, artifact_id, runbot_id):
    logmsg(f"{script_arrow} Logging output to database")
    if not log_text:
        log_text = "Nothing to log"

    log_text = log_text.replace("'", "").replace('\\n', '\n').replace('\\t', '\t')

    params = "'{}', '{}', '{}', {}, {}, {}".format(job_command, log_file_name, log_text, is_failure, artifact_id, runbot_id)    
    mod_to_call = "if (! MIS::Jobs::log_content_to_database(" + params + ")) {exit 1;} else {exit 0;}"
    rc = subprocess.call(["perl", "-mMIS::Jobs", "-e", mod_to_call])


def sync_jira_status_for_outstanding_requests(jira_issue_id=None):
    """
    Function to query existing, non-completed Jira release stories and update the runjob table in MSSQL
    with those stories' current status from Jira.  Runbot will look for 'Ready for Implementation' to execute a request
    """
    # Get all pending Jira IDs (and associated statuses) from MSSQL, if jira_issue_id is blank
    if jira_issue_id is None:
        sql = """
            SELECT  JIRA_ISSUE_ID, JIRA_STATUS_TX
                FROM     mis_reports.jobs.RUNJOB_REQUEST_T
            WHERE    (JIRA_STATUS_TX IS NULL OR 
                JIRA_STATUS_TX NOT IN('Done','Done - With Issues','Failed','Cancelled'))
                AND EXECUTION_END_TS IS NULL
                AND QUEUE_TS > getDate() - 7
            ORDER BY ID;
            """
        rows_list = cs_db.DataBase.mssql_query(sql)
        if rows_list is None:
            logmsg(f"{script_arrow} No non-complete Jira stories found in DB; skipping status syncup")
            return
        issues_csv, status_list = "", []
        
        for iss in rows_list:
            issues_csv += iss.get('JIRA_ISSUE_ID') + ","
            status_list.append(iss.get('JIRA_STATUS_TX'))
        issues_csv = issues_csv[:-1]
        logmsg(f"{script_arrow} Found {len(rows_list)} issues in sync_jira_status_for_outstanding_requests()")
    else:
        logmsg(f"{script_arrow} Executing Jira-DB syncup for current issue ID...")
        issues_csv = jira_issue_id

    # Call Jira WebService for each status
    jira = cs_jira_requests.JiraRequest()
    jira_issues = jira.query_multiple_issues(issues_csv)
    
    for ind, issue in enumerate(jira_issues["result"]["issues"]):
        # If DB status already matches status from Jira, skip this issue
        if issue["fields"]["status"]["name"] == status_list[ind]:
            continue
        # Update Jira status in runjob database
        logmsg(f"{script_arrow} Syncing mis_reports.jobs.RUNJOB_REQUEST_T entry for {issue['key']} with current status from Jira: '{issue['fields']['status']['name']}'")
        try:
            sql = """
                UPDATE mis_reports.jobs.RUNJOB_REQUEST_T
                SET JIRA_STATUS_TX = '{0}'
                WHERE    JIRA_ISSUE_ID = '{1}';
                """.format(issue["fields"]["status"]["name"], issue["key"])
            mssql_resultset = cs_db.DataBase.mssql_update(sql)
        except Exception as e:
            logwarning(f"{script_arrow} Unable to update status for {issue['key']}. Exception:")
            logwarning(f"{e.message} {e.args}", skip_format=True)
            continue  
    logmsg(f"{script_arrow} mis_reports.jobs.RUNJOB_REQUEST_T Jira status syncup complete")
    logmsg("-" * 54)
cs_runbot.txt
Displaying runbot.txt.
