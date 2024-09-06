#!/bin/env python3

########################################################################
# Program: runbot.py                                                   #
# Author:  Jeremy Ulfohn                                               #
# Date:    5 October 2023                                              #
# Purpose: Automates DAIPS rerun/runjob requests in full. Can be run   #
#          manually or via Control-M job (mis_ha02_00_c).              #
########################################################################

import os
import sys
import cs_logging
from cs_logging import logmsg, logerr, logheader, logwarning, logsuccess, print_console_note
import cs_jira_requests
import runbot_library
import cs_util

jira = cs_jira_requests.JiraRequest()
final_status_cd, is_failure = 'COMPLETE', 0
os.environ["LOGGING_MODE"], os.environ['IS_RUNBOT_RUN'] = "VERBOSE", "1"
command_type = None # to be used in creating RunbotCommand object

# Set defaults
logheader("runbot.py -> Execution begins")
is_prod_run = runbot_library.is_prod_run()
logmsg("runbot.py -> Updating existing Jira status in mis_reports.jobs.RUNJOB_REQUEST_T")
runbot_library.sync_jira_status_for_outstanding_requests()    
    
##### Step 1: Get queue from mis_reports.jobs.RUNJOB_REQUEST_T and validate jira ID #####
row_to_execute = runbot_library.get_next_row_dict()
if not row_to_execute:
    logmsg("runbot.py -> ========= NO RUNJOB ITEMS FOUND =========")
    logmsg("runbot.py -> >> MIS_Reports.jobs.RUNJOB_REQUEST_T has no outstanding rows with status_cd = 'NEW'. Exiting...")
    sys.exit(0)
    
runbot_id, jira_issue_id, job_name, runbot_cmd, artifact_id, log_run_id, error_snippet, job_type = runbot_library.get_required_row_values(row_to_execute)
is_rerun = (job_type in ['RERUN_REPORT', 'RELOAD_TABLE'])

if not runbot_cmd:
    logerr("runbot.py -> ERROR: runbot_cmd not found in DB row. Please check data and retry.")
    sys.exit(0)
    
    
# Check that Jira Issue ID is valid
logmsg("runbot.py -> Validating that {} is a valid Jira key...".format(jira_issue_id))
if not jira.is_valid_issue_key(jira_issue_id):
    cs_logging.logmsg("runbot.py -> ERROR: {} is not a valid JIRA Story. Please re-run with a valid JIRA ID.".format(jira_issue_id))
    sys.exit(0)
os.environ["WORKING_JIRA_ID"] = jira_issue_id


##### Step 2: Establish synchronous RunBot logfile on NAS #####
logmsg("runbot.py -> Establishing synchronous RunBot logfile in /NAS/mis/tmp/_runbot/")
runbot_dirname = runbot_log = "/NAS/mis/tmp/_runbot/"
cs_util.create_directory_if_not_extant(runbot_dirname)
# Create runbot_log, formatted according to request command_type
if ".ctl" in runbot_cmd:
    command_type = 'CTL'
    runbot_log += "{}_{}.log".format(runbot_cmd.split('/')[-1].split()[0][:-4], runbot_id)
elif "one_time_sql" in runbot_cmd:
    command_type = 'One-Time SQL'
    runbot_log += "{}_OneTimeSQL_{}.log".format(jira_issue_id, runbot_id)
elif "releaseselfservicereport" in runbot_cmd:
    command_type = 'Self-Service Release'
    runbot_log += "{}_NewSelfService_{}.log".format(runbot_cmd.split()[1], runbot_id)
else: # Possible values: runjob, publish.sh
    command_type = 'Publish' if "publish.sh" in runbot_cmd else 'Runjob'
    runbot_log += "{}_{}.log".format(runbot_cmd.split()[2], runbot_id)
# os.system(f"touch {runbot_log}")
with open(runbot_log, 'a+') as file:
    if file.readlines():
        file.write("\n\n#############################\n")
        file.write("### NEW RUNBOT RUN BEGINS ###\n")
        file.write("#############################\n")
os.chmod(runbot_log, 755)
os.environ['RUNBOT_LOG'] = runbot_log
logmsg("--- RUNBOT run, triggered by {}. Control-M Run: {} ---".format(jira_issue_id or "[JIRA_ID=Null]", "TRUE" if os.getenv('ESPWOB') else "FALSE"))


##### Step 3: Assign Jira story, then update Jira and DB statuses to 'IN PROGRESS' state #####
if not is_rerun:
    username = os.getenv('USER').removeprefix('ad.')
    logmsg(f"runbot.py -> Assigning {jira_issue_id} to {username}")
    jira.assign_story_to_user(jira_issue_id, username)
   
# Add comment to Jira that the runjob is now executing. This will be used to determine =run duration
logmsg(f"runbot.py -> Commenting BEGIN notice on {jira_issue_id}")
job_name_formatted = "" if not job_name else f" (Job Name: {job_name})"
jira.add_comment(jira_issue_id, "Runbot execution of '{0}'{1} begins.".format(runbot_cmd, job_name_formatted))
if not is_rerun:
    jira.update_story_status(jira_issue_id, "IN PROGRESS")

status_change_to_running = runbot_library.update_status_cd_for_row(runbot_id, 'RUNNING')
if not status_change_to_running:
    logerr("runbot.py -> Failed to update status_cd to 'RUNNING' on MSSQL")


##### Step 4: Initalize and run the RunbotCommand object #####
RunbotCommand = runbot_library.RunbotCommand(runbot_cmd, runbot_id, command_type=command_type)
RunbotCommand.process_runbot_command()
if not RunbotCommand.is_successful:
    final_status_cd, is_failure = 'ERROR', 1
        
##### Log CTM output file if command is non-runjob (e.g. CTL, OTS, Release, etc.)
if not RunbotCommand.is_runjob:
    runbot_log_contents = cs_util.get_unix_command_output(f"cat {runbot_log}")
    runbot_library.log_output_to_database(runbot_cmd, runbot_log, runbot_log_contents, is_failure, artifact_id, runbot_id)


##### Step 5: Call update sproc to update 'status_cd' to 'COMPLETE' or 'ERROR' #####
status_change_endstate = runbot_library.update_status_cd_for_row(runbot_id, final_status_cd)
if not status_change_endstate:
    logerr(f"runbot.py -> Failed to update status_cd to '{final_status_cd}' on MSSQL")

##### Getting updated runbot row to comment the log id
runjob_row = runbot_library.get_next_row_dict(runbot_id)
runbot_id, jira_issue_id, job_name, runbot_cmd, artifact_id, log_run_id, error_snippet, job_type = runbot_library.get_required_row_values(runjob_row)


##### Step 6: Comment on Jira story with run results #####
logmsg("runbot.py -> Commenting END notice on {}".format(jira_issue_id))
jira.add_comment(jira_issue_id, "Runbot execution of '{0}'{1} ends. \nStatus: *{2}*".format(runbot_cmd, job_name_formatted, final_status_cd))

# noting log location based on job_type
if is_rerun:
    jira.add_comment(jira_issue_id, f"Log Location: https://artifacttracker.schwab.com/artifacts/summary?id={artifact_id}&tab=Logs&runId={log_run_id}&lineCount=10")

if job_type == "":
    jira.add_comment(jira_issue_id, "Full logfile available at: https://gdtws.schwab.com/ReleaseIntake/RunbotQueueMonitor")

if error_snippet:
    jira.add_comment(jira_issue_id, "{{color:red}}Error Message: {}{{color}}".format(error_snippet))


##### Step 7: Update Jira status for outstanding requests if not rerun, then terminate execution #####
if not is_rerun:
    jira.update_story_status(jira_issue_id, "Failed" if is_failure else "Done")
    runbot_library.sync_jira_status_for_outstanding_requests(jira_issue_id)

finishup_msg = f"runbot.py -> Runbot execution ends. Status='{final_status_cd}'"
logsuccess(finishup_msg) if RunbotCommand.is_successful else logerr(finishup_msg)
print_console_note(f"Logfile available at: {runbot_log}")
sys.exit(0)
runbot.txt
Displaying runbot.txt.
