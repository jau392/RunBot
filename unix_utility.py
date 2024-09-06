""" Purpose: UNIX utility for use with such scripts as run_bulk_jobs.py """

#!/bin/env python3

########################################################################
# Name:     unix_utility.py
# Author:   Jeremy Ulfohn
# Date:     3 June 2024
# Purpose:  Library for general Unix utilities in Python
########################################################################

import os
import subprocess
import asyncio
import cs_environment as env
from cs_logging import logmsg, logwarning, logerr, print_console_note


async def run_command_async(command, results_dict={}):
    """
    INPUT: Unix command (str), results_dict (dict, optional)
           - Not intended for standalone use, only to be called by the next function run_commands_async()
           
    OUTPUT: command (same as input), failed (bool)
    """
    if command in results_dict:
        return command, 'SKIPPED'
    logmsg(f"cs_util.py -> Executing command {command} asynchronously...")
    failed = False
    proc = await asyncio.create_subprocess_shell(command,
                                                stdout=asyncio.subprocess.DEVNULL,
                                                stderr=asyncio.subprocess.DEVNULL
                                                )
    await proc.wait()
    if proc.returncode != 0:
        failed = True
        
    return command, failed
    

async def run_commands_async(commands, results_dict={}):
    """
    INPUT: Unix commands (list), results_dict (dict, optional)
           - Calls run_command_async() to execute all commands in list input at once
           - results_dict is only to be used to tell the downstream function whether to skip the command
           
    OUTPUT: List of (command, failed) tuples, output from run_command_async()
    """
    tasks = [run_command_async(command, results_dict) for command in commands]
    return await asyncio.gather(*tasks)
        

def get_unix_command_output(unix_cmd):
    """
    INPUT: unix_cmd (str), which is any type of non-void UNIX command (such as grep, find, cat, etc.)
    
    OUTPUT: result (str) of the unix_cmd
    """
    proc = subprocess.Popen(unix_cmd,
                            stdout=subprocess.PIPE,
                            shell=True
                            )
    output, err = proc.communicate()
    
    if not output:
        logerr(f"cs_util.py -> Function 'get_unix_command_output({unix_cmd})' returned None")
        if err:
            logerr(f"cs_util.py -> {err}")
        return ""
        
    return output.decode('utf-8').strip()
    

def create_directory_if_not_extant(path):
    """
    INPUT: path (str) of directory
    
    OUTPUT: None; takes input and creates the dir with 777 perms if it does not exist
    """
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), mode=0o777, exist_ok=True)
    return


def check_valid_line(line):
    """
    INPUT: line (str), Checks if line is empty or commented
    
    OUTPUT: is_valid (bool)
    """
    line = line.strip()
    if not line ^ (line.startswith('#') or line.startswith('//')):
        return False
    return True


def check_ctl_for_runjob(command, silent=False):
    """
    INPUT: command (str), silent (bool, optional)
    
    OUTPUT: either input command or runjob (str), depending on whether input contained a single runjob
    """
    if not silent:
        logmsg(f"cs_util.py -> Parsing CTL for runjob command(s): {command}")
    else:
        logmsg("cs_util.py -> Parsing CTL for runjob command(s)...")
    runjobs_list = []
    with open(command) as f:
        for line in list(map(str.strip, f.readlines())):
            if "runjob " in line and check_valid_line(line):
                logmsg(f"CTL Contains Runjob: {line}")
                runjobs_list.append(line)
                
    if len(runjobs_list) == 1:
        return runjobs_list[0]
    if len(runjobs_list) > 1:
        logwarning(f"CTL contains {len(runjobs_list)} runjobs; executing plain CTL. View log files for each runjob individually.")
    return command
    
    
def get_srg_runjob_command(job_nm):
    """
    Given job name (mis_?i??_00_c format), returns SRG runjob command since CTL does not exist for SRG

    params: job_nm (str)
    returns: runjob_cmd (str)
    """
    # If job_nm is passed in CTL format, scrub it back to expected format
    job_nm = f"mis_{job_nm.split('/')[-1].split('praa')[-1].removesuffix('.ctl')}_00_c"
    
    logmsg(f"cs_artifact -> Getting runjob cmd for {job_nm} from MIS_Reports.dbo.ARTFCT_DETAILS_VALUES_T")
    
    sql = """
        SELECT 'runjob srg ' + v.[VALUE] AS 'RUNJOB_CMD'
        FROM   MIS_Reports.dbo.ARTFCT_ATTRB_VALUE_V v
            INNER JOIN MIS_Reports.dbo.ARTFCT_DETAILS_VALUES_T pd ON pd.ARTFCT_ID = v.ARTFCT_ID AND pd.ATTRB_ID = 4
            INNER JOIN MIS_Reports.dbo.ARTFCT_DETAILS_VALUES_T j ON j.ARTFCT_ID = v.ARTFCT_ID AND j.ATTRB_ID = 9
        WHERE  ARTFCT_TYPE_CD = 'SRG' AND v.ATTRB_ID = 35 AND pd.VAL_255 = 'P' AND j.val_255 = '{}'
        """.format(job_nm)
    
    try:
        data = cs_db.DataBase.mssql_query(sql)
        if len(data) > 1:
            logwarning(f"cs_artifact -> More than 1 command returned for {job_nm}; check data and retry")
            return
        if len(data) < 1:
            raise Exception f"No command found for '{job_nm}'; either job name \
                is invalid or data entry is missing"
            return
        return data[0].get('RUNJOB_CMD')
    except Exception as e:
        logerr(f"cs_artifact.get_srg_runjob_command() -> Threw exception:\n{e}")
        return

def run_command_python(command, pipe_output=True):
    """
    INPUT: Unix command (str), pipe_output (bool, optional)
           - Variable pipe_output can toggle between piping or discarding stdout/stderr
    
    OUTPUT: failed (bool)
    """
    logmsg("cs_util.py -> Executing Python command (output " + ("piped)" if pipe_output else "directed to NULL)"))
    failed = False
    proc = subprocess.Popen(command,
                            shell=True,
                            stdout=(subprocess.PIPE if pipe_output else subprocess.DEVNULL),
                            stderr=(subprocess.PIPE if pipe_output else subprocess.DEVNULL)
                            )
    proc.wait()
    if proc.returncode != 0:
        failed = True
    return failed


def publish_to_runjob(publish_cmd):
    """
    INPUT: publish_cmd (str), which is any command such as 'publish RESQ-195'
           - Used to find the logfile for a 'publish' command
    
    OUTPUT: the runjob command corresponding to the input, or None if no CFG was found
    """
    dir_name = f"{'/NAS/mis/jobs' if env.current_user_is_production() else os.getenv('WORKING_JOBS_DIR')}/all/publish/scpt"
    pub_name, pub_id = publish_cmd.split()[1].lower().split('-')
    # Option 1: with hyphen
    possible_cfg = f"{dir_name}/{pub_name}-{pub_id}_publish.cfg"
    if os.path.exists(possible_cfg):
        return f"runjob all_publish {pub_name}-{pub_id}_publish"
    
    # Option 2: without hyphen
    possible_cfg = ''.join(possible_cfg.split('-'))
    if os.path.exists(possible_cfg):
        return f"runjob all_publish {pub_name}{pub_id}_publish"
    # If neither was found, return None
    return


def get_runjob_logfile(runjob_cmd):
    """
    INPUT: runjob_cmd (str), which is any runjob runjob_cmd
    
    OUTPUT: result (str) of the runjob_cmd
    """
    try:
        # First, check if runjob_cmd starts with "publish ??". In this case, it needs to be translated to "runjob all_publish ??_publish"
        if runjob_cmd.startswith('publish'):
            runjob_cmd = publish_to_runjob(runjob_cmd)
            if not runjob_cmd:
                raise Exception("No CFG file matching the provided report number found")
                
        # Main code
        is_srg = (runjob_cmd.split()[1] == "srg")
        identifier = runjob_cmd.split()[2]
        is_prod = env.current_machine_is_production_server()
        base_dir = "/NAS/mis/" if is_prod else "/NAS/mis/tmp/_"
        base_dir += "srg" if is_srg else "jobs/"
        
        # Prod (CS_PROD=P) vs non-prod distinction in logfile name and/or path
        if is_prod:
            if is_srg:
                get_srg_name_command = f"ls -d {base_dir}/*/ | grep {identifier}"
                srg_name = get_unix_command_output(get_srg_name_command).split('/NAS/mis/srg/')[1][:-1]
                # Wrap SRG name in single quotes, as it contains NBSP
                return f"/NAS/mis/srg/'{srg_name}'/logs/logfile.txt"
            else:
                pieces = runjob_cmd.split()[1].split('_')
                base_dir += f"{pieces[0]}/{pieces[1]}/log"
        
        else:
            if not is_srg:
                base_dir += runjob_cmd.split()[1]
        
        logfile_command = f"ls -ltr {base_dir}/{identifier}* | tail -1 " + "| awk '{print $NF}'"
        return get_unix_command_output(logfile_command)
    
    except Exception as err:
        logerr(f"cs_util.get_runjob_logfile({runjob_cmd}) -> Threw exception:\n{err}")
        return "<LOGFILE RETRIEVAL ERROR>"
