/****** Object: Procedure [jobs].[RUNJOB_REQUEST_GET]   Script Date: 3/5/2024 4:50:24 PM ******/
USE [MIS_Reports];
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE PROCEDURE [jobs].[RUNJOB_REQUEST_GET]
    @IS_AUTOMATION BIT = 0,
    @RUNJOB_ID INT = NULL
   WITH
   EXEC AS CALLER
AS
BEGIN
     IF @RUNJOB_ID IS NOT NULL AND @IS_AUTOMATION = 1
        THROW 51000,'Cannot define if the call is an automation as well as for a specific item', 1;
        
    DECLARE @id INT;
    
    SELECT * INTO
        #runjobs
    FROM 
    (SELECT j.ID AS ID,
        CASE WHEN jobid.VAL_255 IS NOT NULL THEN ttl.VAL_255 + ' (' + jobid.VAL_255 + ')' ELSE ttl.VAL_255 END TITLE_TX, 
         j.JIRA_ISSUE_ID, j.SCHWAB_ID, j.JOB_TYPE,
         per.DISPLAY_NM, per.EMAIL_TX,
         j.ARTIFACT_ID, j.JOB_NM,
         j.RUNJOB_CMD, j.START_STEP_ID,
         j.QUEUE_TS, j.EXECUTION_START_TS,
         j.EXECUTION_END_TS, 
         j.STATUS_CD,
         CASE 
            WHEN j.STATUS_CD = 'NEW' THEN 'Awaiting execution'
            WHEN j.STATUS_CD = 'RUNNING' THEN 'Running'
            WHEN j.STATUS_CD = 'COMPLETE' THEN 'Done'
            WHEN j.STATUS_CD = 'ERROR' THEN 'Failed'
            WHEN j.JIRA_STATUS_TX = 'Approval Pending' THEN 'Pending manager approval'
            WHEN j.JIRA_STATUS_TX = 'Ready to Implement' THEN 'Awaiting execution'
            ELSE j.JIRA_STATUS_TX            
         END FRIENDLY_STATUS,
         j.RUN_ID,  l.ERROR_SNIPPIT_TX,
         j.JIRA_STATUS_TX
    FROM     mis_reports.jobs.RUNJOB_REQUEST_T j
            INNER JOIN mis_data.dbo.SCHWAB_PERSON_DM per WITH(NOLOCK) On j.SCHWAB_ID = per.SCHWAB_ID
            INNER JOIN mis_reports.dbo.ARTFCT_DETAILS_VALUES_T ttl WITH(NOLOCK) ON j.ARTIFACT_ID = ttl.ARTFCT_ID AND ttl.ATTRB_ID = 36             
            LEFT OUTER JOIN mis_reports.dbo.ARTFCT_DETAILS_VALUES_T jobid WITH(NOLOCK) ON j.ARTIFACT_ID = jobid.ARTFCT_ID AND jobid.ATTRB_ID = 35
            LEFT OUTER JOIN mis_reports.jobs.BATCH_FRAMEWORK_LOG_T l WITH(NOLOCK) ON j.RUN_ID = l.RUN_ID AND l.SEQ_ID = 1
    WHERE j.ID = @RUNJOB_ID OR @RUNJOB_ID IS NULL
    AND QUEUE_TS >= getDate() - 20
    AND j.JOB_TYPE IN ('RERUN_REPORT','RELOAD_TABLE', 'ONE_TIME_SQL_IMP_DML', 'SELF_SERVICE')
    )tbl;
    
    IF @IS_AUTOMATION = 1
        BEGIN            
            -- getting the first qualifying record for runbot
            SELECT TOP 1 @id = ID FROM #runjobs
                WHERE 
                (
                    JOB_TYPE IN ('RERUN_REPORT','RELOAD_TABLE')
                    AND STATUS_CD = 'NEW'
                )                
--                OR
--                (
--                    JOB_TYPE IN ('ONE_TIME_SQL_IMP_DML', 'SELF_SERVICE')
--                    AND STATUS_CD IN ('NEW_DML', 'NEW_SELF_SERVICE')
--                    AND JIRA_STATUS_TX = 'Ready to Implement'
--                )
            ORDER BY ID;
            
            -- update first status
            UPDATE mis_reports.jobs.RUNJOB_REQUEST_T
                SET STATUS_CD = 'QUEUED'
            WHERE ID = @id;
            
            SELECT * FROM #runjobs
                WHERE ID = @id;
            RETURN;
        END
    
    -- return rows which would be a single row in the table for an automation, all rows for the status queue report
    SELECT * FROM #runjobs
        ORDER BY ID;
        
END
GO

/****** Object: Procedure [jobs].[RUNJOB_REQUEST_UPDATE]   Script Date: 3/5/2024 4:50:36 PM ******/
USE [MIS_Reports];
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE PROCEDURE [jobs].[RUNJOB_REQUEST_UPDATE]
    @ID INT,
    @STATUS VARCHAR(10) = 'RUNNING',
    @RUN_ID VARCHAR(50) = NULL
   WITH
   EXEC AS CALLER
AS
BEGIN
    IF @STATUS = 'RUNNING'
        BEGIN
            UPDATE mis_reports.jobs.RUNJOB_REQUEST_T
                SET EXECUTION_START_TS = getDate(),
                STATUS_CD =  'RUNNING'
            WHERE ID = @ID;
        END
    ELSE    
        BEGIN
        UPDATE mis_reports.jobs.RUNJOB_REQUEST_T
            SET EXECUTION_END_TS = getDate(),
                STATUS_CD = @STATUS                
            WHERE ID = @ID;
        END
END
GO
