





CREATE PROCEDURE [ETLProcess].[AuditLog]
	@ProcessCategory VARCHAR(100)=''
,	@Phase VARCHAR(100)
,	@ProcessName VARCHAR(100) = ''
,	@Stage VARCHAR(100)=''
,	@Status VARCHAR(20)
,	@CurrentStatus VARCHAR(100) = ''
,	@Inserts INT = 0
,	@Updates INT = 0
,	@Deletes INT = 0

AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Captures Audit Logs.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @ProcessCheck INT;
	DECLARE @StgDt DATE;
	DECLARE @ErrorMsg VARCHAR(100);
	DECLARE @RunId INT;
	DECLARE @CategoryCheck INT;	
	
	DECLARE @ProcessCategoryID INT ;
	DECLARE @StatusID INT ;
	DECLARE @ProcessID INT ;		
	DECLARE @CurrentStatusID INT ;

	SELECT @ProcessCategoryID =ProcessCategoryId FROM ETLProcess.ETLProcessCategory WHERE ProcessCategoryName=@ProcessCategory;
	SELECT @StatusID =StatusId FROM ETLProcess.ETLStatus WHERE Status=@Status;


	IF @Phase = 'ProcessCategory' 
	BEGIN
		IF @Status='InProgress'
			BEGIN
				SELECT
					@CategoryCheck = COUNT(1)
				FROM
					ETLAudit.ETLProcessCategory AuditProcessCategory

					INNER JOIN ETLProcess.ETLProcessCategory 
					ON ETLProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId

					INNER JOIN ETLProcess.ETLStatus
					ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus
				WHERE
					ETLStatus.Status <> 'Completed'
					AND ETLProcessCategory.ProcessCategoryId = @ProcessCategoryID

				IF @CategoryCheck = 0
					BEGIN
						SELECT @StatusID =StatusId FROM ETLProcess.ETLStatus WHERE Status='InProgress';

						INSERT INTO ETLAudit.ETLProcessCategory
						(
							ProcessCategoryId
						,	UTC_StartedAt
						,	UTC_CompletedAt
						,	CurrentStatus
						)					
						VALUES
						(
							@ProcessCategoryID
						,	GETUTCDATE()
						,	GETUTCDATE()
						,	@StatusID		
						)
					END
				ELSE
					BEGIN
						SET @ErrorMsg = 'Previous '+@ProcessCategory+' Load Not completed';
							RAISERROR(@ErrorMsg, 16, 1);
						--EXEC ETL.ETLMasterDataNotification @ProcessName = @ProcessName
					END	
			END
		ELSE
			--IF @Status='Completed'
			--BEGIN
				SELECT @StatusID =StatusId FROM ETLProcess.ETLStatus WHERE Status='Completed';

				SELECT
					@RunId = MAX(RunId)
				FROM
					ETLAudit.ETLProcessCategory AS AuditProcessCategory

					INNER JOIN ETLProcess.ETLProcessCategory 
					ON AuditProcessCategory.ProcessCategoryId=ETLProcessCategory.ProcessCategoryId

					INNER JOIN ETLProcess.ETLStatus
					ON ETLStatus.StatusId=AuditProcessCategory.CurrentStatus
				WHERE
					ETLProcessCategory.ProcessCategoryId=@ProcessCategoryID
					AND ETLStatus.Status = 'InProgress'
								
				UPDATE
					AuditProcessCategory
				SET
					CurrentStatus=@StatusID
				,	UTC_CompletedAt = GETUTCDATE()
				FROM
					ETLAudit.ETLProcessCategory AS AuditProcessCategory
				WHERE
					AuditProcessCategory.RunId = @RunId


				--EXEC ETL.ETLMasterDataNotification	
				--	@ProcessName  = @ProcessCategory
				--,	@IsError = 'No'
				--,	@ProcessType ='PreProcessing'
			--END
			
	END

	IF @Phase = 'Process'
	BEGIN
		SELECT @ProcessID =ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName=@ProcessName				
		SELECT @CurrentStatusID =StatusId FROM ETLProcess.ETLStatus WHERE Status=@CurrentStatus
		
		SELECT
			@RunId = MAX(RunId)
		FROM
			ETLAudit.ETLProcessCategory AS AuditProcessCategory

			INNER JOIN ETLProcess.ETLProcessCategory 
			ON AuditProcessCategory.ProcessCategoryId=ETLProcessCategory.ProcessCategoryId

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId=AuditProcessCategory.CurrentStatus
		WHERE
			ETLProcessCategory.ProcessCategoryId=@ProcessCategoryID
			AND ETLStatus.Status = 'InProgress'
		
		SELECT
			@ProcessCheck = COUNT(1)
		FROM
			ETLAudit.ETLProcess AuditProcess

			INNER JOIN ETLProcess.ETLProcess
			ON ETLProcess.ProcessId = AuditProcess.ProcessId			
		WHERE
			ETLProcess.ProcessId = @ProcessID
			AND AuditProcess.RunId = @RunId
			AND CurrentStage =@Stage
			
		IF @ProcessCheck = 0 AND ISNULL(@RunId,0)>0
			BEGIN
				
				--Process Audit
				INSERT INTO ETLAudit.ETLProcess
				(
					RunId
				,	ProcessCategoryId
				,	ProcessId
				,	UTC_StartedAt
				,	UTC_CompletedAt
				,	CurrentStatus
				,	CurrentStage
				)
				VALUES
				(
					@RunId
				,	@ProcessCategoryID
				,	@ProcessID
				,	GETUTCDATE()
				,	GETUTCDATE()
				,	@StatusID
				,	@Stage
				)

				--History
				INSERT INTO ETLAudit.ETLProcessHistory
				(
					RunId
				,	ProcessCategoryId
				,	ProcessId
				,	Inserted
				,	Updated
				,	Deleted
				,	UTC_StartedAt
				,	UTC_CompletedAt
				,	CurrentStatus
				,	Stage
				)
				SELECT
					@RunID
				,	@ProcessCategoryId
				,	@ProcessId
				,	NULL
				,	NULL
				,	NULL
				,	GETUTCDATE()
				,	GETUTCDATE()
				,	@CurrentStatusID
				,	@Stage		;	
			END
		ELSE
			BEGIN
				UPDATE
					AuditProcess
				SET
					CurrentStatus=@CurrentStatusID
				,	UTC_CompletedAt = GETUTCDATE()
				FROM
					ETLAudit.ETLProcess AuditProcess
				WHERE
					AuditProcess.RunId=@RunId
					AND AuditProcess.ProcessId=@ProcessID
					AND AuditProcess.CurrentStage = @Stage;

				--History
				INSERT INTO ETLAudit.ETLProcessHistory
				(
					RunId
				,	ProcessCategoryId
				,	ProcessId
				,	Inserted
				,	Updated
				,	Deleted
				,	UTC_StartedAt
				,	UTC_CompletedAt
				,	CurrentStatus
				,	Stage
				)
				SELECT 
					@RunId
				,	@ProcessCategoryID
				,	@ProcessID
				,	@Inserts
				,	@Updates
				,	@Deletes
				,	GETUTCDATE()
				,	GETUTCDATE()
				,	@CurrentStatusID
				,	@Stage			;	
			END		
	END

	IF @Phase = 'ProcessHistory'
		BEGIN
			SELECT @ProcessID =ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName=@ProcessName				
			SELECT @CurrentStatusID =StatusId FROM ETLProcess.ETLStatus WHERE Status=@CurrentStatus
		
			SELECT
				@RunId = MAX(RunId)
			FROM
				ETLAudit.ETLProcessCategory AS AuditProcessCategory

				INNER JOIN ETLProcess.ETLProcessCategory 
				ON AuditProcessCategory.ProcessCategoryId=ETLProcessCategory.ProcessCategoryId

				INNER JOIN ETLProcess.ETLStatus
				ON ETLStatus.StatusId=AuditProcessCategory.CurrentStatus
			WHERE
				ETLProcessCategory.ProcessCategoryId=@ProcessCategoryID
				AND ETLStatus.Status = 'InProgress'
		
			--History
			INSERT INTO ETLAudit.ETLProcessHistory
			(
				RunId
			,	ProcessCategoryId
			,	ProcessId
			,	Inserted
			,	Updated
			,	Deleted
			,	UTC_StartedAt
			,	UTC_CompletedAt
			,	CurrentStatus
			,	Stage
			)
			SELECT
				@RunID
			,	@ProcessCategoryId
			,	@ProcessId
			,	@Inserts
			,	@Updates
			,	@Deletes
			,	GETUTCDATE()
			,	GETUTCDATE()
			,	@CurrentStatusID
			,	@Stage		;	
		END		
END