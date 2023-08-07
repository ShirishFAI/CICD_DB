



CREATE PROCEDURE [ETLProcess].[GetInternalSourceQuery]
	@ProcessName VARCHAR(100) = NULL
AS
BEGIN
	DECLARE @LastRetrievedDateTime DATETIME;
	DECLARE @RunID INT ;
	
	SELECT
		@RunID = MAX(RunId)
	FROM
		ETLProcess.ETLProcess

		INNER JOIN ETLAudit.ETLProcess AuditProcess
		ON AuditProcess.ProcessId = ETLProcess.ProcessId
	
		INNER JOIN ETLProcess.ETLStatus
		ON ETLStatus.StatusId = AuditProcess.CurrentStatus
	WHERE
		AuditProcess.CurrentStage ='Landing'
		AND ETLStatus.Status='Completed'
		AND ETLProcess.ProcessName=@ProcessName
		
	SELECT 
		@LastRetrievedDateTime = AuditProcess.UTC_StartedAt
	FROM
		ETLProcess.ETLProcess

		INNER JOIN ETLAudit.ETLProcess AuditProcess
		ON AuditProcess.ProcessId = ETLProcess.ProcessId

		INNER JOIN ETLProcess.ETLStatus
		ON ETLStatus.StatusId = AuditProcess.CurrentStatus
	WHERE
		AuditProcess.RunId=@RunID
		AND AuditProcess.CurrentStage ='Landing'
		AND ETLStatus.Status='Completed'
		AND ETLProcess.ProcessName=@ProcessName;		

	SELECT
		Process.ProcessName
	,	Process.ProcessId
	,	Process.ProcessCategoryId
	,	ETLSourceQueries.SourceQuery
	,	ISNULL(@LastRetrievedDateTime,'1900-01-01') AS LastRetrievedDateTime
	,	ETLSourceDetails.DatabaseName AS SourceDBName
	,	ETLSourceDetails.ServerIP AS ServerName
	,	ETLSourceDetails.UserName
	--,	CASE Process.ProcessName
	--		WHEN   'InternalCUC' THEN 'CUC_DF'
	--		WHEN   'InternaliAVM' THEN 'ValuationService_DF'
	--		WHEN   'InternalLLC' THEN 'LLC_DF'
	--		WHEN   'InternalMMS' THEN 'MMS_DF'
	--		WHEN   'InternalMotion' THEN 'Motion_DF'
	--		WHEN   'InternalPVII' THEN 'VII_DF'
	--		WHEN   'InternalTIME' THEN 'TIME_DF'
	--	END AS SourceDBName
	FROM
		ETLProcess.ETLProcess Process

		INNER JOIN ETLProcess.ETLProcessCategory
		ON Process.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId

		INNER JOIN ETLProcess.ETLSourceQueries
		ON ETLSourceQueries.ProcessId = Process.ProcessId	

		INNER JOIN ETLProcess.ETLSourceDetails
		ON ETLSourceDetails.ProcessName = Process.ProcessName
	WHERE
		ETLProcessCategory.ActiveFlag=1
		AND Process.ActiveFlag=1
		AND Process.ProcessName=@ProcessName
		AND ETLProcessCategory.ProcessCategoryName='DTC_InternalSource_ETL'
			
END