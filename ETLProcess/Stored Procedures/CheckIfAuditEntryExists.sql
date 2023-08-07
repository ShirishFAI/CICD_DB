


CREATE PROCEDURE [ETLProcess].[CheckIfAuditEntryExists]
	@ProcessCategoryName VARCHAR(100)
,	@ProcessName VARCHAR(100)
,	@Stage VARCHAR(100)=''
AS
BEGIN	
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Check and return if Audit Entry Exists for the Process.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @RunId  INT;
	DECLARE	@IsAuditEntryExists INT;
	DECLARE	@Status VARCHAR(100);
	
	SELECT
		@RunId = AuditProcessCategory.RunId
	FROM
		ETLProcess.ETLProcessCategory ProcessCategory

		INNER JOIN ETLAudit.ETLProcessCategory AuditProcessCategory
		ON ProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId		

		INNER JOIN ETLProcess.ETLStatus
		ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus
	WHERE
		ETLStatus.Status NOT IN('Completed','Error')
		AND ProcessCategory.ProcessCategoryName=@ProcessCategoryName;
	
	
	SELECT
		@IsAuditEntryExists= COUNT(1) ,@Status= ETLStatus.Status						
	FROM 
		ETLProcess.ETLProcessCategory 
	
		INNER JOIN 	ETLProcess.ETLProcess 
		ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId

		LEFT JOIN 	ETLAudit.ETLProcess AuditProcess
		ON ETLProcess.ProcessId = AuditProcess.ProcessId

		INNER JOIN ETLProcess.ETLStatus
		ON ETLStatus.StatusId = AuditProcess.CurrentStatus
	WHERE 
		ETLProcess.ProcessName=@ProcessName		
		AND AuditProcess.RunId=@RunId
	GROUP BY
		ETLStatus.Status	;						
				
	SELECT 
		ISNULL(@IsAuditEntryExists,0) AS IsAuditEntryExists
	,	@Status As Status	
	,	@ProcessName as ProcessName
	,	@RunId as RunId;
END