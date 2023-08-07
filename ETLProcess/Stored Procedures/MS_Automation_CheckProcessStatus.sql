CREATE PROCEDURE [ETLProcess].[MS_Automation_CheckProcessStatus]
	@ProcessName VARCHAR(100)=''
AS
BEGIN	
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Profisee Automation
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;


	SELECT
		COUNT(1) AS ActiveCount
	FROM
		ETLProcess.ETLProcessCategory

		INNER JOIN ETLProcess.ETLProcess
		ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId
	WHERE
		ETLProcessCategory.ProcessCategoryName='DTC_Profisee_MS_Automation'
		AND ETLProcessCategory.ActiveFlag =1
		AND ETLProcess.ActiveFlag=1
		AND ETLProcess.ProcessName=@ProcessName

END