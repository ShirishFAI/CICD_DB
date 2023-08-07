CREATE PROCEDURE [ETLProcess].[GetMappingFileListToDelete]	
	@ProcessCategory VARCHAR(100)='ALL'
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Get mappping file list to delete
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	SELECT			
		MappingFileList.SourceFileName
	FROM
		ETLProcess.ETLProcess

		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId

		INNER JOIN Stage.MappingFileList
		ON ETLProcess.ProcessName=MappingFileList.ProcessName
	WHERE
		ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1
		AND IsError=0
		AND SourceFileName IS NOT NULL
		AND  1 = ( 
					CASE WHEN @ProcessCategory='ALL' AND ETLProcessCategory.ProcessCategoryName IN('DTC_InternalSource_ETL','DTC_ExternalSource_ETL')THEN 1
						 WHEN @ProcessCategory='DTC_InternalSource_ETL' AND ETLProcessCategory.ProcessCategoryName='DTC_InternalSource_ETL' THEN 1
						 WHEN @ProcessCategory='DTC_ExternalSource_ETL' AND ETLProcessCategory.ProcessCategoryName='DTC_ExternalSource_ETL' THEN 1
						 ELSE 0
					END
			);	
			
END