
CREATE PROCEDURE [ETLProcess].[GetMappingFileProcessStatus]	
	@ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL'
,	@MappingFileName VARCHAR(100)=''
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Get mappping file name and Proess Status
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	SELECT					
		COUNT(1) AS IsActive	
	FROM
		ETLProcess.ETLProcess
	
		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId	
	WHERE
		ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1
		AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory
		AND ETLProcess.ProcessName=LEFT(@MappingFileName,CHARINDEX('_Mapping',@MappingFileName)-1)
END