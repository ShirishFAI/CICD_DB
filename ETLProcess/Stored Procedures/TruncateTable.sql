CREATE PROCEDURE [ETLProcess].[TruncateTable]
	@TableSchemaName VARCHAR(100)
,	@TableName VARCHAR(100)
AS
BEGIN	
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Truncate the table.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @Params NVARCHAR(500);
	DECLARE	@DynamicSQL NVARCHAR(2000);	

	SET @DynamicSQL=N'TRUNCATE TABLE '+ @TableSchemaName+N'.'+@TableName+';';
	SET @Params ='@TableSchemaName VARCHAR(100),@TableName VARCHAR(100)';
	EXECUTE sp_executesql 	@DynamicSQL,@Params,@TableSchemaName,@TableName	;

END