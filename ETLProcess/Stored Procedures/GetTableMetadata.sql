
CREATE PROC [ETLProcess].[GetTableMetadata]
(
	@TableSchema NVARCHAR(20)
,	@TableName NVARCHAR(100) 
)

AS
BEGIN

	SELECT 
		COLUMN_NAME AS [name]
	,	CASE WHEN data_type = 'varchar' THEN 'String' 
			 WHEN data_type = 'nvarchar' THEN 'String' 
			 WHEN data_type = 'char' THEN 'String' 
			 WHEN data_type = 'varbinary' THEN 'Byte[]' 
			 WHEN DATA_TYPE = 'datetime' THEN 'DateTime' 
			 WHEN data_type = 'bit' THEN 'Boolean' 
			 WHEN data_type = 'bigint' THEN 'Int64' 
			 WHEN data_type = 'money' THEN 'decimal' 
			 WHEN data_type = 'numeric' THEN 'decimal' 
			 WHEN data_type = 'smalldatetime' THEN 'DateTime' 
			 WHEN data_type = 'int' THEN 'Int32' 
			 WHEN data_type = 'xml' THEN 'string' 
			 WHEN data_type = 'tinyint' THEN 'Int16' 
			 WHEN data_type = 'uniqueidentifier' THEN 'guid' 
			 ELSE data_type 
		END AS [type] 
	FROM 
		INFORMATION_SCHEMA.COLUMNS
	WHERE 
		TABLE_SCHEMA = @TableSchema 
		AND TABLE_NAME = @TableName 
		
END