


CREATE PROCEDURE [ETLProcess].[GetTableSchemaMapping]
(
	@TableSchema NVARCHAR(20), 
	@TableName   NVARCHAR(100)
)
AS
	BEGIN
        DECLARE @sql NVARCHAR(MAX)= '';

        SET @sql = @sql + '{"type":"TabularTranslator","columnMappings":{"';
        
		SELECT 
			@sql = @sql + COLUMN_NAME + '":"' + COLUMN_NAME + '", "'
        FROM 
			INFORMATION_SCHEMA.COLUMNS
        WHERE 
			TABLE_SCHEMA = @TableSchema
            AND TABLE_NAME = @TableName

		SET @sql = LEFT(@sql, LEN(@sql) - 3) + '}}';
        SELECT @SQL AS Result;
            
    END;