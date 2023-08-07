
CREATE PROCEDURE [ETLProcess].[UpdateAddressBeforMADPush]	
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Mark the invalids before processing
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @EntityName varchar(200) ='dbo.Address';
	DECLARE @strSQL NVARCHAR(max) = N'' ; 
	DECLARE @CASE NVARCHAR(MAX) = N'';
	DECLARE	@SelectClause NVARCHAR(MAX)=N'';			
	DECLARE	@InsertClause NVARCHAR(MAX)=N'';
	DECLARE @DeleteSQL NVARCHAR(MAX)=N'';
	DECLARE @CleansingRule Varchar(500);
	DECLARE @LastRetrievedDateTime DATETIME;

	DECLARE @ProcessCategory VARCHAR(100)='DTC_MasterAddress_ETL';
	DECLARE @ProcessName VARCHAR(100)='PushToMAD';	
	DECLARE @ErrorProcedure VARCHAR(100);
	

	DECLARE @RunDate DATETIME;
	DECLARE @RunDateFormatted VARCHAR(50);
	DECLARE @Inserted BIGINT;
	DECLARE @Updated BIGINT;	

	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
		SYS.OBJECTS O 
	
		INNER JOIN SYS.SCHEMAS S 
		ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'Process'
	,	@ProcessName = @ProcessName
	,	@Stage = 'UpdateAddressBeforMADPush'
	,	@Status = 'InProgress'
	,	@CurrentStatus = 'Started'	;

	BEGIN TRY   
    
		SELECT		
			@LastRetrievedDateTime = ETLProcessCategory.UTC_CompletedAt
		FROM
			ETLAudit.ETLProcessCategory 
		WHERE
			RunId IN
			(
				SELECT		
					MAX(AuditProcessCategory.RunId)
				FROM
					ETLProcess.ETLProcessCategory ProcessCategory
			
					INNER JOIN ETLAudit.ETLProcessCategory AuditProcessCategory
					ON ProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId		
			
					INNER JOIN ETLProcess.ETLStatus
					ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus
				WHERE
					ProcessCategory.ProcessCategoryName=@ProcessCategory	
					AND ETLStatus.Status='Completed'
			);
		
		IF @LastRetrievedDateTime IS NULL
			SET @LastRetrievedDateTime='1900-01-01'					

		SET @InsertClause=N'';
		SET @InsertClause = N' INSERT INTO '+@EntityName+N'_Invalid (';
	
		SELECT  
			@InsertClause = @InsertClause+COLUMN_NAME+N' ,'
		FROM 
			INFORMATION_SCHEMA.COLUMNS c
		WHERE 
			TABLE_SCHEMA+'.'+TABLE_NAME=@EntityName
			AND COLUMN_NAME NOT IN('IsValid','ID')
		ORDER BY 
			COLUMN_NAME;

	
		SET @InsertClause= @InsertClause+N'InvalidRuleId)';

		SET @SelectClause=N'';
		SET @SelectClause = N' SELECT ';
		
		SELECT  
			@SelectClause = @SelectClause+COLUMN_NAME+N','
		FROM 
			INFORMATION_SCHEMA.COLUMNS c
		WHERE 
			TABLE_SCHEMA+'.'+TABLE_NAME=@EntityName
			AND COLUMN_NAME NOT IN('IsValid','ID')
		ORDER BY 
			COLUMN_NAME;
		
		SET @CASE='';

		SELECT
			@CASE = @CASE + ' ' + CleansingRule + ' THEN ' + CAST(CleansingRuleId AS varchar) + ' ' 
		FROM 
			ETLProcess.ETLEntityCleansingRules 
		WHERE 
			Entity = @EntityName
			AND ActiveFlag = 1
			AND ColumnName IN('City','UnitNumber/StreetNumber')


		SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' ELSE 1 END AS InvalidRuleid  '
				+N' FROM '+@EntityName+N' WHERE LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) AND'
				+'(( 	LEN(ISNULL(City,'''')) > 30 OR LEN(ISNULL(UnitNumber,'''')) > 50 OR LEN(ISNULL(StreetNumber,'''')) > 50)'
				+' OR'
				+' (	NULLIF(UnitNumber,'''') IS  NULL AND NULLIF(StreetNumber,'''') IS  NULL	AND NULLIF(Streetname,'''') IS NULL AND NULLIF(FullAddress,'''') IS NULL  ) );'

		SET @strSQL=@InsertClause+@SelectClause
		EXECUTE (@strSQL)
	
		SET @DeleteSQL = 'DELETE E  FROM ' + @EntityName + ' E INNER JOIN ' + @EntityName + '_Invalid EInvalid ON E.Code = EInvalid.Code;'
		EXECUTE (@DeleteSQL)
			
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'Process'
		,	@ProcessName = @ProcessName
		,	@Stage = 'UpdateAddressBeforMADPush'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	
		,	@Updates=@@ROWCOUNT;
	END TRY
		
	BEGIN CATCH			

		INSERT INTO ETLProcess.ETLStoredProcedureErrors
		(
			ProcessCategory
		,	ProcessName
		,	ErrorNumber
		,	ErrorSeverity
		,	ErrorState
		,	ErrorProcedure
		,	ErrorLine
		,	ErrorMessage
		,	ErrorDate

		)
		SELECT  
			@ProcessCategory
		,	@ProcessName
		,	ERROR_NUMBER() AS ErrorNumber  
		,	ERROR_SEVERITY() AS ErrorSeverity  
		,	ERROR_STATE() AS ErrorState  
		,	@ErrorProcedure
		,	ERROR_LINE() AS ErrorLine  
		,	ERROR_MESSAGE() AS ErrorMessage
		,	GETDATE()
		
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'Process'
		,	@ProcessName = @ProcessName
		,	@Stage = 'UpdateAddressBeforMADPush'
		,	@Status = 'Error'
		,	@CurrentStatus = 'Error';

		EXEC ETLProcess.EmailNotification
			@ProcessCategory=@ProcessCategory
		,	@ProcessName= @ProcessName
		,	@ProcessStage='UpdateAddressBeforMADPush'
		,	@ErrorMessage='Failed to update Invalid Address'
		,	@IsError='Yes';

		THROW 50000, N'Failed to update Invalid Address', 1;

	END CATCH
END