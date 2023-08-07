


CREATE PROCEDURE [ETLProcess].[LoadExternalSourceStageLanding]
	@ExternalFileName VARCHAR(500)
AS
BEGIN	
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: External Files - Load to StageLanding.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @Params NVARCHAR(500)='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100),@ExternalFileName VARCHAR(500),@ExternalDataSourceName VARCHAR(100)';
	DECLARE	@DynamicSQL NVARCHAR(MAX);
	DECLARE	@StageLandSchema VARCHAR(50)='StageLanding.';
	DECLARE	@StageProcessSchema VARCHAR(50)='StageProcessing.';
	DECLARE	@ErrorSchema VARCHAR(50)='StageProcessErr.';
	DECLARE	@HistorySchema VARCHAR(50)='SourceHistory.';
	DECLARE	@ExternalDataSourceName VARCHAR(100)='DTCDataSetExternal';
	DECLARE	@TableName VARCHAR(100);
	DECLARE	@ProcessName VARCHAR(100)	;
	DECLARE	@CurrentStatus VARCHAR(100) ;
	DECLARE @RunId  INT;
	DECLARE	@IsAuditEntryExists INT;
	DECLARE	@Status VARCHAR(100);
	DECLARE	@ActiveFlag BIT;
	DECLARE	@IsAuditProcessEntryExists INT;	
	DECLARE	@IsError BIT=0;
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE	@Exception VARCHAR(500);
	DECLARE @DynamicSQLLarge VARCHAR(8000);
	DECLARE	@ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';
	DECLARE @IsKeyCount INT;
	DECLARE @ProcessID INT;
	DECLARE @DistKeyCnt INT=0;
	DECLARE @DistRowCnt INT=0;

	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
		SYS.OBJECTS O 
		
		INNER JOIN SYS.SCHEMAS S 
		ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	SELECT
		@ProcessName=ProcessName
	FROM
		Stage.ExternalFileslist
	WHERE
		FileName=@ExternalFileName

	--SET @TableName = REPLACE(REPLACE(REPLACE(@ExternalFileName,'.csv',''),'.txt',''),'.json','');
	--SET @ProcessName = LEFT(@TableName,LEN(@TableName)-2);

	SET @TableName=@ProcessName

	SELECT
		@ActiveFlag = COUNT(1)
	FROM 
		ETLProcess.ETLProcess

		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId
	WHERE
		ETLProcess.ProcessName = @ProcessName
		AND ETLProcess.ActiveFlag =1
		AND ETLProcessCategory.ActiveFlag=1;
		
	IF ISNULL(@ActiveFlag,0)=1
	BEGIN	
		
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName =@ProcessName
		,	@Stage ='Get RunId for Loading External Files'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		SELECT		
			@RunId = AuditProcessCategory.RunId
		FROM
			ETLProcess.ETLProcessCategory ProcessCategory

			INNER JOIN ETLAudit.ETLProcessCategory AuditProcessCategory
			ON ProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId		

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus
		WHERE
			ETLStatus.Status NOT IN('Completed','Hold')
			AND ProcessCategory.ProcessCategoryName=@ProcessCategory;

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage ='Got RunId for Loading External Files'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	
		,	@Inserts=0;	
	
		SELECT
			@IsAuditEntryExists= COUNT(1) 
		FROM 	
			ETLProcess.ETLProcess 				

			INNER JOIN 	ETLAudit.ETLProcess AuditProcess
			ON ETLProcess.ProcessId = AuditProcess.ProcessId

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId = AuditProcess.CurrentStatus
		WHERE 
			ETLProcess.ProcessName=@ProcessName		
			AND AuditProcess.RunId=@RunId
			AND CurrentStage='Landing';
		
		IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0			
			EXEC ETLProcess.AuditLog
				@ProcessCategory = 'DTC_ExternalSource_ETL'
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = 'Landing'
			,	@Status = 'InProgress'
			,	@CurrentStatus = 'Started'	;	
	
		SELECT 
			@CurrentStatus = ETLStatus.Status
		FROM
			ETLAudit.ETLProcess AuditProcess

			INNER JOIN ETLProcess.ETLProcess
			ON AuditProcess.ProcessId = ETLProcess.ProcessId

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId = AuditProcess.CurrentStatus
		WHERE
			RunId=@RunId
			AND ETLProcess.ProcessName = @ProcessName
			AND AuditProcess.CurrentStage = 'Landing';

		SELECT 
			@ProcessID=ProcessID
		FROM
			ETLProcess.ETLProcess
		WHERE
			ProcessName=@ProcessName
			
		IF ISNULL(@CurrentStatus,'') NOT IN('Completed','Hold')
		BEGIN	 
		
			IF OBJECT_ID( @StageLandSchema+@TableName, 'U') IS NOT NULL
				BEGIN
					
					BEGIN TRY
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Start loading to StageLanding'
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	;

						SET @DynamicSQL=N'';

						IF EXISTS (   SELECT 1   FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName	AND column_name = 'SourceID')
						BEGIN
							SET @DynamicSQL='ALTER TABLE '+ @StageLandSchema+@TableName+' DROP COLUMN SourceID';
							SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
							EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;
						END
	
						IF EXISTS (   SELECT 1  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName	AND column_name = 'Code')
						BEGIN
							SET @DynamicSQL='ALTER TABLE '+ @StageLandSchema+@TableName+' DROP COLUMN Code';
							SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
							EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;
						END

						IF EXISTS (   SELECT 1  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName	AND column_name = 'IsDuplicate')
						BEGIN
							SET @DynamicSQL='ALTER TABLE '+ @StageLandSchema+@TableName+' DROP COLUMN IsDuplicate';
							SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
							EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;
						END

						SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
						SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@TableName+' ;';
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;
	
						SET @DynamicSQL=N'';

						IF @ExternalFileName LIKE '%.txt'
							BEGIN
								SET @DynamicSQL = @DynamicSQL+
											' BULK INSERT '+@StageLandSchema+@TableName
										+	' FROM '''+@ExternalFileName+''''
										+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FIRSTROW = 2,  FIELDTERMINATOR = ''\t'', ROWTERMINATOR=''0x0a''); ';

								SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)'	;	
								EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName,@ExternalFileName,@ExternalDataSourceName;
								
							END
						ELSE IF @ExternalFileName LIKE '%.csv'
							BEGIN
								SET @DynamicSQL = @DynamicSQL+
											' BULK INSERT '+@StageLandSchema+@TableName
										+	' FROM '''+@ExternalFileName+''''
										+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,  FIELDQUOTE=''"''); ';
										--+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,  FIELDTERMINATOR = '','', ROWTERMINATOR=''0x0a''); ';
		
							
								SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)';
								EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName, @ExternalFileName,@ExternalDataSourceName;				

							END
						ELSE IF @ExternalFileName LIKE '%.json'
							BEGIN
								SET @DynamicSQLLarge='TRUNCATE TABLE StageLand.'+@TableName+' ; INSERT INTO '+@StageLandSchema+@TableName+' SELECT ';
							
								SELECT 
									@DynamicSQLLarge = @DynamicSQLLarge + 'JSON_VALUE(s.[value], ''$.'+TRIM(SourceColumnName)+'''),'
								FROM 
									Stage.ColumnMapping;				
			
								SET @DynamicSQLLarge=	LEFT(@DynamicSQLLarge,LEN(@DynamicSQLLarge)-1);

							
								SET @DynamicSQLLarge = @DynamicSQLLarge +' FROM'
										+ 	 '	OPENJSON'
										+	' ((	SELECT ' 
										+				' BulkColumn '
										+			' FROM' 
										+				' OPENROWSET '
										+				' (	BULK '''+@ExternalFileName+''',	DATA_SOURCE = '''+@ExternalDataSourceName+''',	SINGLE_CLOB	) import)) s';

							
								--EXECUTE(@DynamicSQLLarge)
							END										
					
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Completed loading to StageLaning'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'
						,	@Inserts = @@ROWCOUNT;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'Process'
						,	@ProcessName = @ProcessName
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'
						,	@Stage = 'Landing';

					END TRY 

					BEGIN CATCH
						BEGIN TRY
							SET @DynamicSQL=N'';

							IF @ExternalFileName LIKE '%.csv'
								BEGIN
									SET @DynamicSQL = @DynamicSQL+
												' BULK INSERT '+@StageLandSchema+@TableName
											+	' FROM '''+@ExternalFileName+''''
											+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,FIELDQUOTE=''"'' , ROWTERMINATOR=''0x0a''); ';		
							
									SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)';
									EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName, @ExternalFileName,@ExternalDataSourceName;			
								END
							ELSE
								THROW 50005, N'An error occurred while loading data to StageLanding', 1;
						END TRY

						BEGIN CATCH
							UPDATE Stage.ExternalFileslist SET IsError=1 WHERE FileName=@ExternalFileName;

							SET @IsError=1
							SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
							SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@TableName+' ;';
							EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;

							EXEC ETLProcess.AuditLog
								@ProcessCategory = @ProcessCategory
							,	@Phase = 'ProcessHistory'
							,	@ProcessName = @ProcessName
							,	@Stage ='Error loading to StageLanding'
							,	@Status = 'Error'
							,	@CurrentStatus = 'Error'
							,	@Inserts = 0;

							EXEC ETLProcess.AuditLog
								@ProcessCategory = @ProcessCategory
							,	@Phase = 'Process'
							,	@ProcessName = @ProcessName
							,	@Status = 'Error'
							,	@CurrentStatus = 'Error'
							,	@Stage = 'Landing';	

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
						
							
							EXEC ETLProcess.EmailNotification
								@ProcessCategory=@ProcessCategory
							,	@ProcessName= @ProcessName
							,	@ProcessStage='Landing'
							,	@ErrorMessage='Failed to Load StageLanding'
							,	@IsError='Yes';
						END CATCH
					END CATCH	

				END
		END		
	END	

	IF @IsError=1
		THROW 50005, N'An error occurred while loading data to StageLanding', 1;
		
END