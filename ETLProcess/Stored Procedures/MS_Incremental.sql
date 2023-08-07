
CREATE PROCEDURE [ETLProcess].[MS_Incremental]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Process to handle Matching and Survivorship incremental load
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @ProcessCategory VARCHAR(100)='DTC_Profisee_MS_Incremental_ETL';
	DECLARE @ProcessName VARCHAR(100)='ProfiseeMS_Incremental';
	DECLARE @ProcessStage VARCHAR(100)='MS_Incremental';
	DECLARE @ErroMessage VARCHAR(100)='Error Loading Property';

	DECLARE @ProcessID INT;
	DECLARE @IsAuditEntryExists INT;
	DECLARE @RunId INT;
	DECLARE @CurrentStatus VARCHAR(100);
	DECLARE @IsError BIT=0;

	DECLARE @EntityName VARCHAR(200);
	DECLARE	@Params NVARCHAR(500)='@DatabaseName VARCHAR(200), @EntityName   VARCHAR(200)';
	DECLARE	@DynamicSQL NVARCHAR(MAX);
	DECLARE	@AttributeName VARCHAR(100);
	DECLARE	@ObjectName VARCHAR(100);	
	DECLARE @DatabaseName VARCHAR(100)='Profisee';
	DECLARE @HistoryStage VARCHAR(200);


	DECLARE @ErrorProcedure VARCHAR(100);


	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
	SYS.OBJECTS O 
	INNER JOIN SYS.SCHEMAS S 
	ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	
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
	
	SELECT 
		@ProcessID = ETLProcess.ProcessId
	FROM
		ETLProcess.ETLProcess 
	
		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId
	WHERE
		ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1
		AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory
		AND ETLProcess.ProcessName=@ProcessName		;

	IF ISNULL(@ProcessID,0) > 0 AND ISNULL(@RunId,0) > 0
	BEGIN
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
			AND CurrentStage=@ProcessStage

		IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0
			EXEC ETLProcess.AuditLog
				@ProcessCategory = @ProcessCategory
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = @ProcessStage
			,	@Status = 'InProgress'
			,	@CurrentStatus = 'Started'												
			
		SELECT 
			@CurrentStatus = ETLStatus.Status
		FROM
			ETLAudit.ETLProcess

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId = ETLProcess.CurrentStatus
		WHERE
			RunId=@RunId
			AND ETLProcess.ProcessId = @ProcessId
			AND ETLProcess.CurrentStage = @ProcessStage;

		IF @CurrentStatus NOT IN('Completed','Hold') 
		BEGIN
			--Check if all the attribute exists in entities
				IF ISNULL((SELECT COUNT(1) FROM ETLProcess.MS_IncrementalAttributes	WHERE 	ActiveFlag=1),0) > 0
				BEGIN
					DECLARE MS_Incremental CURSOR 
					FOR 
						SELECT 
							EntityName
						,	AttributeName
						FROM 
							ETLProcess.MS_IncrementalAttributes
						WHERE 
							ActiveFlag=1;
		
					OPEN MS_Incremental
					FETCH NEXT FROM MS_Incremental INTO 	@EntityName,@AttributeName				

					WHILE @@FETCH_STATUS = 0
					BEGIN		
						SET @ObjectName = @DatabaseName + '.' + 'data' + '.' + @EntityName;

						IF COL_LENGTH(@ObjectName, @AttributeName) IS NULL
						BEGIN
							SET @IsError=1;				
							SET @ErroMessage='['+@AttributeName+']'+' - Not Exists in data.'+@EntityName;				
							
							EXEC ETLProcess.EmailNotification
								@ProcessCategory=@ProcessCategory
							,	@ProcessName= @ProcessName
							,	@ProcessStage=@ProcessStage
							,	@ErrorMessage=@ErroMessage
							,	@IsError='Yes';
														
							EXEC ETLProcess.AuditLog
								@ProcessCategory = @ProcessCategory
							,	@Phase = 'ProcessHistory'
							,	@ProcessName = @ProcessName
							,	@Stage ='Error Loading'
							,	@Status = 'Error'
							,	@CurrentStatus = 'Error'	
													
							EXEC ETLProcess.AuditLog
								@ProcessCategory = @ProcessCategory
							,	@Phase = 'Process'
							,	@ProcessName = @ProcessName
							,	@Status = 'Error'
							,	@CurrentStatus = 'Error'
							,	@Stage = @ProcessStage
						END
						FETCH NEXT FROM MS_Incremental INTO 	@EntityName,@AttributeName				
					END				
			
					CLOSE MS_Incremental
					DEALLOCATE MS_Incremental	
				END
		END

		IF @CurrentStatus NOT IN('Completed','Hold') AND  @IsError=0
		BEGIN
			BEGIN TRY
				--If all attributes exists
				IF @IsError=0		
				BEGIN
					DROP TABLE IF EXISTS #MatchGroup	;
					DECLARE @Attirbutes TABLE (	Id	INT IDENTITY(1,1),	AttributeName VARCHAR(200));
					CREATE TABLE #MatchGroup ([Match Group] NVARCHAR(200));

					DECLARE MS_Incremental CURSOR
						FOR 
							SELECT 
								DISTINCT EntityName
							FROM 
								ETLProcess.MS_IncrementalAttributes
							WHERE 
								ActiveFlag=1;
	
					OPEN MS_Incremental
					FETCH NEXT FROM MS_Incremental INTO  @EntityName

					WHILE @@FETCH_STATUS = 0
					BEGIN
						SET @DynamicSQL =N'';
						TRUNCATE TABLE #MatchGroup;
						DELETE FROM @Attirbutes;

						INSERT INTO @Attirbutes
							SELECT 
								AttributeName 
							FROM 
								ETLProcess.MS_IncrementalAttributes
							WHERE 
								EntityName=@EntityName;		

						SET @HistoryStage =  'Get inserts for '+@EntityName

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	

						--Get the [Match Group] to be proessesd
						SET @DynamicSQL =	
									'INSERT INTO #MatchGroup'+' '+		
										'SELECT '+
											'DISTINCT MasterRecords.[Match Group] '+							 
										' FROM('+
												'SELECT'+
													'[Match Group], ';
											
						SELECT 
							@DynamicSQL = @DynamicSQL + '['+AttributeName+'],' 
						FROM 
							@Attirbutes;

						SET 
							@DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1) +
									' FROM '+@DatabaseName+'.data.'+ @EntityName +' (NOLOCK) WHERE '+@EntityName+'.[Record Source] = 1 ) MasterRecords'+
									' JOIN ( '+
											'SELECT [Match Group], ';

						SELECT @DynamicSQL = @DynamicSQL + '['+AttributeName+'],' FROM @Attirbutes;
						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+
									' FROM '+@DatabaseName+'.data.'+@EntityName +' (NOLOCK) WHERE '+@EntityName+'.[Record Source] IS NULL) ChildRecords'+
									' ON MasterRecords.[Match Group] = ChildRecords.[Match Group]'+
									' WHERE ';
						SELECT 
							@DynamicSQL = @DynamicSQL + ' MasterRecords.['+AttributeName+']' +'<>'+' ChildRecords.['+AttributeName+']'+
							' OR '
						FROM 
							@Attirbutes;

						SET @DynamicSQL=LEFT(@DynamicSQL,LEN(@DynamicSQL)-2);
						SET @HistoryStage =  'Completed inserts for '+@EntityName;

						PRINT @DynamicSQL
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@DatabaseName,@EntityName;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	
						,	@Inserts = @@ROWCOUNT


						SET @HistoryStage =  'Get updates for '+@EntityName

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	


						--Update the records for matched [Match Group]
						SET @DynamicSQL=NULL
						SET @DynamicSQL =
									'UPDATE Entity '+
									'SET	[Match Score] = NULL'+
										',	[Record Source] = NULL'+
										',	[Match Status] = NULL'+
										',	[Match Group] = NULL'+
										',	[Match Member] = NULL'+
										',	[Match Strategy] = NULL'+
										',	[Match DateTime] = NULL'+
										',	[Match MultiGroup] = NULL'+
										',	[Match User] = NULL'+
										',	[Master] = NULL'+
										',	[Proposed Count] = NULL'+
										',	[Approved Count] = NULL'+' '+
									'FROM'+' '+
										@DatabaseName+'.data.'+ @EntityName+' Entity '+
										'INNER JOIN #MatchGroup MG '+
										'ON MG.[Match Group]=Entity.[Match Group] '+				
									'WHERE '+
										'Entity.[Record Source] IS NULL'

						SET @HistoryStage =  'Completed updates for '+@EntityName;

						PRINT @DynamicSQL					
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@DatabaseName,@EntityName;
						
						--Delete the records for matched [Match Group]	
						SET @DynamicSQL=N'';
						SET @DynamicSQL = 
									'DELETE FROM Entity '+
									'FROM'+' '+
										@DatabaseName+'.data.'+ @EntityName+' Entity '+
										'INNER JOIN #MatchGroup MG '+
										'ON MG.[Match Group]=Entity.[Match Group] '+				
									'WHERE '+
										'Entity.[Record Source]=1';

						PRINT @DynamicSQL
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@DatabaseName,@EntityName;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	
						,	@Updates = @@ROWCOUNT
						
						FETCH NEXT FROM MS_Incremental INTO 	@EntityName		;		
					END

					CLOSE MS_Incremental
					DEALLOCATE MS_Incremental	

					--UPDATE ETLProcess.MS_Automation_Status SET MSStatus='Start',LastModifiedDate=GETUTCDATE();
				END

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Stage = @ProcessStage
			END TRY

			BEGIN CATCH
				SET @IsError=1

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage ='Error Loading'
				,	@Status = 'Error'
				,	@CurrentStatus = 'Error'	
										
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Error'
				,	@CurrentStatus = 'Error'
				,	@Stage = @ProcessStage

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
				,	@ProcessStage=@ProcessStage
				,	@ErrorMessage=@ErroMessage
				,	@IsError='Yes';

			END CATCH
		END

		IF @IsError=1
			THROW 50001, @ErroMessage, 1;		
	END
END