



CREATE PROCEDURE [ETLProcess].[LoadInternalSourceStageProcessing]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Internal Sources - Load to StageProcessing.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
		
	DECLARE	@ProcessCategory VARCHAR(100)='DTC_InternalSource_ETL';
	DECLARE	@StageLandSchema VARCHAR(50)='StageLanding.';
	DECLARE	@StageProcessSchema VARCHAR(50)='StageProcessing.';
	DECLARE	@HistorySchema VARCHAR(50)='SourceHistory.';
	DECLARE	@ErrSchema VARCHAR(50)='StageProcessingErr.';	
	DECLARE	@Cnt BIGINT=0 ;
	DECLARE	@RunId INT;
	DECLARE	@Inserted INT ;
	DECLARE	@Updated INT ;
	DECLARE	@IsKeyCount INT;
	DECLARE	@i INT=1 
	DECLARE	@Counter INT=0 ;
	DECLARE @IsAuditEntryExists INT;
	DECLARE @ProcessId INT ;
	DECLARE	@ProcessName VARCHAR(100) ;	
	DECLARE	@CurrentStatus VARCHAR(100) ;
	DECLARE @LandingHashByteClause NVARCHAR(2000) =N' ';
	DECLARE	@HistoryHashByteClause NVARCHAR(2000) =N' ';
	DECLARE @SelectClasue NVARCHAR(1000) =N' ';
	DECLARE @HisotoryClause NVARCHAR(4000)=N' ';
	DECLARE	@DynamicSQL NVARCHAR(4000)=N' ';
	DECLARE	@Params NVARCHAR(1000)=N' ';
	DECLARE @WhereClause NVARCHAR(4000)=N' ';
	DECLARE	@InsertClause NVARCHAR(MAX)=N'';
	DECLARE @MappingIsKeyCount INT;	
	DECLARE @IsError BIT=0;
	DECLARE @DupCnt INT=0;
	DECLARE @ErrorProcedure VARCHAR(100);

	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
		SYS.OBJECTS O 
		
		INNER JOIN SYS.SCHEMAS S 
		ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	DROP TABLE IF EXISTS #Process;		
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Get RunId to load to StageProcessing'
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
	,	@ProcessName = ''
	,	@Stage ='Got RunId to load to StageProcessing'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Inserts=0;
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Get list of Processes to load to StageProcessing'
	,	@Status = 'InProgress'
	,	@CurrentStatus = 'Started'	;
	
	SELECT 
		ETLProcess.ProcessId
	,	ETLProcess.ProcessName	
	INTO
		#Process
	FROM
		ETLProcess.ETLProcess 

		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId
	WHERE
		ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1
		AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory;
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Obtained list of Processes to load to StageProcessing'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Inserts=@@ROWCOUNT;
	
	DECLARE StageProcessingCursor CURSOR
	FOR 
		SELECT 
			ProcessId
		,	ProcessName			
		FROM 
			#Process				
	OPEN StageProcessingCursor	
		FETCH NEXT FROM StageProcessingCursor INTO  @ProcessId,@ProcessName

	WHILE @@FETCH_STATUS = 0
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
				AND CurrentStage='Processing';			
			
			IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Stage = 'Processing'
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
				AND ETLProcess.CurrentStage = 'Processing';			

			IF @CurrentStatus NOT IN('Completed', 'Hold')
			BEGIN					
				BEGIN TRY
						----Load Code to StageLadning table
						SET @DynamicSQL=N'';
						SET @DynamicSQL=  ' UPDATE '+@StageLandSchema+@ProcessName+' SET Code = '''+CAST(@ProcessId AS VARCHAR)+'_'+CAST(@RunId AS VARCHAR)+'_'''+'+CAST(SourceID AS VARCHAR)';
						SET @Params ='@StageLandSchema VARCHAR(50),@ProcessName VARCHAR(100)';					
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@ProcessName ;
						
						----Truncate StageProcessing
						SET @DynamicSQL=N'' ;
						SET @DynamicSQL= ' TRUNCATE TABLE  '+@StageProcessSchema+@ProcessName;
						SET @Params ='@StageProcessSchema VARCHAR(50),@ProcessName VARCHAR(100)';
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageProcessSchema,@ProcessName ;
								
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Check for error records'
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	;
											
						SET @DynamicSQL=N'';
						SET @DynamicSQL = N'INSERT INTO  '+@ErrSchema+@ProcessName+ N'( SourceID, Code, ErrorStatusId, ';
						
						SELECT 
							@DynamicSQL = @DynamicSQL +N' ['+SourceColumnName+N'],'
						FROM
							(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
							) DistinctColumns
						WHERE
							Rn=1
						ORDER BY
							MappingId;

						
						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)	+N'  ) SELECT SourceID, Code,1 AS ErrorStatusId, ';

						SELECT 
							@DynamicSQL = @DynamicSQL +N' ['+SourceColumnName+N'],'
						FROM
							(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND ( DestinationColumnName IS NOT NULL OR IsKey=1)
							) DistinctColumns
						WHERE
							Rn=1
						ORDER BY
							MappingId;

						
						--Generate WHERE clause
						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+N' FROM '+@StageLandSchema+@ProcessName+ N' WHERE ';
											   
						SELECT 
							@DynamicSQL = @DynamicSQL + N' TRY_CAST( ['+SourceColumnName+N'] AS '+DestinationColumnDataType+N') <> ['+SourceColumnName+N'] OR'
						FROM
							(	SELECT SourceColumnName,MappingId,DestinationColumnDataType,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND DestinationColumnName IS NOT NULL AND DestinationColumnDataType NOT IN('VARCHAR(2000)','VARCHAR(4000)', 'VARCHAR(MAX)')
							) DistinctColumns
						WHERE
							Rn=1
						ORDER BY
							MappingId;
						
						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-2)	;
						SET @Params ='@ErrSchema VARCHAR(50),@ProcessName VARCHAR(100), @StageLandSchema VARCHAR(100)';
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@ErrSchema,@ProcessName,@StageLandSchema;												

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Completed checking for error records'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	
						,	@Inserts=@@ROWCOUNT;		
						
						--Check if there are entry in History									
						SET @Cnt =0;
						SET @DynamicSQL=N'';
						SET @DynamicSQL= N' SELECT @CntOP = COUNT(1) FROM  '+	@HistorySchema +@ProcessName;
						SET @Params ='@StageLandSchema VARCHAR(50),@ProcessName VARCHAR(100), @CntOP INT OUTPUT';
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@ProcessName,@CntOP = @Cnt	OUTPUT;
						
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Get Delta records to Processing'
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	;

						--Generate INSERT caluse
						SET @InsertClause=N'';
						SET @InsertClause = @InsertClause +N' INSERT INTO '+@StageProcessSchema+@ProcessName+N'( SourceID, Code, ';

						SELECT 
							@InsertClause = @InsertClause + N' ['+SourceColumnName+N'],'
						FROM
							(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping
								WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
							) DistinctColumns
						WHERE
							Rn=1
						ORDER BY
							MappingId;

						SET @InsertClause=@InsertClause+N' ActionType)';

						--Generate SELECT Clause
						SET @SelectClasue=N'';
						SET @SelectClasue=
								CASE 
									WHEN @Cnt=0  THEN N' SELECT DISTINCT Land.SourceID, Land.Code,'
									ELSE N' SELECT DISTINCT Land.SourceID, CASE WHEN Hist.Code IS NULL THEN Land.Code  ELSE Hist.Code END AS Code,'
								END;

						SELECT 
							@SelectClasue = @SelectClasue 
											+ CASE 
													WHEN DestinationColumnDataType ='VARCHAR(2000)' THEN  N'LEFT(Land.['+SourceColumnName+N'],2000) AS '+SourceColumnName+N','
													WHEN DestinationColumnDataType ='VARCHAR(4000)' THEN  N'LEFT(Land.['+SourceColumnName+N'],4000) AS '+SourceColumnName+N','
													WHEN DestinationColumnDataType ='VARCHAR(MAX)'  THEN  N'LEFT(Land.['+SourceColumnName+N'],4000) AS '+SourceColumnName+N','
													ELSE N'Land.['+ SourceColumnName+N'] ,'
												END
						FROM
							(	SELECT SourceColumnName,MappingId,DestinationColumnDataType,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
							) DistinctColumns
						WHERE
							Rn=1
						ORDER BY
							MappingId;

						SET @SelectClasue =  
								@SelectClasue+ 
									CASE 
										WHEN @Cnt=0 THEN  N' ''I'' AS ActionType FROM' 
										ELSE N' CASE WHEN Hist.Code IS NULL THEN ''I''  ELSE ''U'' END AS ActionType ' 
									END;
						
						IF @Cnt=0
						BEGIN
								--Generate Inner Query
								SET @DynamicSQL=N'';					
								SET @DynamicSQL =@DynamicSQL+N'( SELECT SourceID, Code,';	
																							
								SELECT 
									@DynamicSQL = @DynamicSQL +SourceColumnName+N' ,'												
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
									) DistinctColumns
								WHERE
									Rn=1
								ORDER BY
									MappingId;

								SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+N' FROM '+@StageLandSchema+@ProcessName+N' )' ;
			
								--PRINT CAST( @InsertClause AS TEXT)
								--PRINT CAST( @SelectClasue AS TEXT)
								--PRINT CAST( @DynamicSQL AS TEXT)
								--PRINT CAST( N' Land WHERE NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@ProcessName+N' Sub WHERE Land.Code = Sub.Code AND Land.SourceID =Sub.SourceID  )' AS TEXT)
																
								EXEC(	@InsertClause
										+N' '+@SelectClasue
										+N' '+@DynamicSQL
										+N' Land WHERE NOT EXISTS( SELECT 1 FROM '
										+@ErrSchema+@ProcessName
										+N' Sub WHERE Land.Code = Sub.Code AND Land.SourceID =Sub.SourceID  )' 
									);

								SET @Inserted = @@ROWCOUNT;		
								
							END
						ELSE
							BEGIN																			
								--Generate Inner Query
								SET @DynamicSQL=N'';					
								SET @DynamicSQL =@DynamicSQL+N'( SELECT SourceID, Code,';	
																
								SELECT 
									@DynamicSQL = @DynamicSQL +SourceColumnName+N' ,'												
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
									) DistinctColumns
								WHERE
									Rn=1
								ORDER BY
									MappingId;

								SET @DynamicSQL =LEFT(@DynamicSQL,LEN(@DynamicSQL)-1) +N' FROM '+@StageLandSchema+@ProcessName
											+N' L  WHERE  NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@ProcessName+N' Sub WHERE L.Code = Sub.Code AND L.SourceID =Sub.SourceID  ) ) Land' ;
											
								--Generate History Clause
								SET @HisotoryClause=N'';
								SET @HisotoryClause=N'( SELECT Code,';
						
								SELECT 
									@HisotoryClause = @HisotoryClause + N'['+SourceColumnName+'],'
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
									) DistinctColumns
								WHERE
									Rn=1
								ORDER BY
									MappingId;

								SET @HisotoryClause =LEFT(@HisotoryClause,LEN(@HisotoryClause)-1) +N' FROM '+@HistorySchema+@ProcessName+N' WHERE HistEndDate IS NULL  ) Hist' 
								
								--Generate ON and WHERE caluse		
								SET @WhereClause=N'';
								SET @WhereClause=N' ON ';
								
								SELECT 
									@WhereClause =@WhereClause+ N' ISNULL(NULLIF(Land.['+SourceColumnName+N'],''''),''|'') = ISNULL(NULLIF(Hist.['+SourceColumnName+N'],''''),''|'') AND'
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND IsKey=1
									) DistinctColumns
								WHERE
									Rn=1
								ORDER BY
									MappingId;

								SET @WhereClause= @WhereClause+N' (';
																
								SELECT 
									@WhereClause =@WhereClause+ N' NULLIF(Hist.['+SourceColumnName+N'],'''') IS NOT NULL OR'
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND IsKey=1
									) DistinctColumns
								WHERE
									Rn=1
								ORDER BY
									MappingId;

								SET @WhereClause = LEFT(@WhereClause,LEN(@WhereClause)-2)+N' ) WHERE ( Hist.Code IS NULL OR'	;
			
								SELECT 
									@WhereClause =@WhereClause+ N' ISNULL(Land.['+SourceColumnName+N'], '''')<>ISNULL(Hist.['+SourceColumnName+N'],'''') OR'																				
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND IsKey=1 AND DestinationColumnDataType IS NOT NULL
									) DistinctColumns
								WHERE
									Rn=1
								ORDER BY
									MappingId;

								SET @WhereClause = LEFT(@WhereClause,LEN(@WhereClause)-2)+N') ';
								
								--PRINT CAST(@InsertClause AS NTEXT)
								--PRINT CAST(N' '+@SelectClasue AS NTEXT)
								--PRINT CAST(N' FROM '+@DynamicSQL AS NTEXT)
								--PRINT CAST(N' LEFT JOIN '+@HisotoryClause AS NTEXT)	
								--PRINT CAST(N' '+@WhereClause AS NTEXT)
																
								EXEC(		@InsertClause
											+N' '+@SelectClasue
											+N' FROM '+@DynamicSQL
											+N' LEFT JOIN '+@HisotoryClause	
											+N' '+@WhereClause								
										);			
								SET @Inserted = @@ROWCOUNT	;													
						END

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Loaded delta records to Processing'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'
						,	@Inserts = @Inserted;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'Process'
						,	@ProcessName = @ProcessName
						,	@Stage = 'Processing'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed';
												
					END TRY

					BEGIN CATCH

						SET @IsError=1;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Error to get Delta records to Processing'
						,	@Status = 'Error'
						,	@CurrentStatus = 'Error'						

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'Process'
						,	@ProcessName = @ProcessName
						,	@Status = 'Error'
						,	@CurrentStatus = 'Error'
						,	@Stage = 'Processing';

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
						,	@ProcessStage='Processing'
						,	@ErrorMessage='Failed to Load data to StageProcessing'
						,	@IsError='Yes';
						
					END CATCH	
				END
				
			FETCH NEXT FROM StageProcessingCursor INTO 	@ProcessId,@ProcessName
		END

	CLOSE StageProcessingCursor
	DEALLOCATE StageProcessingCursor	

	IF @IsError=1
		THROW 50001, N'An error occurred while loading data to StageProcessing', 1;
		
END