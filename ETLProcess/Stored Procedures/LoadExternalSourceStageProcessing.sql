



CREATE PROCEDURE [ETLProcess].[LoadExternalSourceStageProcessing]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: External Files - Load to StageProcessing.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
** 09/09/2022   Shirish - Filtered custom sources (Line 118)
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	DECLARE	@ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';
	DECLARE	@StageLandSchema VARCHAR(50)='StageLanding.';
	DECLARE	@StageProcessSchema VARCHAR(50)='StageProcessing.';
	DECLARE	@HistorySchema VARCHAR(50)='SourceHistory.';
	DECLARE	@ErrSchema VARCHAR(50)='StageProcessingErr.';
	DECLARE	@RunId INT;
	DECLARE @TableName VARCHAR(100)='';
	DECLARE	@ProcessId INT=0;
	DECLARE	@ProcessName VARCHAR(100)='';
	DECLARE	@CurrentStatus VARCHAR(100) ;
	DECLARE	@MappingIsKeyCount INT=0;	
	DECLARE @IsAuditEntryExists INT;
	DECLARE	@HistoryCnt BIGINT=0;
	DECLARE	@Inserted INT ;
	DECLARE	@Updated INT ;
	DECLARE @LandingHashByteClause NVARCHAR(MAX)=N'';
	DECLARE	@HistoryHashByteClause NVARCHAR(MAX)=N'';
	DECLARE	@SelectClasue NVARCHAR(MAX)=N'';		
	DECLARE @HisotoryClause NVARCHAR(MAX)=N'';
	DECLARE	@DynamicSQL NVARCHAR(MAX)=N'';
	DECLARE	@InsertClause NVARCHAR(MAX)=N'';
	DECLARE @WhereClause NVARCHAR(MAX);
	DECLARE	@Params NVARCHAR(2000)=N'';
	DECLARE @IsError BIT=0;
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @DistRowCnt INT;
	DECLARE @DistKeyCnt INT;
	DECLARE @LandingCnt INT;
	DECLARE @MappingFileError INT;

	DROP TABLE IF EXISTS #ETLProcess;
	SELECT @ErrorProcedure= s.name+'.'+o.name FROM SYS.OBJECTS O INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;	

	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Get RunId for Processing External Files'
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
	,	@Stage ='Got RunId for Processing External Files'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Inserts=0;

	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Get list of processes for Processing'
	,	@Status = 'InProgress'
	,	@CurrentStatus = 'Started'	;

	SELECT 
		ETLProcess.ProcessId
	,	ETLProcess.ProcessName	
	INTO
		#ETLProcess
	FROM
		ETLProcess.ETLProcess 

		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId
	WHERE
		ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1
		AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory
		AND ISNULL(ETLProcess.IsSourceSpecificLoad,0)=0;
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Loaded list of processes for Processing'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Inserts=@@ROWCOUNT;

	DECLARE StageProcessingCursor CURSOR
	FOR 
		SELECT 
			ProcessId
		,	ProcessName			
		FROM 
			#ETLProcess		
		
	OPEN StageProcessingCursor
	
	FETCH NEXT FROM StageProcessingCursor INTO  @ProcessId,@ProcessName

	WHILE @@FETCH_STATUS = 0	
		BEGIN
			SET @Inserted =0;

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
				AND CurrentStage='Processing'
			
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

			SELECT 
				@TableName	= t.name 
			FROM 
				sys.tables t 
			WHERE 
				SCHEMA_NAME(t.SCHEMA_ID) = 'StageLanding' 
				AND t.name = @ProcessName;

			SET @LandingCnt =0;

			IF EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName)
			BEGIN
				
				SET @DynamicSQL=N'';
				SET @DynamicSQL= N' SELECT @CntOP = COUNT(1) FROM  '+@StageLandSchema +@TableName;
				SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100), @CntOP INT OUTPUT';
				EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName,@CntOP = @LandingCnt	OUTPUT;							
			END			

			IF EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName)
			BEGIN
				SELECT  @MappingFileError=COUNT(1) FROM Stage.MappingFileList WHERE ProcessName=@ProcessName AND IsError=1;

				--Check if there is error in the mapping file
				IF @MappingFileError>0 AND EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageProcessing'	AND TABLE_NAME = @TableName)
				BEGIN
					SET @DynamicSQL=N'';
					SET @DynamicSQL= N' TRUNCATE TABLE  '+@StageProcessSchema+@TableName;
					EXECUTE(@DynamicSQL);
				END
			END
			
			IF @CurrentStatus NOT IN('Completed','Hold') AND ( @LandingCnt = 0 OR @MappingFileError>0  )
			BEGIN
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage ='No records loaded to StageProcessing'
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'	
				,	@Inserts=@Inserted				
				
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Stage = 'Processing'

			END

			IF @CurrentStatus NOT IN('Completed','Hold') AND @LandingCnt > 0 AND @MappingFileError=0 AND EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageProcessing'	AND TABLE_NAME = @TableName)
			BEGIN
				BEGIN TRY					
			
					IF NOT EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName	AND column_name = 'SourceID')
						BEGIN
							SET @DynamicSQL=N'ALTER TABLE '+ @StageLandSchema+@TableName+N' ADD SourceID INT IDENTITY(1,1)';								
							EXECUTE(@DynamicSQL);
						END
			
					IF NOT EXISTS (   SELECT 1     FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageLanding'	AND TABLE_NAME = @TableName	AND column_name = 'Code')
						BEGIN
							SET @DynamicSQL=N'ALTER TABLE '+ @StageLandSchema+@TableName+N' ADD Code VARCHAR(200)';									
							EXECUTE(@DynamicSQL);
						END
					
					--Load Code to StageLadning table
					SET @DynamicSQL=N'';
					SET @DynamicSQL= N' UPDATE '+@StageLandSchema+@TableName+N' SET Code = '''+CAST(@ProcessId AS VARCHAR)+'_'+CAST(@RunId AS VARCHAR)+N'_'''+N'+CAST(SourceID AS VARCHAR)';
					EXECUTE(@DynamicSQL);		

					--Truncate StageProcessing
					SET @DynamicSQL=N'';
					SET @DynamicSQL= N' TRUNCATE TABLE  '+@StageProcessSchema+@TableName;
					EXECUTE(@DynamicSQL);
					
					--Check entries in History			
					SET @HistoryCnt =0;
					SET @DynamicSQL=N'';
					SET @DynamicSQL= N' SELECT @CntOP = COUNT(1) FROM  '+	@HistorySchema +@TableName;
					SET @Params ='@HistorySchema VARCHAR(50),@TableName VARCHAR(100), @CntOP INT OUTPUT';
					EXECUTE sp_executesql 	@DynamicSQL,@Params,@HistorySchema,@TableName,@CntOP = @HistoryCnt	OUTPUT;
						
					--Get Key columns count		
					SELECT 
						@MappingIsKeyCount = COUNT(1) 
					FROM	
						ETLProcess.ETLSourceMapping						
					WHERE
						ProcessId =@ProcessId	
						AND ETLSourceMapping.IsKey=1;

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Check for error records due to data type mismatch'
					,	@Status = 'InProgress'
					,	@CurrentStatus = 'Started'	;
									   
					--Insert Error records
					SET @DynamicSQL = N'';
					SET @DynamicSQL = N'INSERT INTO  '+@ErrSchema+@TableName+ N'( SourceID, Code, ErrorStatusId, ';
						
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
						MappingId
			
					SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)	+N'  ) SELECT SourceID, Code, 1 AS ErrorStatusId,  ';

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
						MappingId
			
					--Generate WHERE clause
					SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+N' FROM '+@StageLandSchema+@TableName+ N' L WHERE  (';
			
					SELECT 
						@DynamicSQL = @DynamicSQL + N' TRY_CAST( ['+SourceColumnName+N'] AS '+DestinationColumnDataType+N') <> ['+SourceColumnName+N'] OR'
					FROM
						(	SELECT SourceColumnName,MappingId,DestinationColumnDataType,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
							FROM ETLProcess.ETLSourceMapping 
							WHERE ProcessId=@ProcessId AND DestinationColumnName IS NOT NULL AND DestinationColumnDataType LIKE 'VARCHAR(%' ) DistinctColumns
					WHERE
						Rn=1	
					ORDER BY
						MappingId
			
					SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-2)+N') AND NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE L.Code = Sub.Code AND L.SourceID =Sub.SourceID  )' ;
					
					EXECUTE(@DynamicSQL);						
					SET @Inserted=@@ROWCOUNT;

					--Insert error records because conversion issue
					IF EXISTS(SELECT 1 FROM ETLProcess.ETLSourceMapping  WHERE ProcessId=@ProcessId AND (DestinationColumnDataType LIKE  'DATE%' OR DestinationColumnDataType='INT' OR DestinationColumnDataType LIKE 'DECIMAL%' ))
					BEGIN
						SET @DynamicSQL = N'';
						SET @DynamicSQL = N'INSERT INTO  '+@ErrSchema+@TableName+ N'( SourceID, Code, ErrorStatusId, ';
			
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
							MappingId
			
						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)	+N'  ) SELECT SourceID, Code, 2 AS ErrorStatusId,  ';
					
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
							MappingId
			
						--Generate WHERE clause
						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+N' FROM '+@StageLandSchema+@TableName+ N' L WHERE (';
					
						SELECT 
							@DynamicSQL = @DynamicSQL 
								+	CASE 
										WHEN ConvFunction IS NOT NULL THEN
											CASE	
												WHEN DestinationColumnDataType LIKE 'DECIMAL%' THEN N'ISNUMERIC( ISNULL(NULLIF('+REPLACE(ConvFunction,SourceColumnName,N'['+SourceColumnName+N']')+ N',''''),0))=0 OR '
												WHEN DestinationColumnDataType='INT' THEN N'ISNUMERIC( ISNULL(NULLIF('+REPLACE(ConvFunction,SourceColumnName,N'['+SourceColumnName+N']')+ N',''''),0))=0 OR'+
													+N' LEN(ISNULL(NULLIF('+REPLACE(ConvFunction,SourceColumnName,N'['+SourceColumnName+N']')+ N',''''),0))>10 OR '
												WHEN DestinationColumnDataType LIKE 'DATE%' THEN 'ISDATE(ISNULL(NULLIF('+REPLACE(ConvFunction,SourceColumnName,N'['+SourceColumnName+']')+ N','''')''01/01/1900''))=0 OR '
											END
										ELSE 
											CASE	
												WHEN DestinationColumnDataType LIKE 'DECIMAL%' THEN N' ISNUMERIC(ISNULL(NULLIF(['+SourceColumnName+N'],''''),0))=0 OR '
												WHEN DestinationColumnDataType='INT' THEN N' ISNUMERIC(ISNULL(NULLIF(['+SourceColumnName+N'],''''),0))=0 OR LEN(ISNULL(NULLIF(['+SourceColumnName+N'],''''),0)) >10 OR'
												WHEN DestinationColumnDataType LIKE 'DATE%' THEN N' ISDATE(ISNULL(NULLIF(['+SourceColumnName+N'],''''),''01/01/1900''))=0 OR '
											END
									END
						FROM
							(	SELECT SourceColumnName,MappingId,DestinationColumnDataType,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn, ConvFunction 
								FROM ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId 
								AND ( DestinationColumnDataType='INT' OR DestinationColumnDataType LIKE 'DECIMAL%' OR DestinationColumnDataType LIKE 'DATE%')
							
							) DistinctColumns
						WHERE
							Rn=1	
						ORDER BY
							MappingId

						SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-2)+N') AND NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE L.Code = Sub.Code AND L.SourceID =Sub.SourceID  )' ;	
										
						EXECUTE(@DynamicSQL);
						SET @Inserted=@Inserted+@@ROWCOUNT;
					END
					
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Completed checking for error records due to data type mismatch'
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'	
					,	@Inserts=@Inserted;	
										
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Start Loading to StageProcessing'
					,	@Status = 'InProgress'
					,	@CurrentStatus = 'Started'	
										
					IF @MappingIsKeyCount >0  
					BEGIN	
						
						--Generate INSERT caluse
						SET @InsertClause=N'';
						SET @InsertClause = @InsertClause +N' INSERT INTO '+@StageProcessSchema+@TableName+N'( SourceID, Code, ';
						
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
							MappingId
						
						SET @InsertClause=@InsertClause+N' ActionType)';


						--Generate SELECT Clause
						SET @SelectClasue=N'';						
						SET @SelectClasue=
								CASE 
									WHEN @HistoryCnt=0  THEN N' SELECT DISTINCT Land.SourceID, Land.Code,'
									ELSE N' SELECT DISTINCT Land.SourceID, CASE WHEN Hist.Code IS NULL THEN Land.Code  ELSE Hist.Code END AS Code,'
								END;
															   
						SELECT 
							@SelectClasue = @SelectClasue 
											+	CASE	WHEN ConvFunction IS NOT NULL THEN REPLACE(ConvFunction,SourceColumnName,N'Land.['+SourceColumnName+N']')
														WHEN DestinationColumnDataType IN('DATE' ,'DATETIME')THEN N'CONVERT(DATETIME, Land.['+SourceColumnName+N'], 120)'
												 ELSE	
													+N' CASE WHEN ISNUMERIC( Land.['+SourceColumnName+N'])=1 AND CHARINDEX ( ''E+'', Land.['+SourceColumnName+N'] ) > 0 '+
																+N' THEN CAST( TRY_CONVERT(NUMERIC(17'+CASE WHEN DestinationColumnDataType=N'DECIMAL(17,2)' THEN N',2' ELSE N',0' END
																		+N' ), CAST(REPLACE(Land.['+SourceColumnName+N'],'','','''') AS FLOAT)) AS  '+ISNULL(DestinationColumnDataType,'NVARCHAR(510)')+' )'
														+N' ELSE '+ 
																CASE WHEN DestinationColumnDataType LIKE 'DECIMAL%' 
																		THEN 'CAST( CONVERT(NUMERIC(17'+CASE WHEN DestinationColumnDataType=N'DECIMAL(17,2)' THEN N',2' ELSE N',0' END
																			+N' ), TRY_CAST(Land.['+SourceColumnName+N'] AS FLOAT)) AS '+DestinationColumnDataType+N' )'
																	WHEN DestinationColumnDataType='INT' THEN +N'CAST( REPLACE(Land.['+SourceColumnName+N'],'','','''') AS INT )'			
																	ELSE +'CAST(Land.['+SourceColumnName+N'] AS '+ISNULL(DestinationColumnDataType,'NVARCHAR(510)')+' )'		
																END
													+N' END '
												END	+N' AS ['+SourceColumnName+N'],'	
						FROM
							(	SELECT SourceColumnName,MappingId,ConvFunction,DestinationColumnDataType,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
							) DistinctColumns
						WHERE
							Rn=1	
						ORDER BY
							MappingId;

						SET @SelectClasue =  @SelectClasue+ CASE 
																WHEN @HistoryCnt=0 THEN  N' ''I'' AS ActionType FROM' 
																ELSE N' CASE WHEN Hist.Code IS NULL THEN ''I''  ELSE ''U'' END AS ActionType ' 
															END;

						IF @HistoryCnt=0 
							BEGIN
								--Generate Inner Query
								SET @DynamicSQL =N'';					
								SET @DynamicSQL =@DynamicSQL+N'( SELECT SourceID, Code,';	

								SELECT 
									@DynamicSQL = @DynamicSQL + N'L.['+SourceColumnName+N'],'																						
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
									) DistinctColumns
								WHERE
									Rn=1	
								ORDER BY
									MappingId;

								SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+N' FROM '+@StageLandSchema+@TableName ;

								--PRINT CAST( @InsertClause+' '+@SelectClasue+' '+@DynamicSQL AS NTEXT)
								--PRINT CAST(N' L WHERE NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE L.Code = Sub.Code AND L.SourceID =Sub.SourceID  )) Land' AS NTEXT)
			
								EXEC(	@InsertClause+' '+@SelectClasue+' '+@DynamicSQL
										+N' L WHERE NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE L.Code = Sub.Code AND L.SourceID =Sub.SourceID  )) Land' );

							END
						ELSE IF @HistoryCnt > 0
							BEGIN
								--Generate Inner Query
								SET @DynamicSQL=N'';					
								SET @DynamicSQL =@DynamicSQL+N'( SELECT SourceID, Code,';	
								
								SELECT 
									@DynamicSQL = @DynamicSQL + N'L.['+SourceColumnName+N'],'																						
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
									) DistinctColumns
								WHERE
									Rn=1	
								ORDER BY
									MappingId;

								SET @DynamicSQL =LEFT(@DynamicSQL,LEN(@DynamicSQL)-1) 
										+N' FROM '+@StageLandSchema+@TableName
										+N' L  WHERE  NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE L.Code = Sub.Code AND L.SourceID =Sub.SourceID  ) ) Land' ;
																		

								--Generate History Clause
								SET @HisotoryClause=N'';
								SET @HisotoryClause=N'( SELECT Code,';
					
								SELECT 
									@HisotoryClause = @HisotoryClause + N'['+SourceColumnName+N'],'
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
									) DistinctColumns
								WHERE
									Rn=1	
								ORDER BY
									MappingId

								SET @HisotoryClause =LEFT(@HisotoryClause,LEN(@HisotoryClause)-1) 
									+N' FROM '+@HistorySchema+@TableName+N'  WHERE HistEndDate IS NULL AND IsDuplicate=0 ) Hist' ;
											

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


								SET @WhereClause= @WhereClause+N' ('
								
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
									@WhereClause =@WhereClause+ N' ISNULL(Land.['+SourceColumnName+N'],'''') <> ISNULL(Hist.['+SourceColumnName+N'],'''')  OR'
								FROM
									(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
										FROM	ETLProcess.ETLSourceMapping 
										WHERE ProcessId=@ProcessId AND	( IsKey=0 AND DestinationColumnDataType IS NOT NULL)
									) DistinctColumns
								WHERE
									Rn=1	
								ORDER BY
									MappingId;

								SET @WhereClause = LEFT(@WhereClause,LEN(@WhereClause)-2)+N') ';

								--PRINT CAST (@InsertClause AS TEXT)
								--PRINT CAST (N' '+@SelectClasue AS TEXT)
								--PRINT CAST (N' FROM '+@DynamicSQL AS TEXT)
								--PRINT CAST (N' LEFT JOIN '+@HisotoryClause AS TEXT)	
								--PRINT CAST (N' '+@WhereClause AS TEXT)
															   						
								
								EXEC(	@InsertClause
										+N' '+@SelectClasue
										+N' FROM '+@DynamicSQL
										+N' LEFT JOIN '+@HisotoryClause	
										+N' '+@WhereClause								
									);
						END
					END						

					IF @MappingIsKeyCount =0
					BEGIN
						--Generate INSERT caluse
						SET @InsertClause=N'';
						SET @InsertClause = @InsertClause +N' INSERT INTO '+@StageProcessSchema+@TableName+N'( SourceID, Code, ';
						
						SELECT 
							@InsertClause = @InsertClause + N' ['+SourceColumnName+N'],'
						FROM
							(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND DestinationColumnDataType IS NOT NULL
							) DistinctColumns
						WHERE
							Rn=1	
						ORDER BY
							MappingId;

						SET @InsertClause=@InsertClause+N' ActionType,[HashBytes] )';


						--Generate SELECT Clause
						SET @SelectClasue=N'';
						SET @SelectClasue=N' SELECT DISTINCT Land.SourceID, Land.Code,';
						
						SELECT 
								@SelectClasue = @SelectClasue 
									+	CASE	WHEN ConvFunction IS NOT NULL THEN REPLACE(ConvFunction,SourceColumnName,N'Land.['+SourceColumnName+N']')
												WHEN DestinationColumnDataType IN('DATE' ,'DATETIME')THEN N'CONVERT(DATETIME, Land.['+SourceColumnName+N'], 120)'
										ELSE
											+N'  CASE WHEN ISNUMERIC( Land.['+SourceColumnName+N'])=1 AND CHARINDEX ( ''E+'', Land.['+SourceColumnName+N'] ) > 0 '+
															+N' THEN CAST(TRY_CONVERT(NUMERIC(17'+CASE WHEN DestinationColumnDataType=N'DECIMAL(17,2)' THEN N',2' ELSE N',0' END
																	+N' ), CAST(REPLACE(Land.['+SourceColumnName+N'],'','','''') AS FLOAT))  AS '+CASE WHEN DestinationColumnDataType LIKE 'DATE%' THEN N'NVARCHAR(510)' ELSE DestinationColumnDataType END+N' )'
												+N' ELSE '+
															CASE WHEN DestinationColumnDataType LIKE 'DECIMAL%' 
																	THEN N'CAST( CONVERT(NUMERIC(17'+CASE WHEN DestinationColumnDataType=N'DECIMAL(17,2)' THEN N',2' ELSE N',0' END
																+N' ), TRY_CAST(Land.['+SourceColumnName+N'] AS FLOAT)) AS '+DestinationColumnDataType+N' )'
																WHEN DestinationColumnDataType='INT' THEN +N'CAST( REPLACE(Land.['+SourceColumnName+N'],'','','''') AS INT )'		
																ELSE +N'CAST(Land.['+SourceColumnName+N'] AS '+DestinationColumnDataType+N' )'		
															END
								
											+N' END '
								END	+N' AS ['+SourceColumnName+N'],'	
						FROM
							(	SELECT SourceColumnName,MappingId,ConvFunction,DestinationColumnDataType,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND DestinationColumnName IS NOT NULL
							) DistinctColumns
						WHERE
							Rn=1	
						ORDER BY
							MappingId;

						SET @SelectClasue =  @SelectClasue+N'''I'' AS ActionType, Land.HByte FROM ';


						--Generate Inner Query
						SET @DynamicSQL=N'';					
						SET @DynamicSQL =@DynamicSQL+N'( SELECT Landing.SourceID, Landing.Code,';	

						SELECT 
							@DynamicSQL = @DynamicSQL + N' Landing.['+SourceColumnName+N'],'								
						FROM
							(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND DestinationColumnName IS NOT NULL
							) DistinctColumns
						WHERE
							Rn=1	
						ORDER BY
							MappingId;
							
						--Generate Landing column HashBytes
						SET @LandingHashByteClause= N'';
						SET @LandingHashByteClause= N' HASHBYTES (''SHA2_512'',CONCAT_WS(''|'',';
						
						SELECT 
							@LandingHashByteClause = @LandingHashByteClause + N' Landing.['+SourceColumnName+N'],'							
						FROM
							(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
								FROM	ETLProcess.ETLSourceMapping 
								WHERE ProcessId=@ProcessId AND DestinationColumnName IS NOT NULL AND IsKey=0
							) DistinctColumns
						WHERE
							Rn=1	
						ORDER BY
							MappingId;


						SET @LandingHashByteClause =  LEFT(@LandingHashByteClause,LEN(@LandingHashByteClause)-1)+N')) as Hbyte';
										
						IF @HistoryCnt=0  
							BEGIN					
								--print cast(@InsertClause as text)
								--print CAST(@SelectClasue AS TEXT)
								--print cast(@DynamicSQL as text)
								--PRINT CAST(@LandingHashByteClause AS TEXT)
								--PRINT N' FROM '+@StageLandSchema+@TableName+N' as Landing ) Land '										
								--PRINT N' WHERE NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE Land.Code = Sub.Code AND Land.SourceID =Sub.SourceID  )' 
								
								EXEC(	@InsertClause+' '+@SelectClasue+' '+@DynamicSQL+' '+@LandingHashByteClause
										+N' FROM '+@StageLandSchema+@TableName+N' as Landing ) Land '										
										+N' WHERE NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE Land.Code = Sub.Code AND Land.SourceID =Sub.SourceID  )' 
									);
					
							END
			
						ELSE IF @HistoryCnt > 0 
							BEGIN
								--print cast(@InsertClause as text)
								--print CAST(@SelectClasue AS TEXT)
								--print cast(@DynamicSQL as text)
								--PRINT CAST(@LandingHashByteClause AS TEXT)
								--PRINT N' FROM '+@StageLandSchema+@TableName+N' as Landing ) Land '										
								--PRINT N'	WHERE NOT EXISTS ( SELECT 1 FROM '+@HistorySchema+@TableName+N' Hist WHERE Hist.[HashBytes] = Land.HByte )'
								--PRINT N'	AND   NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE Land.Code = Sub.Code AND Land.SourceID =Sub.SourceID  )'				

								EXEC(	@InsertClause+' '+@SelectClasue+' '+@DynamicSQL+' '+@LandingHashByteClause
										+N' FROM '+@StageLandSchema+@TableName+N' as Landing) AS Land'
										+N'	WHERE NOT EXISTS ( SELECT 1 FROM '+@HistorySchema+@TableName+N' Hist WHERE HashBytes = Land.HByte )'
										+N'	AND   NOT EXISTS( SELECT 1 FROM '+@ErrSchema+@TableName+N' Sub WHERE Land.Code = Sub.Code AND Land.SourceID =Sub.SourceID  )' 
									);					
							END
					END

					SET @Inserted = @@ROWCOUNT

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Completed Loading to StageProcessing'
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'	
					,	@Inserts=@Inserted				
					
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'Process'
					,	@ProcessName = @ProcessName
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'
					,	@Stage = 'Processing'

					
				END TRY

				BEGIN CATCH
					SET @IsError=1

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Error Loading to StageProcessing'
					,	@Status = 'Error'
					,	@CurrentStatus = 'Error'	
											
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'Process'
					,	@ProcessName = @ProcessName
					,	@Status = 'Error'
					,	@CurrentStatus = 'Error'
					,	@Stage = 'Processing'

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
		BEGIN
			;THROW 50001, N'An error occurred while loading data to StageProcessing', 1;
		END
END