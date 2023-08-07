







      
CREATE procedure [ETLProcess].[PushToValuationEntity]      

AS       		

/****************************************************************************************
-- AUTHOR		: Rahul Singh
-- DATE			: 09/25/2020
-- PURPOSE		: Move Records From StageProcessing tables to Entity
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Rahul Singh	Original Version
******************************************************************************************/
		
		
		
		SET NOCOUNT ON;
		DECLARE @ProcessCategory VARCHAR(100)='DTC_StageEntityLoad_ETL';
		DECLARE @ProcessName VARCHAR(100)='PushToValuationEntity';
		DECLARE @ProcessStage VARCHAR(100)='Push Records From StageProcessing to Valuation Entity';
		DECLARE @ErroMessage VARCHAR(100)='Error Pushing Records From StageProcessing to Valuation Entity';

		DECLARE @ProcessID INT;
		DECLARE @IsAuditEntryExists INT;
		DECLARE @RunId INT;
		DECLARE @CurrentStatus VARCHAR(100);
		DECLARE @IsError BIT=0;
		DECLARE @ErrorProcedure VARCHAR(100);
		
		Declare @strSQLInsert nvarchar(max)=N''
		Declare @strSQLUpdate nvarchar(max)=N''
		DECLARE @HistoryStage VARCHAR(200);
		DECLARE @strSQLInvalidToValid nvarchar(max)=N''
		DECLARE @strSQLDeleteFromInvalid NVARCHAR(MAX)=N''
		DECLARE	@SelectClause NVARCHAR(MAX)=N'';			
		DECLARE	@InsertClause NVARCHAR(MAX)=N'';
		DECLARE @CurrentRunETLProcessUTC_StartedAt varchar(50);
		DECLARE @CurrentRunETLProcessUTC_CompletedAt varchar(50);

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
				@CurrentStatus = ETLStatus.Status,
				@CurrentRunETLProcessUTC_StartedAt=''''+Convert(varchar,ETLProcess.UTC_StartedAt,21)+'''',
				@CurrentRunETLProcessUTC_CompletedAt=''''+Convert(varchar,ETLProcess.UTC_CompletedAt,21)+''''
			FROM
				ETLAudit.ETLProcess

				INNER JOIN ETLProcess.ETLStatus
				ON ETLStatus.StatusId = ETLProcess.CurrentStatus
			WHERE
				RunId=@RunId
				AND ETLProcess.ProcessId = @ProcessId
				AND ETLProcess.CurrentStage = @ProcessStage;

		--SELECT @CurrentStatus

		IF @CurrentStatus NOT IN('Completed','Hold')

		BEGIN

		
			DECLARE @StageProcessingTableList Table(StageProcessingTableName Varchar(200))

			DROP TABLE IF EXISTS #SourceColumnTargetColumnXref

			CREATE TABLE #SourceColumnTargetColumnXref (SourceColumn Varchar(200),TargetColumn Varchar(200),rn Int)

			DROP TABLE IF EXISTS #Map

			CREATE TABLE #Map(SourceColumnName Varchar(500),DestinationCOlumnName Varchar(200))
		
			INSERT INTO @StageProcessingTableList
			SELECT DISTINCT 
			c.TABLE_SCHEMA+'.'+c.TABLE_NAME AS StageProcessingTableName
			
			FROM 
			INFORMATION_SCHEMA.COLUMNS c
			WHERE TABLE_SCHEMA='StageProcessing' 
			AND EXISTS(
						SELECT 1
						FROM  ETLProcess.ETLProcessCategory pc
						JOIN ETLProcess.ETLProcess p On pc.ProcessCategoryId=p.ProcessCategoryId
						WHERE p.ProcessName=c.TABLE_NAME  AND p.ActiveFlag=1 AND pc.ActiveFlag=1 
					  )
			
					  

			
			
			 
			

					DECLARE @EntityTableList Table(EntityTableName Varchar(200))
					INSERT INTO @EntityTableList
					SELECT DISTINCT 
						c.TABLE_SCHEMA+'.'+c.TABLE_NAME AS EntityTableName
						FROM 
						INFORMATION_SCHEMA.COLUMNS c
						WHERE TABLE_SCHEMA='dbo' AND c.TABLE_NAME IN('Valuation') 

			
			 

		
		
						DECLARE @StageProcessingTableName Varchar(200)
						DECLARE @EntityTableName Varchar(200)

						DECLARE  StageProcessingTableCursor CURSOR FOR       
							SELECT 
								StageProcessingTableName
								FROM 
								@StageProcessingTableList
		
						OPEN StageProcessingTableCursor;

				FETCH NEXT 
					FROM StageProcessingTableCursor
					INTO @StageProcessingTableName;

		WHILE @@FETCH_STATUS = 0       
		BEGIN

			---Entity Cursor Starts here
				
				DECLARE  EntityTableCursor CURSOR FOR       
				SELECT 
					EntityTableName
				FROM 
				@EntityTableList
				
		
				OPEN EntityTableCursor;

				FETCH NEXT 
				FROM EntityTableCursor
				INTO @EntityTableName;
				
					
					WHILE @@FETCH_STATUS = 0       

					BEGIN

							DECLARE @sourceRowCount int;
						IF EXISTS(SELECT 1 FROM sys.objects o Join sys.schemas s ON o.schema_id=s.schema_id WHERE s.name+'.'+o.name=@StageProcessingTableName)
	
							BEGIN
							DECLARE @SourceRowCountSQL nvarchar(max);
							SET @SourceRowCountSQL = N'select @sourceRowCount = count(*) from ' + @StageProcessingTableName;
	
							EXEC sp_executesql @SourceRowCountSQL, N'@sourceRowCount int output', @sourceRowCount = @sourceRowCount output;
							END
						
						Declare @InsertTargetColumnsList Varchar(max)
								,@InsertSourceColumnsList Varchar(max)
								,@UpdateColumnsList Varchar(max)

						--Get Source and Target Columns
						TRUNCATE TABLE #Map
						INSERT INTO #Map
						SELECT
						DISTINCT
						sourcecolumns=
				
							STUFF((SELECT   '+' + Case when (destinationcolumndatatype  like '%Dec%' or DestinationColumnDataType like '%int%'  ) and ISNULL(ConcatOrder,0)<>0 then   'ISNULL('+'s.['+SourceColumnName+']'+',0)' 
																when (destinationcolumndatatype  like '%char%'   ) and ISNULL(ConcatOrder,0)<>0 then 'ISNULL('+'s.['+SourceColumnName+']'+','''')' else 's.['+SourceColumnName+']' END
							--STUFF((SELECT   '+' + SourceColumnName
							FROM ETLProcess.ETLSourceMapping AS T2
							WHERE T1.DestinationColumnName = T2.DestinationColumnName and t1.ProcessId=t2.ProcessId -- LINK HERE
							order by ConcatOrder asc
							FOR XML PATH ('')),1, 1,'')
				
							,DestinationColumnName
							--,destinationcolumndatatype
				
					
						FROM    ETLProcess.ETLSourceMapping AS T1 where isnull(T1.DestinationColumnName,'')<>'' 
						AND  T1.ProcessId=(Select ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName=REPLACE(@StageProcessingTableName,'StageProcessing.',''))

						Update #Map Set SourceColumnName=REPLACE(SourceColumnName,'+','+'' ''+') Where SourceColumnName like '%,''%'

						

						TRUNCATE TABLE #SourceColumnTargetColumnXref
						INSERT INTO #SourceColumnTargetColumnXref
						SELECT  DISTINCT 
							Source.SourceColumnName
							,Target.EntityTableColumnName
							,Dense_Rank()Over( Order By Source.sourceColumnName ) as rn
							
						FROM 
						(

						SELECT DISTINCT
							 SourceColumnName ,DestinationCOlumnName from #Map
						)Source 
						
						JOIN
						(
						SELECT DISTINCT
							c.COLUMN_NAME AS EntityTableColumnName
							FROM 
							INFORMATION_SCHEMA.COLUMNS c
							WHERE c.TABLE_SCHEMA+'.'+c.TABLE_NAME=@EntityTableName
							And C.COLUMN_NAME<>'PIN'
							
						)Target ON Target.EntityTableColumnName=Source.DestinationColumnName
						
						
						

						Set @InsertTargetColumnsList=(
												STUFF(
													(SELECT ', ' + TargetColumn
													FROM #SourceColumnTargetColumnXref t2 ORDER BY rn ASC
          
													FOR XML PATH ('')), 1, 1, '')  
												)

						
						Set @InsertSourceColumnsList=(
												STUFF(
													(SELECT ', ' +SourceColumn
													FROM #SourceColumnTargetColumnXref t2 ORDER BY rn ASC
          
													FOR XML PATH ('')), 1, 1, '')  
												)	
						Set @UpdateColumnsList=(
												STUFF(
													(SELECT ', ' +'t.'+TargetColumn+' = ' +SourceColumn
													FROM #SourceColumnTargetColumnXref t2 ORDER BY rn ASC
          
													FOR XML PATH ('')), 1, 1, '')  
												)

						IF EXISTS (SELECT 1 FROM #SourceColumnTargetColumnXref) AND @sourceRowCount>0
						BEGIN
								
							BEGIN TRY
								SET @HistoryStage =  'Start inserts for '+@StageProcessingTableName+N'';

									EXEC ETLProcess.AuditLog
										@ProcessCategory = @ProcessCategory
									,	@Phase = 'ProcessHistory'
									,	@ProcessName = @ProcessName
									,	@Stage = @HistoryStage
									,	@Status = 'InProgress'
									,	@CurrentStatus = 'Started';
						
							--Select @InsertTargetColumnsList,@InsertSourceColumnsList,@UpdateColumnsList
							--Inserts
							SET @strSQLInsert=@strSQLInsert+' INSERT INTO '+@EntityTableName+'('+'Code,'+@InsertTargetColumnsList+' ,Data_Source_ID, Data_Source_Priority,DateCreatedUTC,LastModifiedDateUTC '+') SELECT s.Code,'+  @InsertSourceColumnsList+' ,SUBSTRING(s.code, 1, CHARINDEX(''_'', s.code) -1) as Data_Source_ID, (Select Distinct Data_Source_Priority From ETLProcess.ETLProcess Where ProcessId= SUBSTRING(s.code, 1, CHARINDEX(''_'', s.code) -1)) As Data_Source_Priority,'+@CurrentRunETLProcessUTC_StartedAt+', '+@CurrentRunETLProcessUTC_StartedAt+' ' +' FROM '+@StageProcessingTableName+' s LEFT JOIN '+@EntityTableName+ ' t ON s.Code=t.Code WHERE ActionType=''I'' AND t.Code IS NULL AND s.IsDuplicate=0 ;'
							
							SET @HistoryStage =  'Completed inserts for '+@StageProcessingTableName+N'' ;

							EXECUTE sp_executesql @statement = @strSQLInsert
							
								EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = @HistoryStage
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Inserts = @@ROWCOUNT;
							--Updates
								
								SET @HistoryStage =  'Start Updates for '+@StageProcessingTableName;

								--Check if Update records from StageProcessing are moved to Invalid

								SET @InsertClause=N'';
								SET @InsertClause = N' INSERT INTO '+@EntityTableName+N' (';
			
								SELECT  
									@InsertClause = @InsertClause+COLUMN_NAME+N' ,'
								FROM 
									INFORMATION_SCHEMA.COLUMNS c
								WHERE 
									TABLE_SCHEMA+'.'+TABLE_NAME=@EntityTableName
									AND COLUMN_NAME NOT IN('IsValid','ID','LastModifiedDateUTC')
								ORDER BY 
									COLUMN_NAME;
			
								SET @InsertClause= @InsertClause+N'LastModifiedDateUTC)';

								SET @SelectClause=N'';
								SET @SelectClause = N' SELECT ';
				
								SELECT  
									@SelectClause = @SelectClause+'e.'+COLUMN_NAME+N','
								FROM 
									INFORMATION_SCHEMA.COLUMNS c
								WHERE 
									TABLE_SCHEMA+'.'+TABLE_NAME=@EntityTableName
									AND COLUMN_NAME NOT IN('IsValid','ID','LastModifiedDateUTC')
								ORDER BY 
									COLUMN_NAME;

								SET @SelectClause= @SelectClause+N' GETUTCDATE() FROM '+@EntityTableName+N'_Invalid'+N' e JOIN '+@StageProcessingTableName+N' s ON s.Code=e.Code WHERE s.ActionType=''U''  AND s.IsDuplicate=0 ;'			

								SET @strSQLInvalidToValid=@InsertClause+@SelectClause

								SET @strSQLDeleteFromInvalid=@strSQLDeleteFromInvalid+N' DELETE e  FROM '+@EntityTableName+N'_Invalid'+N' e JOIN '+@StageProcessingTableName+N' s ON s.Code=e.Code WHERE s.ActionType=''U''  AND s.IsDuplicate=0 ;'

								EXECUTE sp_executesql @statement = @strSQLInvalidToValid

								EXECUTE sp_executesql @statement = @strSQLDeleteFromInvalid

								SET @strSQLUpdate=@strSQLUpdate+' UPDATE t SET '+@UpdateColumnsList+' ,t.LastModifiedDateUTC =  '+@CurrentRunETLProcessUTC_StartedAt+' FROM '+@StageProcessingTableName+' s JOIN '+@EntityTableName+' t ON t.Code=s.Code Where s.ActionType=''U''  AND s.IsDuplicate=0  ;'

								EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = @HistoryStage
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

								SET @HistoryStage =  'Completed Updates for '+@StageProcessingTableName;

								EXECUTE sp_executesql @statement = @strSQLUpdate

								EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = @HistoryStage
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@updates = @@ROWCOUNT

								


							END TRY
							BEGIN CATCH
								SET @IsError=1
								SELECT 
									@ErrorProcedure= s.name+'.'+o.name 
								FROM 
									SYS.OBJECTS O 
	
									INNER JOIN SYS.SCHEMAS S 
									ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

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
								,	 @ErrorProcedure  
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
						

						FETCH NEXT
						FROM
						EntityTableCursor INTO @EntityTableName ; 
						
					END

					CLOSE EntityTableCursor;      
					DEALLOCATE EntityTableCursor;      
		
					-- Entity Cursor Ends here
				 FETCH NEXT			
				FROM      
				StageProcessingTableCursor INTO @StageProcessingTableName ;   
				END           
				CLOSE StageProcessingTableCursor;      
				DEALLOCATE StageProcessingTableCursor;      
			
	
				--	Select @strSQL

				--EXECUTE sp_executesql @statement = @strSQL 

			IF @IsError=1
			THROW 50001, @ErroMessage, 1;	

			IF @IsError=0
			EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'Process'
								,	@ProcessName = @ProcessName
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'
								,	@Stage = @ProcessStage;
		
		END --IF @CurrentStatus NOT IN('Completed','Hold')

		
			
	END --IF ISNULL(@ProcessID,0) > 0 AND ISNULL(@RunId,0) > 0