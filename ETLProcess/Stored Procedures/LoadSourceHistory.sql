

CREATE PROCEDURE [ETLProcess].[LoadSourceHistory]	
	@ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL'
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Loading the Historoy information for the Source.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE	@TableName VARCHAR(100);
	DECLARE	@ProcessId INT;
	DECLARE	@ProcessName VARCHAR(100);
	DECLARE	@RunId INT;
	DECLARE	@StageLandSchema VARCHAR(50)='StageLanding.';
	DECLARE	@StageProcessSchema VARCHAR(50)='StageProcessing.';
	DECLARE	@HistorySchema VARCHAR(50)='SourceHistory.';
	DECLARE	@DynamicSQL NVARCHAR(MAX)=NULL;
	DECLARE	@InsertClause NVARCHAR(max);
	DECLARE	@Params NVARCHAR(1000)='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
	DECLARE	@MappingIsKeyCount INT
	DECLARE	@HistoryCount INT;
	DECLARE	@ProcessingCount INT;
	DECLARE	@i INT=1;
	DECLARE	@CurrentStatus VARCHAR(100) ;
	DECLARE @IsAuditEntryExists INT;
	DECLARE @Inserted INT;
	DECLARE @Updated INT;
	DECLARE @IsError BIT=0;
	DECLARE	@LandingClause NVARCHAR(max);
	DECLARE	@ProcessingClause NVARCHAR(max);
	DECLARE	@WhereClause NVARCHAR(max);
	DECLARE	@SelectClause NVARCHAR(max);
	DECLARE @ErrorProcedure VARCHAR(100);

	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
		SYS.OBJECTS O 
		
		INNER JOIN SYS.SCHEMAS S 
		ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;
		
	DROP TABLE IF EXISTS #ETLProcess;

	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Get RunId for Loading to SourceHistory'
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
	,	@Stage ='Got RunId for Loading to SourceHistory'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Inserts=0;
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Get list of Processes to load to SourceHistory'
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
		ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId
	WHERE 
		ETLProcessCategory.ProcessCategoryName = @ProcessCategory
		AND ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1;
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'ProcessHistory'
	,	@ProcessName = ''
	,	@Stage ='Obtained list of processes to load to SourceHistory'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Inserts=@@ROWCOUNT;
	
	DECLARE CursorStageHistory CURSOR
	FOR 
		SELECT 
			ProcessId
		,	ProcessName		
		FROM 
			#ETLProcess	
	OPEN CursorStageHistory
		FETCH NEXT FROM CursorStageHistory INTO  @ProcessId,@ProcessName	

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
				AND CurrentStage='History'
			GROUP BY
				ETLStatus.Status;							
			
			IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Stage = 'History'
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started'	;															
			
			SET @CurrentStatus='';
			SELECT 
				@CurrentStatus = ETLStatus.Status
			FROM
				ETLAudit.ETLProcess

				INNER JOIN ETLProcess.ETLStatus
				ON ETLStatus.StatusId = ETLProcess.CurrentStatus
			WHERE
				RunId=@RunId
				AND ETLProcess.ProcessId = @ProcessId
				AND ETLProcess.CurrentStage = 'History';

			IF @ProcessCategory='DTC_ExternalSource_ETL'
				BEGIN
					SET @TableName='';
					SELECT 	
						@TableName	= t.name 
					FROM 
						SYS.TABLES t 
					WHERE 
						SCHEMA_NAME(t.SCHEMA_ID) = 'SourceHistory' 
						AND t.name = @ProcessName;		
				END
			ELSE IF @ProcessCategory='DTC_InternalSource_ETL'
				BEGIN
					SELECT 	@TableName	= @ProcessName;
				END

			SET @ProcessingCount =0;

			IF EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageProcessing'	AND TABLE_NAME = @TableName) AND ISNULL(@TableName,'')<>''
			BEGIN				
				SET @DynamicSQL=N'';
				SET @DynamicSQL=N' SELECT @CntOP = COUNT(1) FROM  '+	@StageProcessSchema +@TableName;
				SET @Params =N'@HistorySchema VARCHAR(50),@TableName VARCHAR(100), @CntOP INT OUTPUT';
				EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageProcessSchema,@TableName,@CntOP = @ProcessingCount OUTPUT;
			END

			IF @ProcessingCount=0 AND  @CurrentStatus<>'Completed'
			BEGIN
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage ='No records present in StageProcessing'
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'	
				,	@Inserts=@Inserted
				,	@Updates = @Updated;

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Stage = 'History';
			END

			IF @CurrentStatus<>'Completed' AND @ProcessingCount > 0 AND ISNULL(@TableName,'')<>''
				AND EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageProcessing'	AND TABLE_NAME = @TableName) 
				AND EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'SourceHistory'	AND TABLE_NAME = @TableName)
			BEGIN	
				BEGIN TRY
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Start Loading to SourceHistory'
					,	@Status = 'InProgress'
					,	@CurrentStatus = 'Started'	;
					
					SET @HistoryCount =0;
					SET @DynamicSQL=N'';
					SET @DynamicSQL=N' SELECT @CntOP = COUNT(1) FROM  '+	@HistorySchema +@TableName;
					SET @Params =N'@HistorySchema VARCHAR(50),@TableName VARCHAR(100), @CntOP INT OUTPUT';
					EXECUTE sp_executesql 	@DynamicSQL,@Params,@HistorySchema,@TableName,@CntOP = @HistoryCount OUTPUT;
					
					SELECT 
						@MappingIsKeyCount = COUNT(1)
					FROM
						ETLProcess.ETLSourceMapping							
					WHERE
						ProcessId =@ProcessId
						AND IsKey=1;
			
					IF @HistoryCount > 0 AND @MappingIsKeyCount > 0
					BEGIN
						SET @DynamicSQL=N'';
						SET @DynamicSQL =	
									 N' UPDATE History '+
									+N' SET HistEndDate=CAST(CAST(GETUTCDATE()-1 as DATE) as DATETIME)  '
									+N' FROM '+@HistorySchema+@TableName 
									+N' History INNER JOIN '+@StageProcessSchema+@TableName+N' Processing ON History.Code =Processing.Code'
									+N' WHERE Processing.ActionType=''U'' AND History.HistEndDate IS NULL';
						EXEC (@DynamicSQL);
						
						SET @Updated=@@ROWCOUNT;
					END										
					
					/**********************
					Generate INSERT caluse
					********************/								
					SET @InsertClause = N' INSERT INTO '+@HistorySchema+@TableName+N' ( Code,IsDuplicate, ';					

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

					SET @InsertClause= @InsertClause+CASE WHEN @MappingIsKeyCount =0 THEN N' [HashBytes], HistEndDate  ' ELSE 'HistEndDate' END +N' )';
					
					
					/********************
					PROCESSING CLAUSE
					**********************/										
					SET @ProcessingClause=N' SELECT Processing.Code, Processing.IsDuplicate,';	
											
					SELECT 
						@ProcessingClause=@ProcessingClause+N' Landing.['+SourceColumnName+N'],'
					FROM
						(	SELECT SourceColumnName,MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn 
							FROM	ETLProcess.ETLSourceMapping 
							WHERE ProcessId=@ProcessId AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
						) DistinctColumns
					WHERE
						Rn=1
					ORDER BY
						MappingId;
					
					IF @MappingIsKeyCount=0
						SET @ProcessingClause = @ProcessingClause+N' Processing.[HashBytes],';
					
					SET @ProcessingClause = @ProcessingClause
							+N' CASE WHEN Processing.IsDuplicate=1 THEN CAST(CAST(GETUTCDATE() as DATE) as DATETIME) ELSE NULL END HistEndDate  FROM '
							+@StageProcessSchema+@TableName+N' Processing' 
							+N' INNER JOIN '+@StageLandSchema+@TableName+N' Landing ON Landing.SourceID = Processing.SourceID';							
					
					--PRINT @InsertClause
					--PRINT @ProcessingClause	
					EXECUTE (@InsertClause+N' '+@ProcessingClause);
					SET @Inserted=@@ROWCOUNT;					

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Completed Loading to SourceHistory'
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'	
					,	@Inserts=@Inserted
					,	@Updates = @Updated;

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'Process'
					,	@ProcessName = @ProcessName
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'
					,	@Stage = 'History';
				END TRY

				BEGIN CATCH
					SET @IsError=1;

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Error Loading to SourceHistory'
					,	@Status = 'Error'
					,	@CurrentStatus = 'Error'	;					

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'Process'
					,	@ProcessName = @ProcessName
					,	@Status = 'Error'
					,	@CurrentStatus = 'Error'
					,	@Stage = 'History';

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
					,	@ErrorMessage='Failed to Load data to SourceHistory'
					,	@IsError='Yes';

				END CATCH
			END
			
			FETCH NEXT FROM CursorStageHistory INTO 	@ProcessId,@ProcessName
		END

	CLOSE CursorStageHistory
	DEALLOCATE CursorStageHistory	

	IF @IsError=1
		THROW 50005, N'An error occurred while loading data to sourceHistory', 1;
		

END