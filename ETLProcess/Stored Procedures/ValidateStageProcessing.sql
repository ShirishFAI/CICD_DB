





CREATE PROCEDURE [ETLProcess].[ValidateStageProcessing]
	@ProcessCategory VARCHAR(100)=''
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Validate the Processing table data
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE	@Params NVARCHAR(2000)=N'';
	DECLARE @DistRowCnt INT=0;
	DECLARE @DistKeyCnt INT=0;
	DECLARE @KeyCount INT=0;
	DECLARE @ProcessingCnt INT=0;
	DECLARE	@StageProcessSchema VARCHAR(50)='StageProcessing.';
		
	DECLARE	@ProcessId INT=0;
	DECLARE	@ProcessName VARCHAR(100)='';
	DECLARE	@TableName VARCHAR(100)='';
	DECLARE	@DynamicSQL NVARCHAR(MAX)=N'';
	DECLARE	@PotentialDupSQL NVARCHAR(MAX)=N'';

	DECLARE @ProcessStage VARCHAR(100)='Validate Stage Processing';
	DECLARE @HistoryStage VARCHAR(200);
	DECLARE @ErroMessage VARCHAR(100)='Error Validate Stage Processing';
	DECLARE @IsAuditEntryExists INT;
	DECLARE @RunId INT;
	DECLARE @CurrentStatus VARCHAR(100);
	DECLARE @IsError BIT=0;
	DECLARE @ErrorProcedure VARCHAR(100);


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

	--SET @ProcessId=22;
	--SET @ProcessName='AB_GrandePrairie_Development_Permits___Nov_2011_to_Current';
	
	--SELECT @ProcessId = ProcessId FROM ETLProcess.ETLProcess WHERE	ProcessName=@ProcessName;

	DECLARE ActiveProcessesCursor CURSOR
	FOR 
		SELECT 
			ETLProcess.ProcessId
		,	ETLProcess.ProcessName			
		FROM
			ETLProcess.ETLProcess 

			INNER JOIN ETLProcess.ETLProcessCategory
			ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId
		WHERE
			ETLProcess.ActiveFlag=1
			AND ETLProcessCategory.ActiveFlag=1
			AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory;

	OPEN ActiveProcessesCursor	
	FETCH NEXT FROM ActiveProcessesCursor INTO  @ProcessId,@ProcessName
	
	WHILE @@FETCH_STATUS = 0	
	BEGIN
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

				SET @TableName=@ProcessName ;

			IF @CurrentStatus NOT IN('Completed','Hold') AND EXISTS ( SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS    WHERE TABLE_SCHEMA = 'StageProcessing'	AND TABLE_NAME = @TableName)
			BEGIN
				SELECT @KeyCount = COUNT(1) FROM ETLProcess.ETLSourceMapping	WHERE ProcessId =@ProcessId	AND ETLSourceMapping.IsKey=1;

				SET @ProcessingCnt =0;
				SET @DynamicSQL=N'';
				SET @DynamicSQL= N' SELECT @CntOP = COUNT(1) FROM  '+@StageProcessSchema +@TableName;
				SET @Params ='@StageProcessSchema VARCHAR(50),@TableName VARCHAR(100), @CntOP INT OUTPUT';
				EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageProcessSchema,@TableName,@CntOP = @ProcessingCnt	OUTPUT;

				IF @ProcessingCnt > 0
				BEGIN
					SET @DynamicSQL=N' ;WITH DuplicateCTE AS ( SELECT ROW_NUMBER() OVER(PARTITION BY';

					IF @KeyCount > 0
					BEGIN
						SELECT 
							@DynamicSQL = @DynamicSQL + N' ['+SourceColumnName+N'],'
						FROM
							ETLProcess.ETLSourceMapping
						WHERE
							ProcessId=@ProcessId		
							AND IsKey=1;		
					END
					ELSE
					BEGIN
						SELECT 
							@DynamicSQL = @DynamicSQL + N' ['+SourceColumnName+N'],'
						FROM
							ETLProcess.ETLSourceMapping
						WHERE
							ProcessId=@ProcessId		
							AND DestinationColumnName IS NOT NULL;	
					END
	
					SET @DynamicSQL = LEFT(@DynamicSQL,LEN(@DynamicSQL)-1);
					SET @DynamicSQL=@DynamicSQL+N' ORDER BY Code ASC) Rn,IsDuplicate FROM '+@StageProcessSchema+@TableName+N' )  UPDATE DuplicateCTE  SET IsDuplicate = CASE WHEN Rn=1 THEN 0 ELSE 1 END';	
					BEGIN TRY
						SET @HistoryStage =  'Started Validate StageProcessing For '+@TableName+N'';

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started';

					SET @HistoryStage =  'Completed Validate StageProcessing For '+@TableName+N'';

					EXECUTE (@DynamicSQL);
					
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage = @HistoryStage
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'	
					,	@Updates = @@ROWCOUNT;
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
					,	@Stage ='Error Validate Stage Processingh'
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
					--PRINT @DynamicSQL;	
					IF @IsError=1
					THROW 50001, @ErroMessage, 1;	
				END	
				
			END --IF @CurrentStatus NOT IN('Completed','Hold')
			
			EXEC ETLProcess.AuditLog
				@ProcessCategory = @ProcessCategory
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Status = 'Completed'
			,	@CurrentStatus = 'Completed'
			,	@Stage = @ProcessStage;											
		END --IF ISNULL(@ProcessID,0) > 0 AND ISNULL(@RunId,0) > 0
		
		FETCH NEXT FROM ActiveProcessesCursor INTO 	@ProcessId,@ProcessName
	END

	CLOSE ActiveProcessesCursor
	DEALLOCATE ActiveProcessesCursor	
END