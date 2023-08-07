
CREATE PROCEDURE [ETLProcess].[CreateSourceStagingTable]
	@ProcessCategory VARCHAR(100)
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Create staging tables for the spcefied Process
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	DECLARE @LandSchema VARCHAR(50)='[StageLanding].';
	DECLARE	@ProcessSchema VARCHAR(50)='[StageProcessing].';
	DECLARE	@HistorySchema VARCHAR(50)='[SourceHistory].';
	DECLARE	@ErrorSchema VARCHAR(50)='[StageProcessingErr].';
	DECLARE	@DynamicSQL NVARCHAR(MAX);	
	DECLARE	@MappingColumnCount INT;
	DECLARE	@ProcessID INT;
	DECLARE @IsKeyCount INT;
	DECLARE @IsProcessActive INT;
	DECLARE @MappingCount INT;
	DECLARE	@ProcessName VARCHAR(100);
	DECLARE @ProcessCategoryName VARCHAR(100);
	DECLARE	@IsError BIT;
	DECLARE @ErrorText VARCHAR(200);
	DECLARE @ErrorProcedure VARCHAR(100);

	TRUNCATE TABLE Stage.ExternalFileslist;

	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
		SYS.OBJECTS O 
	
		INNER JOIN SYS.SCHEMAS S 
		ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;	

	DECLARE CursorGetActiveProcesses CURSOR 
	FOR 
       	SELECT					
			ETLProcess.ProcessId
		,	ETLProcess.ProcessName
		,	ETLProcessCategory.ProcessCategoryName
		FROM
			ETLProcess.ETLProcess

			INNER JOIN ETLProcess.ETLProcessCategory
			ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId
		WHERE
			ETLProcess.ActiveFlag=1
			AND ETLProcessCategory.ActiveFlag=1			
			AND  1 = ( 
						CASE WHEN @ProcessCategory='ALL' AND ETLProcessCategory.ProcessCategoryName IN('DTC_InternalSource_ETL','DTC_ExternalSource_ETL')THEN 1
							 WHEN @ProcessCategory='DTC_InternalSource_ETL' AND ETLProcessCategory.ProcessCategoryName='DTC_InternalSource_ETL' THEN 1
							 WHEN @ProcessCategory='DTC_ExternalSource_ETL' AND ETLProcessCategory.ProcessCategoryName='DTC_ExternalSource_ETL' THEN 1
							 ELSE 0
						END
				)	

	OPEN CursorGetActiveProcesses
	FETCH NEXT FROM CursorGetActiveProcesses INTO 	@ProcessID,@ProcessName, @ProcessCategoryName

	WHILE @@FETCH_STATUS = 0
	BEGIN	

		--Check if mapping data does not exists
		SELECT 
			@MappingCount=COUNT(1)
		FROM
			ETLProcess.ETLProcess

			INNER JOIN ETLProcess.ETLSourceMapping
			ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
		WHERE
			ETLProcess.ProcessName =@ProcessName;

		--IF @MappingCount=0  
		--BEGIN
		--	EXEC ETLProcess.EmailNotification
		--		@ProcessCategory=@ProcessCategory
		--	,	@ProcessName= @ProcessName
		--	,	@ProcessStage='Table Creation'
		--	,	@ErrorMessage='No mapping data exists and failed to create table'
		--	,	@IsError='Yes';

		--	--SET @IsError=1
		--	--SET @ErrorText = 'No mapping data exists and failed to create table'
		--END
		
		----All Good, Start
		IF @MappingCount > 0  
		BEGIN
			BEGIN TRY
				--Check if the Land table exists
				IF OBJECT_ID( @LandSchema+@ProcessName, 'U') IS NULL
				BEGIN					
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Create StageLanding Table'
					,	@Status = 'InProgress'
					,	@CurrentStatus = 'Started'	;

					SET @DynamicSQL= N'';
					SET @DynamicSQL= N'CREATE TABLE '+@LandSchema+@ProcessName+ N' ( '
			
					SELECT
						@DynamicSQL = @DynamicSQL+N'  ['+REPLACE(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(SourceColumnName, CHAR(10), CHAR(32)),CHAR(13), CHAR(32)),CHAR(160), CHAR(32)),CHAR(9),CHAR(32)))),']',N']]')+N'] '
						+ CASE	WHEN DestinationColumnDataType IN('VARCHAR(MAX)','VARCHAR(2000)','VARCHAR(4000)') THEN N'NVARCHAR(MAX)' 
								ELSE	
									CASE	WHEN @ProcessCategoryName='DTC_InternalSource_ETL' THEN N'NVARCHAR(510)'
											ELSE N'NVARCHAR(MAX)' 
									END
						  END	+N' ,'
					FROM
						(	SELECT 
								ETLSourceMapping.SourceColumnName,ETLSourceMapping.DestinationColumnDataType,ETLSourceMapping.MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn
							FROM
								ETLProcess.ETLProcess

								INNER JOIN ETLProcess.ETLSourceMapping
								ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
							WHERE
								ETLProcess.ProcessName =@ProcessName								
						) DistinctColumns
					WHERE
						Rn=1
					ORDER BY 
						MappingId

					SET @DynamicSQL=LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+N' )'	;		
					EXECUTE(@DynamicSQL);

					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Completed Creating StageLanding Table'
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'	;			
				END

				SET @DynamicSQL=N'TRUNCATE TABLE '+@LandSchema+@ProcessName	;		
				EXECUTE(@DynamicSQL);
	
				--Check if the Process tables exists
				IF OBJECT_ID( @ProcessSchema+@ProcessName, 'U') IS NULL
				BEGIN	
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Create StageProcessing Table'
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	;

						SELECT 
							@IsKeyCount = COUNT(1)
						FROM
							ETLProcess.ETLProcess

							INNER JOIN ETLProcess.ETLSourceMapping
							ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
						WHERE
							ETLProcess.ProcessName =@ProcessName
							AND ETLSourceMapping.IsKey=1
						
						
						SET @DynamicSQL= N'';
						SET @DynamicSQL= N'CREATE TABLE '+@ProcessSchema+@ProcessName+ N' ( SourceID INT, Code VARCHAR(200) , ';

						SELECT
							@DynamicSQL = @DynamicSQL+N'  ['+REPLACE(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(SourceColumnName, CHAR(10), CHAR(32)),CHAR(13), CHAR(32)),CHAR(160), CHAR(32)),CHAR(9),CHAR(32)))),']',N']]')+N'] '
							+ CASE WHEN DestinationColumnDataType IS NULL THEN N'NVARCHAR(510)' ELSE CASE WHEN DestinationColumnDataType='VARCHAR(-1)' THEN N'VARCHAR(MAX)' ELSE DestinationColumnDataType END END +N','
						FROM
						(
							SELECT 
								ETLSourceMapping.SourceColumnName,ETLSourceMapping.DestinationColumnDataType,ETLSourceMapping.MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn
							FROM
								ETLProcess.ETLProcess

								INNER JOIN ETLProcess.ETLSourceMapping
								ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
							WHERE
								ETLProcess.ProcessName =@ProcessName
								AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
						) DistinctColumns
						WHERE
							Rn=1
						ORDER BY 
							MappingId
			
						IF @IsKeyCount=0
							SET @DynamicSQL=@DynamicSQL+ N'HashBytes BINARY(64),'

						SET @DynamicSQL=@DynamicSQL+  N' ActionType Char(1),IsDuplicate BIT )' 
						EXECUTE(@DynamicSQL);

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Completed Creating StageProcessing Table'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	;
			
					END
				SET @DynamicSQL=N'TRUNCATE TABLE '+@ProcessSchema+@ProcessName	;		
				EXECUTE(@DynamicSQL);

				----Check if the History table exists
				IF OBJECT_ID( @HistorySchema+@ProcessName, 'U') IS NULL
				BEGIN	
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Create SourceHistory Table'
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	;

						SET @DynamicSQL= N'';
						SET @DynamicSQL= N'CREATE TABLE '+@HistorySchema+@ProcessName+ N' (  Code VARCHAR(200) ,';
			
						SELECT 
							@IsKeyCount = COUNT(1)
						FROM
							ETLProcess.ETLProcess

							INNER JOIN ETLProcess.ETLSourceMapping
							ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
						WHERE
							ETLProcess.ProcessName =@ProcessName
							AND ETLSourceMapping.IsKey=1;		
													
						SELECT
							@DynamicSQL = @DynamicSQL+N'  ['+REPLACE(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(SourceColumnName, CHAR(10), CHAR(32)),CHAR(13), CHAR(32)),CHAR(160), CHAR(32)),CHAR(9),CHAR(32)))),']',N']]')+N'] '
							+ CASE	WHEN DestinationColumnDataType IN('VARCHAR(MAX)','VARCHAR(2000)','VARCHAR(4000)') THEN N'NVARCHAR(MAX)' 
									ELSE 
										CASE	WHEN @ProcessCategoryName='DTC_InternalSource_ETL' THEN N'NVARCHAR(510)'
												ELSE N'NVARCHAR(MAX)' 
										END
							  END	+N' ,'
						FROM
						(
							SELECT 
									ETLSourceMapping.SourceColumnName,ETLSourceMapping.DestinationColumnDataType,ETLSourceMapping.MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn
							FROM
								ETLProcess.ETLProcess

								INNER JOIN ETLProcess.ETLSourceMapping
								ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
							WHERE
								ETLProcess.ProcessName =@ProcessName
								AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
						) DistinctColumns
						WHERE
							Rn=1
						ORDER BY 
							MappingId

						IF @IsKeyCount=0
							SET @DynamicSQL=@DynamicSQL+ N'HashBytes BINARY(64),'

						SET @DynamicSQL=@DynamicSQL+N' HistEndDate DateTime, IsDuplicate BIT )';			
						EXECUTE(@DynamicSQL);

						SET @DynamicSQL=N'';
						SET @DynamicSQL= N'CREATE CLUSTERED INDEX CI_SourceHistory_'+@ProcessName+N'_Code ON SourceHistory.'+@ProcessName+N'(Code);';
						SET @DynamicSQL= @DynamicSQL+N' CREATE NONCLUSTERED INDEX NCI_SourceHistory_'+@ProcessName+N'_HistEndDate ON SourceHistory.'+@ProcessName+N'(HistEndDate);';
						EXECUTE(@DynamicSQL);

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Completed Creating SourceHistory Table'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	;
			
					END
	
				----Check if the Error table exists
				IF OBJECT_ID( @ErrorSchema+@ProcessName, 'U') IS NULL
				BEGIN	
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Create StageProcessingErr Table'
					,	@Status = 'InProgress'
					,	@CurrentStatus = 'Started'	;

					SET @DynamicSQL=N'';
					SET @DynamicSQL=N'CREATE TABLE '+@ErrorSchema+@ProcessName+ N' (  SourceID INT , Code Varchar(200),ErrorStatusId TINYINT,';

					SELECT
						@DynamicSQL = @DynamicSQL+N'  ['+REPLACE(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(SourceColumnName, CHAR(10), CHAR(32)),CHAR(13), CHAR(32)),CHAR(160), CHAR(32)),CHAR(9),CHAR(32)))),']',']]')+N'] '
						+ CASE	WHEN DestinationColumnDataType IN('VARCHAR(MAX)','VARCHAR(2000)','VARCHAR(4000)') THEN N'NVARCHAR(MAX)' 
								ELSE 
									CASE	WHEN @ProcessCategoryName='DTC_InternalSource_ETL' THEN N'NVARCHAR(510)'
											ELSE N'NVARCHAR(MAX)' 
									END
						  END	+N' ,'
					FROM
					(
						SELECT 
								ETLSourceMapping.SourceColumnName,ETLSourceMapping.DestinationColumnDataType,ETLSourceMapping.MappingId,ROW_NUMBER() OVER( PARTITION BY SourceColumnName ORDER BY MappingID) AS Rn
						FROM
							ETLProcess.ETLProcess

							INNER JOIN ETLProcess.ETLSourceMapping
							ON ETLProcess.ProcessId = ETLSourceMapping.ProcessId
						WHERE
							ETLProcess.ProcessName =@ProcessName
							AND	( DestinationColumnName IS NOT NULL OR IsKey=1)
					) DistinctColumns
					WHERE
						Rn=1
					ORDER BY 
						MappingId

					SET @DynamicSQL=LEFT(@DynamicSQL,LEN(@DynamicSQL)-1)+' )'	;		

					EXECUTE(@DynamicSQL);					
		
					EXEC ETLProcess.AuditLog
						@ProcessCategory = @ProcessCategory
					,	@Phase = 'ProcessHistory'
					,	@ProcessName = @ProcessName
					,	@Stage ='Completed Creating StageProcessingErr Table'
					,	@Status = 'Completed'
					,	@CurrentStatus = 'Completed'	;
				END
			END TRY

			BEGIN CATCH
				EXEC ETLProcess.EmailNotification
					@ProcessCategory=@ProcessCategory
				,	@ProcessName= @ProcessName
				,	@ProcessStage='Table Creation'
				,	@ErrorMessage='Failed to create table'
				,	@IsError='Yes';

				SET @IsError=1;
				SET @ErrorText = 'Failed to create table';

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

			END CATCH
		END
		
		FETCH NEXT FROM CursorGetActiveProcesses INTO 	@ProcessID,@ProcessName,@ProcessCategoryName
	END
	
	IF @IsError=1
		THROW 50001, @ErrorText, 1;
END