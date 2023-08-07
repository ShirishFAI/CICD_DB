
CREATE PROCEDURE [ETLProcess].[EntityCleanse_DataFix_Issue]          
(          
	@ProcessName VARCHAR(100)   
,	@EntityName VARCHAR(100)   

 )          
AS        
/****************************************************************************************
-- AUTHOR		: Rahul Singh            
-- DATE			: 09/09/2020
-- PURPOSE		: Entity Cleansing
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/09/2020	Rahul Singh			Original Version
** 03/25/2021	Sanjay Janardhan	Added logic to update the ProvinceCode and JurCode before entity cleansing
******************************************************************************************/
   	SET NOCOUNT ON; 
   
    DECLARE @tableName varchar(200) =@EntityName;
	DECLARE @LastRunId INT;
    DECLARE @strSQL nvarchar(max) = N'' ; 
	DECLARE @CASE NVARCHAR(MAX) = N'';
	DECLARE @Join NVARCHAR(MAX)=N'';
	DECLARE	@SelectClause NVARCHAR(MAX)=N'';			
	DECLARE	@InsertClause NVARCHAR(MAX)=N'';
	DECLARE @DeleteSQL NVARCHAR(MAX)=N'';
	DECLARE @DateClause NVARCHAR(MAX)=N'';
	DECLARE @CleansingRule Varchar(500);
	DECLARE @LastRetrievedDateTime DATETIME;

	DECLARE @ProcessCategory VARCHAR(100)='DTC_StageEntityCleansing_ETL';
	DECLARE @ProcessStage VARCHAR(100)=@EntityName;
	DECLARE @HistoryStage VARCHAR(200);
	DECLARE @ErroMessage VARCHAR(100)='Error in Entity Cleansing';
	DECLARE @ProcessID INT;
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

	SELECT
		@LastRunId = MAX(RunId)
	FROM
		ETLAudit.ETLProcessCategory

		INNER JOIN ETLProcess.ETLProcessCategory Category
		ON Category.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId
	WHERE			
		ETLProcessCategory.CurrentStatus=5
		AND Category.ProcessCategoryName=@ProcessCategory

	IF @LastRunId > 0	
			SELECT 
				  @LastRetrievedDateTime=UTC_CompletedAt
			FROM
				ETLAudit.ETLProcessCategory
			WHERE
				RunId=@LastRunId
		
	SET @LastRetrievedDateTime=ISNULL(@LastRetrievedDateTime,'1900-01-01');

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
		AND ETLProcess.ProcessName=@ProcessName;
						

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
			BEGIN TRY
				SET @HistoryStage =  'Started updating ProvinceCode for '+@EntityName+N'';
				
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = @HistoryStage
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started';

				SET @HistoryStage =  'Completed updating ProvinceCode for  '+@EntityName+N'';
			
				IF @EntityName ='dbo.Address'
					BEGIN
						IF @LastRetrievedDateTime='1900-01-01'
							UPDATE 
								dbo.Address 
							SET 
								JurCode=ProvinceCode 
							,	LastModifiedDateUTC = GETUTCDATE()
							WHERE 
								ISNULL(JurCode,'') ='' 
								AND ProvinceCode<>'BC'	;		
						ELSE			
							UPDATE 
								dbo.Address 
							SET 
								JurCode=ProvinceCode 
							,	LastModifiedDateUTC = GETUTCDATE()
							WHERE 
								ISNULL(JurCode,'') ='' 
								AND ProvinceCode<>'BC' 
								AND Address.LastModifiedDateUTC > @LastRetrievedDateTime;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	
						,	@Inserts = @@ROWCOUNT;
					END
				ELSE
					BEGIN
						IF @EntityName IN('dbo.Listing','dbo.Property','dbo.Taxation','dbo.Valuation')
							BEGIN
								IF @LastRetrievedDateTime='1900-01-01'
									SET @strSQL =	N' UPDATE Entity '
													+	N' SET Entity.ProvinceCode = Address.ProvinceCode, Entity.LastModifiedDateUTC = GETUTCDATE(), '
													+	N' Entity.JurCode = (CASE WHEN Address.ProvinceCode<>''BC'' AND NULLIF(Entity.JurCode,'''') IS NULL THEN Address.ProvinceCode ELSE Entity.JurCode END)'
													+	N' FROM '+@EntityName+N' Entity INNER JOIN dbo.Address ON Address.Code = Entity.Code '
													+	N' WHERE ISNULL(Entity.ProvinceCode,'''') <>Address.ProvinceCode;'
			
								ELSE
									SET @strSQL =	N' UPDATE Entity '
													+	N' SET Entity.ProvinceCode = Address.ProvinceCode, Entity.LastModifiedDateUTC = GETUTCDATE(),'
													+	N' Entity.JurCode = (CASE WHEN Address.ProvinceCode<>''BC'' AND NULLIF(Entity.JurCode,'''') IS NULL THEN Address.ProvinceCode ELSE Entity.JurCode END)'
													+	N' FROM '+@EntityName+N' Entity INNER JOIN dbo.Address ON Address.Code = Entity.Code '
													+	N' WHERE ISNULL(Entity.ProvinceCode,'''') <>Address.ProvinceCode '
													+	N' AND ( Entity.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) ); '		
												
							END
						ELSE IF @EntityName IN('dbo.Building','dbo.Business','dbo.Parcel','dbo.PIN')
							BEGIN
								IF @LastRetrievedDateTime='1900-01-01'
									SET @strSQL =	N' UPDATE Entity SET Entity.ProvinceCode = Address.ProvinceCode, Entity.LastModifiedDateUTC = GETUTCDATE() FROM '+@EntityName+N' Entity INNER JOIN dbo.Address ON Address.Code = Entity.Code '
													+	N' WHERE ISNULL(Entity.ProvinceCode,'''') <>Address.ProvinceCode;'			
								ELSE
									SET @strSQL =	N' UPDATE Entity SET Entity.ProvinceCode = Address.ProvinceCode, Entity.LastModifiedDateUTC = GETUTCDATE() FROM '+@EntityName+N' Entity INNER JOIN dbo.Address ON Address.Code = Entity.Code '
													+	N' WHERE ISNULL(Entity.ProvinceCode,'''') <>Address.ProvinceCode '
													+	N' AND ( Entity.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) ) ;'
							END	

						EXECUTE sp_executesql @statement = @strSQL  ;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = @HistoryStage
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	
						,	@Inserts = @@ROWCOUNT;
					END

				SET @strSQL=N'';
				SET @InsertClause=N'';
				SET @InsertClause = N' INSERT INTO '+@TableName+N'_Invalid (';
			
				SELECT  
					@InsertClause = @InsertClause+COLUMN_NAME+N' ,'
				FROM 
					INFORMATION_SCHEMA.COLUMNS c
				WHERE 
					TABLE_SCHEMA+'.'+TABLE_NAME=@tableName
					AND COLUMN_NAME NOT IN('IsValid','ID')
				ORDER BY 
					COLUMN_NAME;
			
				SET @InsertClause= @InsertClause+N'IsPermanentlyInvalid,InvalidRuleId)';

				SET @SelectClause=N'';
				SET @SelectClause = N' SELECT ';
				
				SELECT  
					@SelectClause = @SelectClause+'e.'+COLUMN_NAME+N','
				FROM 
					INFORMATION_SCHEMA.COLUMNS c
				WHERE 
					TABLE_SCHEMA+'.'+TABLE_NAME=@tableName
					AND COLUMN_NAME NOT IN('IsValid','ID')
				ORDER BY 
					COLUMN_NAME;
			
				SELECT @CASE = @CASE + ' ' + CleansingRule + ' THEN ' + CAST(CleansingRuleId AS varchar) + ' ' 
				FROM ETLProcess.ETLEntityCleansingRules WHERE Entity = @tableName AND ActiveFlag = 1

				SET @Join= @Join+ CASE WHEN @tableName IN('dbo.Parcel','dbo.Building','dbo.Business','dbo.Sales') 
												THEN N' e LEFT JOIN Address ON Address.Code=e.Code '
									   WHEN @tableName IN('dbo.Valuation','dbo.Listing','dbo.Property')
												THEN N' e LEFT JOIN Address ON Address.Code = e.Code LEFT JOIN PIN ON PIN.Code=e.Code LEFT JOIN Taxation ON Taxation.Code=e.Code'
										Else N' e ' END

				SET @DateClause= @DateClause+CASE WHEN @tableName IN('dbo.Parcel','dbo.Building','dbo.Business','dbo.Sales') 
												 THEN N' (e.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)) '
												 WHEN @tableName IN('dbo.Valuation','dbo.Listing','dbo.Property')
												 THEN N' (e.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR PIN.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR Taxation.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME))  '
										ELSE N' (e.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)) ' END

				IF @LastRetrievedDateTime='1900-01-01'
					--SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid  '+N' FROM '+@TableName+N' WHERE CASE '+ @CASE + ' END <> 0;'			
					SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid, 1 AS IsPermanentlyInvalid FROM '+@TableName+@Join+N' WHERE CASE '+ @CASE + ' END <> 0;'			
				ELSE
					--SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid  '+N' FROM '+@TableName+N' WHERE CASE '+ @CASE + ' END <> 0 AND LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)  ;'			
					SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid, 1 AS IsPermanentlyInvalid FROM '+@TableName+@Join+N' WHERE CASE '+ @CASE + ' END <> 0 AND '+@DateClause+'  ;'			

			

				SET @strSQL=@InsertClause+@SelectClause

				SET @DeleteSQL = 'DELETE ' + @EntityName + ' FROM ' + @EntityName + ' INNER JOIN ' + @EntityName + '_Invalid ON ' + @EntityName + '.Code = ' + @EntityName + '_Invalid.Code;'
					
			
				SET @HistoryStage =  'Started Cleansing [Sending records to Invalid Entity] For '+@EntityName+N'';
				
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = @HistoryStage
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started';

				SET @HistoryStage =  'Completed Cleansing [Sending  records to Invalid Entity] For '+@EntityName+N'';

				EXECUTE sp_executesql @statement = @strSQL  								
				
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = @HistoryStage
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'	
				,	@Inserts = @@ROWCOUNT;

				SET @HistoryStage =  'Started Deleting invalid records For '+@EntityName+N'';

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = @HistoryStage
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started';

				SET @HistoryStage =  'Completed Deleting invalid records For '+@EntityName+N'';

				EXECUTE sp_executesql @statement = @DeleteSQL  								
								
				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = @HistoryStage
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'	
				,	@Deletes = @@ROWCOUNT;

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Stage = @ProcessStage;			

			END TRY

			BEGIN CATCH
				SET @HistoryStage = 'Error Entity Cleasing -'+@EntityName;

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
				,	@Stage =@HistoryStage
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
				,	GETDATE();

				--EXEC ETLProcess.EmailNotification
				--	@ProcessCategory=@ProcessCategory
				--,	@ProcessName= @ProcessName
				--,	@ProcessStage=@ProcessStage
				--,	@ErrorMessage=@ErroMessage
				--,	@IsError='Yes';

				THROW 50001, @ErroMessage, 1;
			END CATCH
		END 												
	END