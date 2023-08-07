
CREATE PROCEDURE [ETLProcess].[ProfiseeLoadStgEntity_DataFix_Issue]
	@ProcessName VARCHAR(100)
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Profisee - Load Merge Tables.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
** 04/16/2021	Sanjay Janardhan	Added code to remove ';' in FullAddress column as per the request from DSG

EXEC [ETLProcess].[ProfiseeLoadStgEntity] 'LoadProfiseeStgAddress'
******************************************************************************************/
	SET NOCOUNT ON	;
	SET ANSI_WARNINGS ON;	

	DECLARE @ProcessCategory VARCHAR(100)='DTC_ProfiseeStgLoad_ETL';	
	DECLARE @ProcessStage VARCHAR(100)='ProfStgLoad';
	DECLARE @ErroMessage VARCHAR(100)='Error Loading Profisee Stage';
	
	DECLARE @ProcessID INT;
	DECLARE @IsAuditEntryExists INT;
	DECLARE @RunId INT;
	DECLARE @CurrentStatus VARCHAR(100);
	DECLARE @IsError BIT=0;
	DECLARE @LastRetrievedDateTime DATETIME;
	DECLARE @TableName VARCHAR(100);
	DECLARE	@InsertClause NVARCHAR(4000)=N'';
	DECLARE @SelectClause NVARCHAR(4000)=N'';
	DECLARE @DynamicSQL NVARCHAR(1000)=N'';
	DECLARE @LastRunId INT;
	DECLARE @ProfiseDBName VARCHAR(20)='Profisee';
	DECLARE @LogDesc VARCHAR(100);
	DECLARE @Inserted INT;
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
		AND ETLProcess.ProcessName=@ProcessName	;

	IF ISNULL(@ProcessID,0) > 0 AND ISNULL(@RunId,0) > 0
	BEGIN
		SET @TableName=SUBSTRING(@ProcessName,16,LEN(@ProcessName));

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
			AND CurrentStage=@ProcessStage;

		IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0
			EXEC ETLProcess.AuditLog
				@ProcessCategory = @ProcessCategory
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = @ProcessStage
			,	@Status = 'InProgress'
			,	@CurrentStatus = 'Started'		;										
		
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
				SET @LogDesc='Start Loading '+@ProfiseDBName+N'.stg.t'+@TableName++N'_Merge '

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage =@LogDesc
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started'	

				IF @TableName='Property'
				BEGIN
					SET @DynamicSQL=N''
					SET @DynamicSQL=N'TRUNCATE TABLE '+@ProfiseDBName+N'.data.t'+@TableName;
					PRINT @DynamicSQL;
					EXECUTE (@DynamicSQL);					

					SET @InsertClause=N'';
					SET @InsertClause = N' INSERT INTO '+@ProfiseDBName+N'.stg.t'+@TableName++N'_Merge '+N' ( Code,';
			
					SELECT  
						@InsertClause = @InsertClause+COLUMN_NAME+N','
					FROM 
						INFORMATION_SCHEMA.COLUMNS c
					WHERE 
						TABLE_SCHEMA='dbo' 
						AND TABLE_NAME=@TableName
						AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','DateCreatedUTC','LastModifiedDateUTC','Code')
					ORDER BY 
						COLUMN_NAME;

					SET @InsertClause= LEFT(@InsertClause,LEN(@InsertClause)-1)+N')';		

					SET @SelectClause=N'';
					SET @SelectClause =	 N'SELECT ROW_NUMBER() OVER( ORDER BY (SELECT 1)) AS  Code,* FROM ('
										+N' SELECT DISTINCT ';
			
					SELECT  
						@SelectClause = @SelectClause+COLUMN_NAME+N','
					FROM 
						INFORMATION_SCHEMA.COLUMNS c
					WHERE 
						TABLE_SCHEMA='dbo' 
						AND TABLE_NAME=@TableName
						AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','DateCreatedUTC','LastModifiedDateUTC','Code')
					ORDER BY 
						COLUMN_NAME;

					SET @SelectClause= LEFT(@SelectClause,LEN(@SelectClause)-1)+N' FROM dbo.'+@TableName+N' ) GetUnique '
					
					--PRINT @InsertClause+@SelectClause;
					EXECUTE (@InsertClause+N' '+@SelectClause);
					SET @Inserted=@@ROWCOUNT;
				END
				ELSE
				BEGIN
					SELECT
						@LastRunId = MAX(RunId)
					FROM
						ETLProcess.ETLProcess

						INNER JOIN ETLAudit.ETLProcess AuditProcess
						ON AuditProcess.ProcessId = ETLProcess.ProcessId
	
						INNER JOIN ETLProcess.ETLStatus
						ON ETLStatus.StatusId = AuditProcess.CurrentStatus
					WHERE			
						ETLStatus.Status='Completed'
						AND ETLProcess.ProcessName=@ProcessName

					IF @LastRunId > 0		
						SELECT 
							@LastRetrievedDateTime = AuditProcess.UTC_StartedAt
						FROM
							ETLProcess.ETLProcess

							INNER JOIN ETLAudit.ETLProcess AuditProcess
							ON AuditProcess.ProcessId = ETLProcess.ProcessId

							INNER JOIN ETLProcess.ETLStatus
							ON ETLStatus.StatusId = AuditProcess.CurrentStatus
						WHERE
							AuditProcess.RunId=@LastRunId				
							AND ETLStatus.Status='Completed'
							AND ETLProcess.ProcessName=@ProcessName;

					SET @LastRetrievedDateTime=ISNULL(@LastRetrievedDateTime,'1900-01-01');					
					SET @InsertClause=N'';
					SET @InsertClause = N' INSERT INTO '+@ProfiseDBName+'.stg.t'+@TableName++N'_Merge'+N' (';												
					

					IF @TableName='Address'
						BEGIN
							SELECT  
								@InsertClause = @InsertClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','IsMADReceived','IsMADSent','MADReceivedDateUTC','MADSentDateUTC','FullAddress')
							ORDER BY 
								COLUMN_NAME;

							--SET @InsertClause= LEFT(@InsertClause,LEN(@InsertClause)-1)+N')';
							SET @InsertClause= @InsertClause+N' FullAddress )';

							--Generate Select Clause							
							SET @SelectClause=N' SELECT ';

							SELECT  
								@SelectClause = @SelectClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME='Address'
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','IsMADReceived','IsMADSent','MADReceivedDateUTC','MADSentDateUTC','FullAddress')
							ORDER BY 
								COLUMN_NAME;

							--SET @SelectClause= LEFT(@SelectClause,LEN(@SelectClause)-1)+N' FROM dbo.'+@TableName+N' WHERE NOT EXISTS (SELECT 1 FROM dbo.MADAddress WHERE CAST(MADAddress.MADAddressID AS VARCHAR(20)) =Address.MasterAddressID)';
							--SET @SelectClause= LEFT(@SelectClause,LEN(@SelectClause)-1)+N' FROM dbo.'+@TableName+N' WHERE ISNUMERIC(MasterAddressID)=0';
							SET @SelectClause= @SelectClause+N' CASE WHEN CHARINDEX('';'', FullAddress) >1 THEN SUBSTRING(FullAddress, 1, CHARINDEX('';'', FullAddress) - 1) ELSE  FullAddress END AS FullAddress'
															+N' FROM dbo.'+@TableName+N' WHERE ISNUMERIC(MasterAddressID)=0';

							IF @LastRetrievedDateTime<>'1900-01-01'														
								SET @SelectClause= @SelectClause+N' AND LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)'
									

							--PRINT @InsertClause+@SelectClause
							EXECUTE (@InsertClause+N' '+@SelectClause);
							SET @Inserted=@@ROWCOUNT;


							SET @SelectClause=N'';
							SET @SelectClause=N' SELECT DISTINCT
								AreaDescription
							,	MADAddress.City
							,	[Address].Code
							,	Community
							,	MADAddress.Country
							,	CrossStreet
							,	Data_Source_Priority
							,	[Address].DateCreatedUTC
							,	District
							,	MADAddress.FSA
							--,	MADAddress.FullAddress
							,	IsMunicipalAddress
							,	JurCode
							,	JurDescription
							,	LandDistrict
							,	LandDistrictName
							,	[Address].LastModifiedDateUTC
							,	MADAddress.Latitude
							,	[Address].LatitudeLongitude
							,	MADAddress.Longitude
							,	CAST(MasterAddressID AS VARCHAR(100)) MasterAddressID
							,	Municipality
							,	Neighbourhood
							,	NeighbourhoodDescription
							,	MADAddress.PostalCode
							,	MADAddress.ProvinceCode
							,	Range
							,	Region
							,	SchoolDistrictDescription
							,	MADAddress.StreetDirection
							,	MADAddress.StreetName
							,	MADAddress.StreetNumber
							,	MADAddress.StreetType
							,	Township
							,	MADAddress.UnitNumber
							,	CASE WHEN CHARINDEX('';'', MADAddress.FullAddress) >1 THEN		SUBSTRING(MADAddress.FullAddress, 1, CHARINDEX('';'', MADAddress.FullAddress) - 1) ELSE  MADAddress.FullAddress END AS FullAddress
							FROM 
								dbo.Address [Address]

								INNER JOIN dbo.MADAddress
								ON CAST(MADAddress.MADAddressID as VARCHAR(100)) = [Address].MasterAddressID
							WHERE
								ISNUMERIC([Address].MasterAddressID)=1';

							IF @LastRetrievedDateTime<>'1900-01-01'
								SET @SelectClause	 = @SelectClause+N' AND [Address].LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)';

							--SET @SelectClause	 = @SelectClause+N' WHERE [Address].LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)';

							--PRINT @InsertClause+@SelectClause
							EXECUTE (@InsertClause+N' '+@SelectClause);
							SET @Inserted=@Inserted+@@ROWCOUNT;
					END
					
					IF @TableName IN('Taxation', 'PIN')
						BEGIN
							SELECT  
								@InsertClause = @InsertClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID')
							ORDER BY 
								COLUMN_NAME;

							SET @InsertClause= @InsertClause+N' MatchKeyMember )';

							--Generate Select Clause
							SET @SelectClause = N' SELECT ';
			
							SELECT  
								@SelectClause = @SelectClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID')
							ORDER BY 
								COLUMN_NAME;

							SET @SelectClause= @SelectClause+' '+
									CASE 
										WHEN @TableName='PIN'		THEN N' ISNULL(PIN,'''')+'':''+ ISNULL(ProvinceCode,'''')'									
										WHEN @TableName='Taxation'	THEN N' ISNULL(ARN,'''')+'':''+ ISNULL(JurCode,'''')+ISNULL(CAST(TaxYear AS VARCHAR(10)),'''')'
									END +N' AS MatchKeyMember' 

							SET @SelectClause= @SelectClause+N' FROM dbo.'+@TableName; 

							IF @LastRetrievedDateTime<>'1900-01-01'
								SET @SelectClause= @SelectClause+N' WHERE LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)';

							--PRINT @InsertClause+@SelectClause;
							EXECUTE ( @InsertClause+N' '+@SelectClause);
							SET @Inserted=@@ROWCOUNT;
						END			
						
					IF @TableName IN('Business', 'Sales')
						BEGIN
							SELECT  
								@InsertClause = @InsertClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','MasterAddressID')
							ORDER BY 
								COLUMN_NAME;

							SET @InsertClause= @InsertClause+N'MasterAddressID)';

							--Generate Select Clause
							SET @SelectClause = N' SELECT ';
			
							SELECT  
								@SelectClause = @SelectClause+N'Main.'+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','MasterAddressID')
							ORDER BY 
								COLUMN_NAME;

							SET @SelectClause =@SelectClause+N' Address.MasterAddressID FROM dbo.'+@TableName+N' Main LEFT JOIN dbo.Address Address ON Address.Code = Main.Code'

							IF @LastRetrievedDateTime<>'1900-01-01'
								SET @SelectClause= @SelectClause
										+N' WHERE Main.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR'
										+N' Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)';

							--PRINT @InsertClause+@SelectClause;
							EXECUTE (@InsertClause+N' '+@SelectClause);
							SET @Inserted=@@ROWCOUNT;
						END

					IF @TableName IN('Valuation', 'Listing')
						BEGIN
							SELECT  
								@InsertClause = @InsertClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','MasterAddressID','PIN','ARN')
							ORDER BY 
								COLUMN_NAME;

							SET @InsertClause= @InsertClause+N'MasterAddressID, PIN, ARN)';

							--Generate Select Clause
							SET @SelectClause = N' SELECT ';
			
							SELECT  
								@SelectClause = @SelectClause+N'Main.'+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','MasterAddressID','PIN','ARN')
							ORDER BY 
								COLUMN_NAME;

							SET @SelectClause =@SelectClause+N' Address.MasterAddressID, PIN.PIN, Taxation.ARN FROM dbo.'+@TableName+N' Main LEFT JOIN dbo.Address Address ON Address.Code = Main.Code LEFT JOIN dbo.PIN PIN ON PIN.Code = Main.Code'
								+N' LEFT JOIN dbo.Taxation Taxation ON Taxation.Code = Main.Code'

							IF @LastRetrievedDateTime<>'1900-01-01'
								SET @SelectClause= @SelectClause
										+N' WHERE Main.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR'
										+N' Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR'
										+N' PIN.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR'
										+N' Taxation.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)';

							--PRINT @InsertClause+@SelectClause;
							EXECUTE (@InsertClause+N' '+@SelectClause);
							SET @Inserted=@@ROWCOUNT;
						END

					IF @TableName IN('Building', 'Parcel')
						BEGIN
							SELECT  
								@InsertClause = @InsertClause+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','MasterAddressID','PIN')
							ORDER BY 
								COLUMN_NAME;
								
							SET @InsertClause= @InsertClause+N'MatchKeyMember, MasterAddressID, PIN )';

							--Generate Select Clause
							SET @SelectClause = N' SELECT ';
			
							SELECT  
								@SelectClause = @SelectClause+N'Main.'+COLUMN_NAME+N','
							FROM 
								INFORMATION_SCHEMA.COLUMNS c
							WHERE 
								TABLE_SCHEMA='dbo' 
								AND TABLE_NAME=@TableName
								AND COLUMN_NAME NOT IN('IsValid','ID','Data_Source_ID','MasterAddressID','PIN')
							ORDER BY 
								COLUMN_NAME;

							SET @SelectClause= @SelectClause+' '+
									CASE 
											WHEN @TableName='Building'	THEN ' ISNULL(Address.MasterAddressID,'''')+'':''+ ISNULL(PIN.PIN,'''')+'':''+ ISNULL(PIN.ProvinceCode,'''')'								
											WHEN @TableName='Parcel'	THEN ' ISNULL(Address.MasterAddressID,'''')+'':''+ ISNULL(PIN.PIN,'''')+'':''+ ISNULL(PIN.ProvinceCode,'''')+'':''+ISNULL(Main.Sequence,'''')'											
									END +N' AS MatchKeyMember,' 

							SET @SelectClause =@SelectClause+N' Address.MasterAddressID, PIN.PIN FROM dbo.'+@TableName+N' Main LEFT JOIN dbo.Address Address ON Address.Code = Main.Code LEFT JOIN dbo.PIN PIN ON PIN.Code = Main.Code'
								
							IF @LastRetrievedDateTime<>'1900-01-01'
								SET @SelectClause= @SelectClause
											+N' WHERE Main.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR'
											+N' Address.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) OR'
											+N' PIN.LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME) '	;							

							--PRINT @InsertClause
							--PRINT @SelectClause;
							EXECUTE (@InsertClause+N' '+@SelectClause);
							SET @Inserted=@@ROWCOUNT;
						END
						
				END		
				

				SET @LogDesc='Completed Loading '+@ProfiseDBName+N'.stg.t'+@TableName++N'_Merge '

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage =@LogDesc
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Inserts = @Inserted;

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Stage = @ProcessStage;
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