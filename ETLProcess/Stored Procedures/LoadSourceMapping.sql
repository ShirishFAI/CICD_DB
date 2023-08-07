
CREATE PROCEDURE [ETLProcess].[LoadSourceMapping]
	@ProcessCategory VARCHAR(100)='ALL'
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Load ETLProcess.ETLSourceMapping
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	TRUNCATE TABLE Stage.MappingFileList

	DECLARE	
		@SourceFileName VARCHAR(200)
	,	@ProcessName VARCHAR(100)
	,	@ProcessID INT
	,	@CheckColumnCount INT
	,	@IsError BIT
	,	@CheckMappingFileCount INT
	,	@CheckIfDestColumnNotExists INT
	,	@CountFullAddress INT
	,	@CountAddressColumns INT
	,	@CountDupConcat INT=0;

	INSERT INTO Stage.MappingFileList
		SELECT			
			MappingFiles.SourceFileName
		,	ETLProcess.ProcessName
		,	ETLProcess.ProcessId
		,   0 as IsError
		FROM
			ETLProcess.ETLProcess

			INNER JOIN ETLProcess.ETLProcessCategory
			ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId

			LEFT JOIN (SELECT DISTINCT SourceFileName FROM Stage.ColumnMapping) MappingFiles
			ON ETLProcess.ProcessName=REPLACE(MappingFiles.SourceFileName,'_Mapping.csv','')
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
							
	DECLARE ProcessEachFileCheck CURSOR 
	FOR 
		SELECT SourceFileName,ProcessId,ProcessName FROM Stage.MappingFileList

		OPEN ProcessEachFileCheck
		FETCH NEXT FROM ProcessEachFileCheck INTO 	@SourceFileName,@ProcessID,@ProcessName
		WHILE @@FETCH_STATUS = 0
		BEGIN	
			SET @IsError=0;
			SET @CountDupConcat=0;

			--New mapping
			IF ISNULL(@SourceFileName,'')<>''
			BEGIN
				SELECT 
					@CheckColumnCount = COUNT(ETLSourceMapping.MappingId)
				FROM
					 ETLProcess.ETLSourceMapping							
				WHERE
					ProcessId=@ProcessID;	
			
				--If already mapping data exists then send email
				IF @CheckColumnCount > 0
				BEGIN				
					UPDATE
						Stage.MappingFileList
					SET
						IsError =1 
					WHERE
						ProcessId=@ProcessID;	
				
					EXEC ETLProcess.EmailNotification
						@ProcessCategory=@ProcessCategory
					,	@ProcessName= @ProcessName
					,	@ProcessStage='MappingFile'
					,	@ErrorMessage='Already mapping data exists for the Process. Data will not be loaded for the source.'
					,	@IsError='Yes';		
				
					--SET @ErrorText='Already mapping data exists for the Process. Please remove the mapping file.';
					SET @IsError=1;							
				END	
			
				--Mapping file do not have entries
				IF @IsError=0
				BEGIN	
					SELECT 
						@CheckMappingFileCount = COUNT(1)										
					FROM
						Stage.ColumnMapping 	
					WHERE
						SourceFileName=@SourceFileName										
						AND	ColumnMapping.SourceColumnName IS NOT NULL
						AND ColumnMapping.DestinationColumnName IS NOT NULL;
					
					--If Mapping file is missing
					IF @CheckMappingFileCount=0
					BEGIN			
						UPDATE
							Stage.MappingFileList
						SET
							IsError =1 
						WHERE
							ProcessId=@ProcessID;	

						EXEC ETLProcess.EmailNotification
							@ProcessCategory=@ProcessCategory
						,	@ProcessName= @ProcessName
						,	@ProcessStage='MappingFile'
						,	@ErrorMessage='Mapping file is missing for the Process. Data will not be loaded for the source.'
						,	@IsError='Yes';

						--SET @ErrorText='Mapping file is missing for the Process';
						SET @IsError=1;								
					END
				END

				--Check if the Destinatination column not found
				IF @IsError=0
				BEGIN
				
					--Get Destination column name not found in Entity
					SELECT 
						@CheckIfDestColumnNotExists = COUNT(1)										
					FROM
						Stage.ColumnMapping 
	
						LEFT JOIN 
						(	SELECT DISTINCT
								C.COLUMN_NAME ColumnName  												
							FROM 
								INFORMATION_SCHEMA.COLUMNS C
    
								INNER JOIN INFORMATION_SCHEMA.TABLES T 	
								ON C.TABLE_NAME = T.TABLE_NAME
								AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
								AND T.TABLE_TYPE = 'BASE TABLE'
							WHERE
								c.TABLE_SCHEMA=	'DBO'
								AND C.TABLE_NAME IN('Address','Building','Business','Listing','Parcel','PIN','Sales','Taxation','Valuation','Permit')
						) ColumnMetaData
						ON ColumnMapping.DestinationColumnName = ColumnMetaData.ColumnName
					WHERE
						SourceFileName=@SourceFileName										
						AND	ColumnMapping.SourceColumnName IS NOT NULL
						AND ColumnMapping.DestinationColumnName IS NOT NULL
						AND ColumnMetaData.ColumnName IS NULL;

					IF @CheckIfDestColumnNotExists > 0 
					BEGIN	
						UPDATE
							Stage.MappingFileList							
						SET
							IsError =1 
						WHERE
							ProcessId=@ProcessID;	

						EXEC ETLProcess.EmailNotification
							@ProcessCategory=@ProcessCategory
						,	@ProcessName= @ProcessName
						,	@ProcessStage='MappingFile'
						,	@ErrorMessage='Mapping File Issue - Specified destination column name not found in Entity. Data will not be loaded for the source.'
						,	@IsError='Yes';

						--SET @ErrorText='Mapping File Issue - Specified destination column name not found in Entity';
						SET @IsError=1;		
					END
				END

				--Check if FullAddress and ParsedAddress have mapping
				IF @IsError=0
				BEGIN
					SELECT 
						@CountFullAddress = COUNT(1)										
					FROM
						Stage.ColumnMapping 	
					WHERE
						SourceFileName=@SourceFileName																
						AND ColumnMapping.DestinationColumnName='FullAddress';

					IF @CountFullAddress >0
					BEGIN 
						SELECT 
							@CountAddressColumns = COUNT(1)										
						FROM
							Stage.ColumnMapping 	
						WHERE
							SourceFileName=@SourceFileName																
							AND ColumnMapping.DestinationColumnName IN('UnitNumber','StreetNumber','StreetName','StreetType','StreetDirection');

						IF @CountAddressColumns > 0
						BEGIN
							UPDATE
								Stage.MappingFileList							
							SET
								IsError =1 
							WHERE
								ProcessId=@ProcessID;	

							EXEC ETLProcess.EmailNotification
								@ProcessCategory=@ProcessCategory
							,	@ProcessName= @ProcessName
							,	@ProcessStage='MappingFile'
							,	@ErrorMessage='Mapping File Issue - Mapping specified for FullAddress and other Address fields aswell. Data will not be loaded for the source.'
							,	@IsError='Yes';
							
							SET @IsError=1;	
						END
					END
				END

				--Check if duplicate DestinatinationColumn present and ConcatOrder same
				IF @IsError=0
				BEGIN
				
					--Get Destination column name not found in Entity
					SELECT
						@CountDupConcat=COUNT(1)
					FROM
						ETLProcess.ETLSourceMapping
					WHERE
						DestinationColumnName IS NOT NULL
						AND ProcessId=@ProcessID
					GROUP BY
						DestinationColumnName
					,	ConcatOrder
					HAVING 
						COUNT(1) > 1;

					IF @CountDupConcat > 0 
					BEGIN	
						UPDATE
							Stage.MappingFileList							
						SET
							IsError =1 
						WHERE
							ProcessId=@ProcessID;	

						EXEC ETLProcess.EmailNotification
							@ProcessCategory=@ProcessCategory
						,	@ProcessName= @ProcessName
						,	@ProcessStage='MappingFile'
						,	@ErrorMessage='Mapping File Issue - Duplicate DestinationColumnName exists in mapping with same Concat order. Data will not be loaded for the source.'
						,	@IsError='Yes';

						--SET @ErrorText='Mapping File Issue - Specified destination column name not found in Entity';
						SET @IsError=1;		
					END
				END

				--All Good, load data to ETLProcess.ETLSourceMapping
				IF @IsError=0
				BEGIN
					INSERT INTO ETLProcess.ETLSourceMapping(ProcessId,SourceColumnName,DestinationColumnName,IsKey,DestinationColumnDataType,ConcatOrder,ConvFunction)
						SELECT 
							@ProcessId
						,	TRIM(ColumnMapping.SourceColumnName) SourceColumnName
						,	TRIM(ColumnMapping.DestinationColumnName) DestinationColumnName
						,	ISNULL(NULLiF(ColumnMapping.IsKey,''),0) IsKey
						,	CASE WHEN ColumnMetaData.DataType='VARCHAR(-1)' THEN 'VARCHAR(8000)'  ELSE ColumnMetaData.DataType END AS DataType
						,	ISNULL(NULLiF(ColumnMapping.ConcatOrder,''),0) ConcatOrder
						,	NULLiF(ColumnMapping.ConvFunction,'') ConvFunction
						FROM
							Stage.ColumnMapping 
	
							LEFT JOIN 
							(	SELECT DISTINCT
									C.COLUMN_NAME ColumnName  
								,	CASE 
										WHEN C.DATA_TYPE IN('VARCHAR','NVARCHAR') THEN CONCAT(C.DATA_TYPE,'(',CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)),')')
										WHEN C.DATA_TYPE IN('DECIMAL','NUMERIC') THEN CONCAT(C.DATA_TYPE,' (',CAST(C.NUMERIC_PRECISION AS VARCHAR(10)),',', CAST(C.NUMERIC_SCALE AS VARCHAR(10)),')')
										ELSE C.DATA_TYPE
									END as DataType
								FROM 
									INFORMATION_SCHEMA.COLUMNS C
    
									INNER JOIN INFORMATION_SCHEMA.TABLES T 	
									ON C.TABLE_NAME = T.TABLE_NAME
									AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
									AND T.TABLE_TYPE = 'BASE TABLE'
								WHERE
									c.TABLE_SCHEMA=	'DBO'
									AND C.TABLE_NAME IN('Address','Building','Business','Listing','Parcel','PIN','Sales','Taxation','Valuation','Permit')
							) ColumnMetaData
							ON ColumnMapping.DestinationColumnName = ColumnMetaData.ColumnName
						WHERE
							SourceFileName=@SourceFileName										
							AND	SourceColumnName IS NOT NULL	
				END	
			END
		

			--No new mapping, but still check if there is missing mapping
			--IF ISNULL(@SourceFileName,'')=''
			--BEGIN
			
			SELECT @CheckColumnCount = COUNT(1) FROM ETLProcess.ETLSourceMapping WHERE ProcessId=@ProcessID;

			IF @CheckColumnCount=0 AND @IsError=0
			BEGIN
				UPDATE
					Stage.MappingFileList
				SET
					IsError =1 
				WHERE
					ProcessId=@ProcessID;	

				EXEC ETLProcess.EmailNotification
					@ProcessCategory=@ProcessCategory
				,	@ProcessName= @ProcessName
				,	@ProcessStage='MappingFile'
				,	@ErrorMessage='Mapping file is missing for the Process. Data will not be loaded for the source.'
				,	@IsError='Yes';
			END
			--END

			FETCH NEXT FROM ProcessEachFileCheck INTO 	@SourceFileName,@ProcessID,@ProcessName
		END
				
END