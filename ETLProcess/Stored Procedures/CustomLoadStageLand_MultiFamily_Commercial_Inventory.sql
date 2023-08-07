


CREATE PROCEDURE [ETLProcess].[CustomLoadStageLand_MultiFamily_Commercial_Inventory] 
	@ExternalFileName VARCHAR(500)
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Shirish Waghmale
-- DATE			: 02/10/2022
-- PURPOSE		: Multifamily External Source File - Load to StageLanding.
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 02/11/2022	Shirish Waghmale	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @Params NVARCHAR(500)='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100),@ExternalFileName VARCHAR(500),@ExternalDataSourceName VARCHAR(100)';
	DECLARE	@DynamicSQL NVARCHAR(MAX);
	DECLARE	@StageLandSchema VARCHAR(50)='StageLanding.';
	DECLARE	@StageProcessSchema VARCHAR(50)='StageProcessing.';
	DECLARE	@ErrorSchema VARCHAR(50)='StageProcessErr.';
	DECLARE	@HistorySchema VARCHAR(50)='SourceHistory.';
	DECLARE	@ExternalDataSourceName VARCHAR(100)='DTCDataSetExternal';
	DECLARE	@TableName VARCHAR(100)='MultiFamily_Commercial_Inventory';
	DECLARE	@CustomLoad_TableName VARCHAR(100)='CustomLoad_Commercial_Inventory';
	DECLARE	@ProcessName VARCHAR(100)	;
	DECLARE	@CurrentStatus VARCHAR(100) ;
	DECLARE @RunId  INT;
	DECLARE	@IsAuditEntryExists INT;
	DECLARE	@Status VARCHAR(100);
	DECLARE	@ActiveFlag BIT;
	DECLARE	@IsAuditProcessEntryExists INT;	
	DECLARE	@IsError BIT=0;
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE	@Exception VARCHAR(500);
	DECLARE @DynamicSQLLarge VARCHAR(8000);
	DECLARE	@ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';
	DECLARE @IsKeyCount INT;
	DECLARE @ProcessID INT;
	DECLARE @DistKeyCnt INT=0;
	DECLARE @DistRowCnt INT=0;

	SELECT 
		@ErrorProcedure= s.name+'.'+o.name 
	FROM 
		SYS.OBJECTS O 
		
		INNER JOIN SYS.SCHEMAS S 
		ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	SELECT
		@ProcessName=CleansedFileName
	FROM
		Stage.ExternalFileslist
	WHERE
		FileName=@ExternalFileName;

	SET @TableName=@ProcessName;

	SELECT
		@ActiveFlag = COUNT(1)
	FROM 
		ETLProcess.ETLProcess

		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId
	WHERE
		ETLProcess.ProcessName = @ProcessName
		AND ETLProcess.ActiveFlag =1
		AND ETLProcessCategory.ActiveFlag=1
		AND ISNULL(IsSourceSpecificLoad,0)=1;

	IF ISNULL(@ActiveFlag,0)=1
	BEGIN	
		
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName =@ProcessName
		,	@Stage ='Get RunId for Loading External Files'
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
		,	@ProcessName = @ProcessName
		,	@Stage ='Got RunId for Loading External Files'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	
		,	@Inserts=0;	
	
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
			AND CurrentStage='Landing';

		IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0			
			EXEC ETLProcess.AuditLog
				@ProcessCategory = 'DTC_ExternalSource_ETL'
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = 'Landing'
			,	@Status = 'InProgress'
			,	@CurrentStatus = 'Started'	;	
	
		SELECT 
			@CurrentStatus = ETLStatus.Status
		FROM
			ETLAudit.ETLProcess AuditProcess

			INNER JOIN ETLProcess.ETLProcess
			ON AuditProcess.ProcessId = ETLProcess.ProcessId

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId = AuditProcess.CurrentStatus
		WHERE
			RunId=@RunId
			AND ETLProcess.ProcessName = @ProcessName
			AND AuditProcess.CurrentStage = 'Landing';

		SELECT 
			@ProcessID=ProcessID
		FROM
			ETLProcess.ETLProcess
		WHERE
			ProcessName=@ProcessName;

		IF ISNULL(@CurrentStatus,'') NOT IN('Completed','Hold')
		BEGIN	
		
			IF OBJECT_ID( @StageLandSchema+@TableName, 'U') IS NOT NULL
				BEGIN
					
					BEGIN TRY
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Start loading to StageLanding'
						,	@Status = 'InProgress'
						,	@CurrentStatus = 'Started'	;

					IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES T WHERE T.TABLE_NAME = 'CustomLoad_Commercial_Inventory' AND T.TABLE_SCHEMA = REPLACE(@StageLandSchema,'.',''))
					BEGIN
					
						CREATE TABLE StageLanding.CustomLoad_Commercial_Inventory (
							[Area] NVARCHAR(500)
							,[Jurisdiction] NVARCHAR(500)
							,[Roll Number] NVARCHAR(500)
							,[Year Built] NVARCHAR(500)
							,[Effective Year] NVARCHAR(500)
							,[Number of Storeys] NVARCHAR(500)
							,[Gross Leasable Area] NVARCHAR(500)
							,[Net Leasable Area] NVARCHAR(500)
							,[Parking Type – Underground] NVARCHAR(500)
							,[Parking Type – Surface] NVARCHAR(500)
							,[Parking Type – Parking structure] NVARCHAR(500)
							,[Number of Units in an Co-op] NVARCHAR(500)
							,[Number of Bachelor Units in a Co-op] NVARCHAR(500)
							,[Number of 1 Bedroom Units in a Co-op] NVARCHAR(500)
							,[Number of 2 Bedroom Units in a Co-op] NVARCHAR(500)
							,[Number of 3 Bedroom Units in a Co-op] NVARCHAR(500)
							,[Number of 4 Bedroom Units in a Co-op] NVARCHAR(500)
							,[Predominant Manual Class] NVARCHAR(MAX)
							,[Gross Building Area] NVARCHAR(500)
							,[Basement Area] NVARCHAR(500)
							,[Strata Industrial / Commercial Lot Area] NVARCHAR(500)
							,[Apartment – Number of Units] NVARCHAR(500)
							,[Apartment – Number of Bachelor Units] NVARCHAR(500)
							,[Apartment – Number of 1 Bedroom Units] NVARCHAR(500)
							,[Apartment – Number of 2 Bedroom Units] NVARCHAR(500)
							,[Apartment – Number of 3 Bedroom Units] NVARCHAR(500)
							,[Apartment – Number of 4 Bedroom Units] NVARCHAR(500)
							,[Apartment – Number of House Keeping Rooms] NVARCHAR(500)
							,[Number of Units in a Hotel] NVARCHAR(500)
							,[Number of Units in Motel] NVARCHAR(500)
							,[Senior Housing – Number of Units] NVARCHAR(500)
							,[Senior Housing – Number of Bachelor Units] NVARCHAR(500)
							,[Senior Housing – Number of 1 Bedrooms] NVARCHAR(500)
							,[Senior Housing – Number of 2 Bedrooms] NVARCHAR(500)
							,[Senior Housing – Number of 3 Bedrooms] NVARCHAR(500)
							,[Senior Housing – Number of Bed Sitting Rooms] NVARCHAR(500)
							,[Senior Housing – Number of Licensed Care Private Beds] NVARCHAR(500)
							,[Senior Housing – Number of Licensed] NVARCHAR(500)
							,[Care Semi-Private Beds] NVARCHAR(500)
							,[Total Balcony Area] NVARCHAR(500)
							,[Mezzanine Area] NVARCHAR(500)
							,[Type of Heating] NVARCHAR(500)
							,[Placeholder 1] NVARCHAR(500)
							,[Elevators] NVARCHAR(500)
							,[Type of Construction] NVARCHAR(500)
							,[Other Buildings] NVARCHAR(500)
							,[School District] NVARCHAR(500)
							,[Zoning] NVARCHAR(500)
							)
					END;

						SET @DynamicSQL=N'';

						IF @ExternalFileName LIKE '%.txt'
							BEGIN
								SET @DynamicSQL = @DynamicSQL+
											' BULK INSERT '+@StageLandSchema+@CustomLoad_TableName
										+	' FROM '''+@ExternalFileName+''''
										+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FIRSTROW = 1,  FIELDTERMINATOR = ''","'', ROWTERMINATOR=''0x0a''); ';

								SET @Params ='@StageLandSchema VARCHAR(50),@CustomLoad_TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)'	;	
								EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@CustomLoad_TableName,@ExternalFileName,@ExternalDataSourceName;
							END
						
						ELSE IF @ExternalFileName LIKE '%.csv'
							BEGIN
								SET @DynamicSQL = @DynamicSQL+
											' BULK INSERT '+@StageLandSchema+@CustomLoad_TableName
										+	' FROM '''+@ExternalFileName+''''
										+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,  FIELDQUOTE=''"''); ';
										--+	' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,  FIELDTERMINATOR = '','', ROWTERMINATOR=''0x0a''); ';
		
							
								SET @Params ='@StageLandSchema VARCHAR(50),@CustomLoad_TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)';
								EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@CustomLoad_TableName, @ExternalFileName,@ExternalDataSourceName;				

							END;

						-- Unpivot on the ParkingTotal FOR ParkingType
						DROP TABLE IF EXISTS #ParkingType_Data;

						SELECT REPLACE(Area, '"', '') AS Area
							,[Jurisdiction]
							,[Roll Number]
							,[Year Built]
							,[Effective Year]
							,[Number of Storeys]
							,[Gross Leasable Area]
							,[Net Leasable Area]
							,ParkingTotal
							,CASE 
								WHEN ParkingType = 'Parking Type – Underground' THEN 'Underground'
								WHEN ParkingType = 'Parking Type – Surface' THEN 'Surface'
								WHEN ParkingType = 'Parking Type – Parking structure' THEN 'Parking structure'
								ELSE NULL
							 END AS ParkingType
							,[Number of Units in an Co-op]
							,[Number of Bachelor Units in a Co-op]
							,[Number of 1 Bedroom Units in a Co-op]
							,[Number of 2 Bedroom Units in a Co-op]
							,[Number of 3 Bedroom Units in a Co-op]
							,[Number of 4 Bedroom Units in a Co-op]
							,[Predominant Manual Class]
							,[Gross Building Area]
							,[Basement Area]
							,[Strata Industrial / Commercial Lot Area]
							,[Apartment – Number of Units]
							,[Apartment – Number of House Keeping Rooms]
							,[Number of Units in a Hotel]
							,[Number of Units in Motel]
							,[Senior Housing – Number of Units]
							,[Senior Housing – Number of Bachelor Units]
							,[Senior Housing – Number of 1 Bedrooms]
							,[Senior Housing – Number of 2 Bedrooms]
							,[Senior Housing – Number of 3 Bedrooms]
							,[Senior Housing – Number of Bed Sitting Rooms]
							,[Senior Housing – Number of Licensed Care Private Beds]
							,[Senior Housing – Number of Licensed]
							,[Care Semi-Private Beds]
							,[Total Balcony Area]
							,[Mezzanine Area]
							,[Type of Heating]
							,[Placeholder 1]
							,[Elevators]
							,[Type of Construction]
							,[Other Buildings]
							,[School District]
							,REPLACE([Zoning], '"', '') AS Zoning
						INTO #ParkingType_Data
						FROM StageLanding.CustomLoad_Commercial_Inventory
						UNPIVOT(
								ParkingTotal FOR ParkingType IN ([Parking Type – Underground]
																,[Parking Type – Surface]
																,[Parking Type – Parking structure]
																)
							   ) AS ParkingType ;
						
						
						-- Unpivot on the NUnits FOR NBedRooms
						DROP TABLE IF EXISTS #NumberOfUnits_Data;

						SELECT REPLACE(Area, '"', '') AS Area
							,[Jurisdiction]
							,[Roll Number]
							,[Year Built]
							,[Effective Year]
							,[Number of Storeys]
							,[Gross Leasable Area]
							,[Net Leasable Area]
							,(CAST(ISNULL([Parking Type – Underground], 0) AS INT) + CAST(ISNULL([Parking Type – Surface], 0) AS INT) + CAST(ISNULL([Parking Type – Parking structure], 0) AS INT)) AS ParkingTotal
							,[Number of Units in an Co-op]
							,[Number of Bachelor Units in a Co-op]
							,[Number of 1 Bedroom Units in a Co-op]
							,[Number of 2 Bedroom Units in a Co-op]
							,[Number of 3 Bedroom Units in a Co-op]
							,[Number of 4 Bedroom Units in a Co-op]
							,[Predominant Manual Class]
							,[Gross Building Area]
							,[Basement Area]
							,[Strata Industrial / Commercial Lot Area]
							,NumberOfUnits
							,CASE 
								WHEN NumberOfBedrooms = 'Apartment – Number of Bachelor Units' THEN '0'
								WHEN NumberOfBedrooms = 'Apartment – Number of 1 Bedroom Units' THEN '1'
								WHEN NumberOfBedrooms = 'Apartment – Number of 2 Bedroom Units' THEN '2'
								WHEN NumberOfBedrooms = 'Apartment – Number of 3 Bedroom Units' THEN '3'
								WHEN NumberOfBedrooms = 'Apartment – Number of 4 Bedroom Units'	THEN '4'
								ELSE NULL
							END AS NumberOfBedrooms
							,[Apartment – Number of House Keeping Rooms]
							,[Number of Units in a Hotel]
							,[Number of Units in Motel]
							,[Senior Housing – Number of Units]
							,[Senior Housing – Number of Bachelor Units]
							,[Senior Housing – Number of 1 Bedrooms]
							,[Senior Housing – Number of 2 Bedrooms]
							,[Senior Housing – Number of 3 Bedrooms]
							,[Senior Housing – Number of Bed Sitting Rooms]
							,[Senior Housing – Number of Licensed Care Private Beds]
							,[Senior Housing – Number of Licensed]
							,[Care Semi-Private Beds]
							,[Total Balcony Area]
							,[Mezzanine Area]
							,[Type of Heating]
							,[Placeholder 1]
							,[Elevators]
							,[Type of Construction]
							,[Other Buildings]
							,[School District]
							,REPLACE([Zoning], '"', '') AS Zoning
						INTO #NumberOfUnits_Data
						FROM StageLanding.CustomLoad_Commercial_Inventory
						UNPIVOT(
							NumberOfUnits FOR NumberOfBedrooms IN ( 
																	[Apartment – Number of Bachelor Units]
																	,[Apartment – Number of 1 Bedroom Units]
																	,[Apartment – Number of 2 Bedroom Units]
																	,[Apartment – Number of 3 Bedroom Units]
																	,[Apartment – Number of 4 Bedroom Units]
																	)
								) AS Nunits;

						-- Insert Union Data
						DROP TABLE IF EXISTS #MultiFamily_Commercial_Inventory;

						Select * INTO #MultiFamily_Commercial_Inventory
						FROM
						(
						SELECT Area
							,Jurisdiction
							,[Roll Number]
							,[Year Built]
							,[Effective Year]
							,[Number of Storeys]
							,[Gross Leasable Area]
							,[Net Leasable Area]
							,ParkingTotal
							,ParkingType
							,[Apartment – Number of Units] AS NumberOfUnits
							,'' AS NumberOfBedrooms
							,[Number of Units in an Co-op]
							,[Number of Bachelor Units in a Co-op]
							,[Number of 1 Bedroom Units in a Co-op]
							,[Number of 2 Bedroom Units in a Co-op]
							,[Number of 3 Bedroom Units in a Co-op]
							,[Number of 4 Bedroom Units in a Co-op]
							,[Predominant Manual Class]
							,[Gross Building Area]
							,[Basement Area]
							,[Strata Industrial / Commercial Lot Area]
							,[Apartment – Number of House Keeping Rooms]
							,[Number of Units in a Hotel]
							,[Number of Units in Motel]
							,[Senior Housing – Number of Units]
							,[Senior Housing – Number of Bachelor Units]
							,[Senior Housing – Number of 1 Bedrooms]
							,[Senior Housing – Number of 2 Bedrooms]
							,[Senior Housing – Number of 3 Bedrooms]
							,[Senior Housing – Number of Bed Sitting Rooms]
							,[Senior Housing – Number of Licensed Care Private Beds]
							,[Senior Housing – Number of Licensed]
							,[Care Semi-Private Beds]
							,[Total Balcony Area]
							,[Mezzanine Area]
							,[Type of Heating]
							,[Placeholder 1]
							,[Elevators]
							,[Type of Construction]
							,[Other Buildings]
							,[School District]
							,Zoning
						FROM #ParkingType_Data
						
						UNION
						
						SELECT Area
							,Jurisdiction
							,[Roll Number]
							,[Year Built]
							,[Effective Year]
							,[Number of Storeys]
							,[Gross Leasable Area]
							,[Net Leasable Area]
							,ParkingTotal
							,'' AS ParkingType
							,NumberOfUnits
							,NumberOfBedrooms
							,[Number of Units in an Co-op]
							,[Number of Bachelor Units in a Co-op]
							,[Number of 1 Bedroom Units in a Co-op]
							,[Number of 2 Bedroom Units in a Co-op]
							,[Number of 3 Bedroom Units in a Co-op]
							,[Number of 4 Bedroom Units in a Co-op]
							,[Predominant Manual Class]
							,[Gross Building Area]
							,[Basement Area]
							,[Strata Industrial / Commercial Lot Area]
							,[Apartment – Number of House Keeping Rooms]
							,[Number of Units in a Hotel]
							,[Number of Units in Motel]
							,[Senior Housing – Number of Units]
							,[Senior Housing – Number of Bachelor Units]
							,[Senior Housing – Number of 1 Bedrooms]
							,[Senior Housing – Number of 2 Bedrooms]
							,[Senior Housing – Number of 3 Bedrooms]
							,[Senior Housing – Number of Bed Sitting Rooms]
							,[Senior Housing – Number of Licensed Care Private Beds]
							,[Senior Housing – Number of Licensed]
							,[Care Semi-Private Beds]
							,[Total Balcony Area]
							,[Mezzanine Area]
							,[Type of Heating]
							,[Placeholder 1]
							,[Elevators]
							,[Type of Construction]
							,[Other Buildings]
							,[School District]
							,Zoning
						FROM #NumberOfUnits_Data
						)A;

						SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
						SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@TableName+' ;';
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;
												
						SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
						SET @DynamicSQL='INSERT INTO '+@StageLandSchema+@TableName+' ([Area],[Jurisdiction],[Roll Number],[Year Built],
						[Effective Year],[Number of Storeys],[Gross Leasable Area],[Net Leasable Area],[ParkingTotal],[ParkingType],
						[NumberOfUnits],[NumberOfBedrooms],[Number of Units in an Co-op],[Number of Bachelor Units in a Co-op],
						[Number of 1 Bedroom Units in a Co-op],[Number of 2 Bedroom Units in a Co-op],[Number of 3 Bedroom Units in a Co-op],
						[Number of 4 Bedroom Units in a Co-op],[Predominant Manual Class],[Gross Building Area],[Basement Area],
						[Strata Industrial / Commercial Lot Area],[Apartment – Number of House Keeping Rooms],[Number of Units in a Hotel],
						[Number of Units in Motel],[Senior Housing – Number of Units],[Senior Housing – Number of Bachelor Units],
						[Senior Housing – Number of 1 Bedrooms],[Senior Housing – Number of 2 Bedrooms],[Senior Housing – Number of 3 Bedrooms],
						[Senior Housing – Number of Bed Sitting Rooms],[Senior Housing – Number of Licensed Care Private Beds],
						[Senior Housing – Number of Licensed],[Care Semi-Private Beds],[Total Balcony Area],[Mezzanine Area],
						[Type of Heating],[Placeholder 1],[Elevators],[Type of Construction],[Other Buildings],[School District],[Zoning]) 
						
						Select * from #MultiFamily_Commercial_Inventory;'
						
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;

						SET @Params ='@StageLandSchema VARCHAR(50),@CustomLoad_TableName VARCHAR(100)';
						SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@CustomLoad_TableName+' ;';
						EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@CustomLoad_TableName	;
						
						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage ='Completed loading to StageLanding'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'
						,	@Inserts = @@ROWCOUNT;

						EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'Process'
						,	@ProcessName = @ProcessName
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'
						,	@Stage = 'Landing';

					END TRY 

					BEGIN CATCH
						UPDATE Stage.ExternalFileslist SET IsError=1 WHERE FileName=@ExternalFileName;

							SET @IsError=1
							SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';
							SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@TableName+' ;';
							EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	;

							EXEC ETLProcess.AuditLog
								@ProcessCategory = @ProcessCategory
							,	@Phase = 'ProcessHistory'
							,	@ProcessName = @ProcessName
							,	@Stage ='Error loading to StageLanding'
							,	@Status = 'Error'
							,	@CurrentStatus = 'Error'
							,	@Inserts = 0;

							EXEC ETLProcess.AuditLog
								@ProcessCategory = @ProcessCategory
							,	@Phase = 'Process'
							,	@ProcessName = @ProcessName
							,	@Status = 'Error'
							,	@CurrentStatus = 'Error'
							,	@Stage = 'Landing';	

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
							,	@ProcessStage='Landing'
							,	@ErrorMessage='Failed to Load StageLanding'
							,	@IsError='Yes';
						END CATCH
			END
		END		
	END	

	IF @IsError=1
		THROW 50005, N'An error occurred while loading data to StageLanding', 1;
END