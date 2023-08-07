

CREATE PROCEDURE [ETLProcess].[EntityCleanse]          
(          
	@ProcessName VARCHAR(100)   
,	@EntityName VARCHAR(100)   

 )          
AS        
-- =============================================            
-- Author:      Rahul Singh            
-- Create Date: 2020-09-09            
-- Description:             
-- Version History    1. Created 2020=09-09  
--					  2. Modified Proc to add isDuplicate Logic
-- =============================================      
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
	DECLARE @data_source_id int=1
	DECLARE @max_data_source_id int=1
	DECLARE @cnt int=0
	
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
			
			SET @InsertClause= @InsertClause+N'InvalidRuleId)';

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
				SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid  '+N' FROM '+@TableName+@Join+N' WHERE CASE '+ @CASE + ' END <> 0;'			
			ELSE
				--SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid  '+N' FROM '+@TableName+N' WHERE CASE '+ @CASE + ' END <> 0 AND LastModifiedDateUTC > CAST('''+CAST(@LastRetrievedDateTime AS NVARCHAR(30))+N''' AS DATETIME)  ;'			
				SET @SelectClause= @SelectClause+N'CASE' + @CASE + ' END AS InvalidRuleid  '+N' FROM '+@TableName+@Join+N' WHERE CASE '+ @CASE + ' END <> 0 AND '+@DateClause+'  ;'			

			

			SET @strSQL=@InsertClause+@SelectClause

			SET @DeleteSQL = 'DELETE ' + @EntityName + ' FROM ' + @EntityName + ' INNER JOIN ' + @EntityName + '_Invalid ON ' + @EntityName + '.Code = ' + @EntityName + '_Invalid.Code;'
					
			BEGIN TRY
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

			/****************************************************************************************************************************************
			Address deduplication  00:01:13
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0
			

			IF (@tableName='dbo.Address')
			BEGIN
				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.address'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select IsDuplicate,
										ROW_NUMBER() Over(Partition by [Data_Source_ID],[MasterAddressID],[UnitNumber],[StreetNumber],[StreetName],[StreetType],[StreetDirection],[City],[PostalCode],[ProvinceCode],[FSA],[District],[JurCode],[Country],[FullAddress],[Latitude],[Longitude],[LatitudeLongitude],[Neighbourhood],[NeighbourhoodDescription],[Municipality],[Region],[Township],[Range],[LandDistrict],[LandDistrictName],[AreaDescription],[JurDescription],[SchoolDistrictDescription],[CrossStreet],[Community],[IsMunicipalAddress],[RegionalHospitalDistrict_DistrictDescription],[SchoolDistrict]
										order by Code) As RNK
										from dbo.Address
										Where Isduplicate=0
									)
					
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

								EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Address'
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

								THROW 50001, @ErroMessage, 1;

						END CATCH


				

				EXEC ETLProcess.AuditLog
							@ProcessCategory = @ProcessCategory
						,	@Phase = 'ProcessHistory'
						,	@ProcessName = @ProcessName
						,	@Stage = 'Update IsDuplicate completed : dbo.Address'
						,	@Status = 'Completed'
						,	@CurrentStatus = 'Completed'	
						,	@Updates = @cnt;
			END

			/****************************************************************************************************************************************
			Building deduplication -- 00:38:00, 00:00:57
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0


			IF (@tableName='dbo.Building')
			BEGIN
				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.Building'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select IsDuplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressID],[PIN],[ProvinceCode],[BuildingDescription],[BuildingFeet],[BuildingHeight],[BuildingLength],[BuildingM2],[BuildingMeasureUnit],[BuildingMetre],[TypeOfPermit],[BuildingSqft],[BuildingStyle],[BuildingType],[BuildingTypeCode],[YearBuilt],[NumberOfStories],[NumberOfUnits],[LivingAreaSQFT],[HouseTypeCode],[CondoLevel],[CondominumClause],[CondoPlanNumber],[CondoUnitNumber],[HouseArea],[FrontDirection],[CondoExposure],[NumberOfBedrooms],[BedroomPlus],[BedroomString],[NumberOfWashroom],[Furnished],[DenFront],[Description1],[Description2],[Amenities0],[Amenities1],[Amenities2],[Amenities3],[Amenities4],[Pool],[Level],[Locker],[MaintenanceFee],[GarageType],[ParkingType],[Parking],[ParkingGarage],[ParkingText],[ParkingTotal],[UtilitiesIncluded],[Water],[ConstructionMaterial],[ConstructionStatus],[ExteriorFinish],[RoofMaterial],[RoofStyle],[Sewer],[FoundationType],[AirConditioning],[Fireplace],[FireplaceFuel],[FireplaceType],[Heating],[HeatingFuel],[BasementType],[Basement],[FinishedBasement],[Rooms_0_Desc],[Rooms_0_Level],[Rooms_0_Size],[Rooms_0_Type],[Rooms_1_Desc],[Rooms_1_Level],[Rooms_1_Size],[Rooms_1_Type],[Rooms_2_Desc],[Rooms_2_Level],[Rooms_2_Size],[Rooms_2_Type],[Rooms_3_Desc],[Rooms_3_Level],[Rooms_3_Size],[Rooms_3_Type],[Rooms_4_Desc],[Rooms_4_Level],[Rooms_4_Size],[Rooms_4_Type],[Rooms_5_Desc],[Rooms_5_Level],[Rooms_5_Size],[Rooms_5_Type],[Rooms_6_Desc],[Rooms_6_Level],[Rooms_6_Size],[Rooms_6_Type],[Rooms_7_Desc],[Rooms_7_Level],[Rooms_7_Size],[Rooms_7_Type],[Rooms_8_Desc],[Rooms_8_Level],[Rooms_8_Size],[Rooms_8_Type],[Rooms_9_Desc],[Rooms_9_Level],[Rooms_9_Size],[Rooms_9_Type],[Rooms_10_Desc],[Rooms_10_Level],[Rooms_10_Size],[Rooms_10_Type],[Rooms_11_Desc],[Rooms_11_Level],[Rooms_11_Size],[Rooms_11_Type],[IsCondo],[AttachedGarage],[DetachedGarage],[Fuel],[Garage],[IsMobileHome],[IsNewHome],[NewHomeEasement],[EstateTypeCode],[OccupancyTypeCode],[PropertyCategory],[PropertyClassification],[Data_Source_ID],[BasementFinishArea],[BasementTotalArea],[DeckSqFootage],[DeckSqFootageCovered],[Elevators],[MezzanineArea],[NumDens],[OtherBuildingFlag],[TotalBalconyArea],[TypeofHeating] 
										Order by Code)  As RNK
										from dbo.Building b
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Building'
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

								THROW 50001, @ErroMessage, 1;

						END CATCH
		
				

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Building'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;

			END

			/****************************************************************************************************************************************
			Parcel deduplication -- 00:05:06 00:02:45
			****************************************************************************************************************************************/

			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Parcel')
			BEGIN
				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.parcel'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressID],[PIN],[ProvinceCode],[Acreage],[LotDepth],[LotFrontage],[IsNativeLand],[IsEnergy],[IsVacantLand],[IsRenovatedLotNum],[MetesAndBounds],[PrimaryProperty],[GVSEligible],[LotMeasureUnit],[LotSQM],[LotSQFT],[LotHA],[LandSQFT],[LotDescription],[LotSize],[LandType],[LandUse],[PlanNumber],[ZoningDescription],[ZoningCode],[PropertyTypeCode],[PropertyUse],[Easement],[LegalDescription],[Sequence],[Data_Source_ID],[IsPartLot],[LegalDescriptionBlock],[LegalDescriptionDistrictLot],[LegalDescriptionExceptPlan],[LegalDescriptionLegalSubdivision],[LegalDescriptionLegalText],[LegalDescriptionLot],[LegalDescriptionParcel],[LegalDescriptionPart1],[LegalDescriptionPart2],[LegalDescriptionPart3],[LegalDescriptionPart4],[LegalDescriptionPortion],[LegalDescriptionSection],[LegalDescriptionStrataLot],[LegalDescriptionSubBlock],[LegalDescriptionSubLot] 
										Order by Code) As RNK
										from dbo.Parcel b
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Parcel'
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

							THROW 50001, @ErroMessage, 1;

						END CATCH

				

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Parcel'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;


			END

			/****************************************************************************************************************************************
			PIN deduplication -- 00:01:36 00:02:38
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.PIN')
			BEGIN
				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.PIN'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By PIN,OriginalPIN,ProvinceCode,[Data_Source_ID] Order by Code) As RNK
										from dbo.PIN
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.PIN'
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

							THROW 50001, @ErroMessage, 1;
				

						END CATCH
						

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.PIN'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;

			END

			/****************************************************************************************************************************************
			Taxation deduplication -- 00:02:29, 00:01:32
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Taxation')
			BEGIN
				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.taxation'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [ARN],[JurCode],[AssessmentYear],[AssessmentValue],[AnnualTaxAmount],[TaxYear],[TaxAssessedValue],[NetTax],[GrossTax],[AssessmentClass],[ProvinceCode],[Data_Source_ID] 
										Order by Code) As RNK
										from dbo.Taxation
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN
							
							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Taxation'
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

							THROW 50001, @ErroMessage, 1;

						END CATCH
						

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Taxation'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;
			END


			/****************************************************************************************************************************************
			Permit deduplication -- 00:00:10
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Permit')
			BEGIN

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.permit'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressId],[ProvinceCode],[JurCode],[ARN],[AppliedDate],[DateOfDecision],[IssueDate],[MustCommenceDate],[CompletedDate],[CanceledRefusedDate],[DatePermitExpires],[ValueOfConstruction],[PermitClass],[PermitDescription],[PermitType],[PermitFee],[PermitNumber],[PermitStatus],[DwellingUnitsCreated],[DwellingUnitsDemolished],[UnitsNetChange],[Data_Source_ID] 
										Order by Code)  As RNK
										from dbo.Permit
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Permit'
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

								THROW 50001, @ErroMessage, 1;

						END CATCH
						

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Permit'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;
			END

			/****************************************************************************************************************************************
			Business deduplication -- 00:03:40  00:02:52
			***************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0


			IF (@tableName='dbo.Business')
			BEGIN

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.business'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressID],[BusinessCategory],[BusinessCode],[BusinessDescription],[BusinessType],[NaicsCode],[NaicsDescription],[ProvinceCode],[Company],[Data_Source_ID] 
										Order by Code) As RNK
										from dbo.Business
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Business'
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

							THROW 50001, @ErroMessage, 1;

						END CATCH
							

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Business'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;

			END
			/****************************************************************************************************************************************
			Listing deduplication -- 00:01:43 00:02:28
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Listing')
			BEGIN

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.listing'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressID],[PIN],[ProvinceCode],[ARN],[JurCode],[MLSNumber],[SellerName],[DateEnd],[DateStart],[DateUpdate],[ListDays],[ListType],[ListStatus],[ListHistory],[PriceAsked],[FCTTransactionType],[LoanAmt],[LendingValue],[GuaranteeValue],[OwnershipType],[RentAssignment],[Data_Source_ID] 
										Order by Code) As RNK
										from dbo.Listing
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Listing'
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

							THROW 50001, @ErroMessage, 1;

						END CATCH


				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Listing'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;

			END

			/****************************************************************************************************************************************
			Valuation deduplication -- 00:02:41  00:01:35
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Valuation')
			BEGIN

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.Valuation'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressID],[PIN],[ProvinceCode],[ARN],[JurCode],[EstimatedValue],[HighValue],[LowValue],[CompleteDate],[MPACValue],[TERANETValue],[InsuredValue],[MPACConfidenceLevel],[MPACPropertyType],[POSDate],[MPACLowConfidenceLimit],[MPACHighConfidenceLimit],[ValuePurchasePrice],[AppraisedValue],[Data_Source_ID] 
										Order by Code) As RNK
										from dbo.Valuation
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Valuation'
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

							THROW 50001, @ErroMessage, 1;

						END CATCH
						

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Valuation'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;
			END

			/****************************************************************************************************************************************
			Sales deduplication -- 00:01:58  00:01:29
			****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Sales')
			BEGIN

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.Sales'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By [MasterAddressID],[LastSaleDate],[SaleType],[PurchasePrice],[OriginalPurchasePrice],[BuyerName],[PriceSold],[LastSaleAmount],[LastSaleYear],[ClosingDate],[POSDateSales],[StatusID],[Data_Source_ID] 
										Order by Code) As RNK
										from dbo.Sales
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Sales'
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

							THROW 50001, @ErroMessage, 1;

						END CATCH
						

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Sales'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;

			END
			/****************************************************************************************************************************************
			Property deduplication -- 00:02:46
			*****************************************************************************************************************************************/
			--declare @data_source_id int=1
			--,@max_data_source_id int=1
			--,@cnt int=0

			IF (@tableName='dbo.Property')
			BEGIN

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate started : dbo.Property'
								,	@Status = 'InProgress'
								,	@CurrentStatus = 'Started';

				
						BEGIN TRY
								BEGIN TRAN
									;WITH CTE AS
									(
										Select Isduplicate
										,ROW_NUMBER() Over(Partition By MasterAddressID,PIN,ProvinceCode,ARN,JurCode,SUBSTRING(Code,1,Charindex('_',Code)-1) Order by Code) As RNK
										from dbo.Property
										Where Isduplicate=0
									)

										
									UPDATE CTE
									SET Isduplicate=1
									where rnk>1

									set @cnt=@@ROWCOUNT

								COMMIT
						END TRY
						BEGIN CATCH
							ROLLBACK TRAN

							EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage ='Update IsDuplicate Error : dbo.Property'
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

								THROW 50001, @ErroMessage, 1;

						END CATCH
						

				EXEC ETLProcess.AuditLog
									@ProcessCategory = @ProcessCategory
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @ProcessName
								,	@Stage = 'Update IsDuplicate completed : dbo.Property'
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @cnt;
			END

		END 												
	END