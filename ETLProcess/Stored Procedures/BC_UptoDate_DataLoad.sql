


CREATE PROCEDURE [ETLProcess].[BC_UptoDate_DataLoad]
AS
BEGIN
	DECLARE @StartDate DATETIME;
	DECLARE @LastModifiedDateUTC DATETIME;
	DECLARE @ProcessName VARCHAR(100) = 'BC AddFolio Through Weekly File';
	DECLARE @ProcessCategory VARCHAR(100) = 'DTC_ExternalSource_ETL';
	DECLARE @ErrorProcedure VARCHAR(100);

	SET @StartDate = GETDATE();

	SELECT @ErrorProcedure = s.name + '.' + o.name
	FROM SYS.OBJECTS O
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID = O.SCHEMA_ID
	WHERE OBJECT_ID = @@PROCID;

	BEGIN TRY
		BEGIN TRAN

		INSERT INTO dbo.BC_UPTO_DATE (
			FolioRecord_ID
			,RollYear
			,AssessmentAreaDescription
			,JurisdictionCode
			,JurisdictionDescription
			,RollNumber
			,ActualUseDescription
			,VacantFlag
			,TenureDescription
			,FolioAddresses_FolioAddress_City
			,FolioAddresses_FolioAddress_ID
			,FolioAddresses_FolioAddress_PostalZip
			,FolioAddresses_FolioAddress_PrimaryFlag
			,FolioAddresses_FolioAddress_ProvinceState
			,FolioAddresses_FolioAddress_StreetDirectionSuffix
			,FolioAddresses_FolioAddress_StreetName
			,FolioAddresses_FolioAddress_StreetNumber
			,FolioAddresses_FolioAddress_StreetType
			,FolioAddresses_FolioAddress_UnitNumber
			,LandMeasurement_LandDepth
			,LandMeasurement_LandDimension
			,LandMeasurement_LandDimensionTypeDescription
			,LandMeasurement_LandWidth
			,FolioDescription_Neighbourhood_NeighbourhoodCode
			,FolioDescription_Neighbourhood_NeighbourhoodDescription
			,RegionalDistrict_DistrictDescription
			,SchoolDistrict_DistrictDescription
			,LegalDescriptions_LegalDescription_Block
			,LegalDescriptions_LegalDescription_DistrictLot
			,LegalDescriptions_LegalDescription_ExceptPlan
			,LegalDescriptions_LegalDescription_FormattedLegalDescription
			,LegalDescriptions_LegalDescription_ID
			,LegalDescriptions_LegalDescription_LandDistrict
			,LegalDescriptions_LegalDescription_LandDistrictDescription
			,LegalDescriptions_LegalDescription_LeaseLicenceNumber
			,LegalDescriptions_LegalDescription_LegalText
			,LegalDescriptions_LegalDescription_Lot
			,LegalDescriptions_LegalDescription_Meridian
			,LegalDescriptions_LegalDescription_MeridianShort
			,LegalDescriptions_LegalDescription_Parcel
			,LegalDescriptions_LegalDescription_Part1
			,LegalDescriptions_LegalDescription_Part2
			,LegalDescriptions_LegalDescription_Part3
			,LegalDescriptions_LegalDescription_Part4
			,LegalDescriptions_LegalDescription_PID
			,LegalDescriptions_LegalDescription_Plan
			,LegalDescriptions_LegalDescription_Portion
			,LegalDescriptions_LegalDescription_Range
			,LegalDescriptions_LegalDescription_Section
			,LegalDescriptions_LegalDescription_StrataLot
			,LegalDescriptions_LegalDescription_SubBlock
			,LegalDescriptions_LegalDescription_SubLot
			,LegalDescriptions_LegalDescription_LegalSubdivision
			,LegalDescriptions_LegalDescription_Township
			,Sales_Sale_ConveyanceDate
			,Sales_Sale_ConveyancePrice
			,Sales_Sale_ID
			,HashBytes
			,DateCreatedUTC
			,LastModifiedDateUTC
			,Code
			)
		SELECT DISTINCT Landing.FolioRecord_ID
			,Landing.RollYear
			,Landing.AssessmentAreaDescription
			,Landing.JurisdictionCode
			,Landing.JurisdictionDescription
			,Landing.RollNumber
			,Landing.ActualUseDescription
			,Landing.VacantFlag
			,Landing.TenureDescription
			,Landing.FolioAddresses_FolioAddress_City
			,Landing.FolioAddresses_FolioAddress_ID
			,Landing.FolioAddresses_FolioAddress_PostalZip
			,Landing.FolioAddresses_FolioAddress_PrimaryFlag
			,Landing.FolioAddresses_FolioAddress_ProvinceState
			,Landing.FolioAddresses_FolioAddress_StreetDirectionSuffix
			,Landing.FolioAddresses_FolioAddress_StreetName
			,Landing.FolioAddresses_FolioAddress_StreetNumber
			,Landing.FolioAddresses_FolioAddress_StreetType
			,Landing.FolioAddresses_FolioAddress_UnitNumber
			,Landing.LandMeasurement_LandDepth
			,Landing.LandMeasurement_LandDimension
			,Landing.LandMeasurement_LandDimensionTypeDescription
			,Landing.LandMeasurement_LandWidth
			,Landing.FolioDescription_Neighbourhood_NeighbourhoodCode
			,Landing.FolioDescription_Neighbourhood_NeighbourhoodDescription
			,Landing.RegionalDistrict_DistrictDescription
			,Landing.SchoolDistrict_DistrictDescription
			,Landing.LegalDescriptions_LegalDescription_Block
			,Landing.LegalDescriptions_LegalDescription_DistrictLot
			,Landing.LegalDescriptions_LegalDescription_ExceptPlan
			,Landing.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,Landing.LegalDescriptions_LegalDescription_ID
			,Landing.LegalDescriptions_LegalDescription_LandDistrict
			,Landing.LegalDescriptions_LegalDescription_LandDistrictDescription
			,Landing.LegalDescriptions_LegalDescription_LeaseLicenceNumber
			,Landing.LegalDescriptions_LegalDescription_LegalText
			,Landing.LegalDescriptions_LegalDescription_Lot
			,Landing.LegalDescriptions_LegalDescription_Meridian
			,Landing.LegalDescriptions_LegalDescription_MeridianShort
			,Landing.LegalDescriptions_LegalDescription_Parcel
			,Landing.LegalDescriptions_LegalDescription_Part1
			,Landing.LegalDescriptions_LegalDescription_Part2
			,Landing.LegalDescriptions_LegalDescription_Part3
			,Landing.LegalDescriptions_LegalDescription_Part4
			,Landing.LegalDescriptions_LegalDescription_PID
			,Landing.LegalDescriptions_LegalDescription_Plan
			,Landing.LegalDescriptions_LegalDescription_Portion
			,Landing.LegalDescriptions_LegalDescription_Range
			,Landing.LegalDescriptions_LegalDescription_Section
			,Landing.LegalDescriptions_LegalDescription_StrataLot
			,Landing.LegalDescriptions_LegalDescription_SubBlock
			,Landing.LegalDescriptions_LegalDescription_SubLot
			,Landing.LegalDescriptions_LegalDescription_LegalSubdivision
			,Landing.LegalDescriptions_LegalDescription_Township
			,Landing.Sales_Sale_ConveyanceDate
			,Landing.Sales_Sale_ConveyancePrice
			,Landing.Sales_Sale_ID
			,HASHBYTES('SHA2_512', CONCAT_WS('|', 
					Landing.FolioRecord_ID, Landing.RollYear, Landing.AssessmentAreaDescription, Landing.JurisdictionCode, Landing.JurisdictionDescription, Landing.RollNumber, Landing.ActualUseDescription, Landing.VacantFlag, Landing.TenureDescription, Landing.FolioAddresses_FolioAddress_City, Landing.FolioAddresses_FolioAddress_ID, Landing.FolioAddresses_FolioAddress_PostalZip, Landing.FolioAddresses_FolioAddress_PrimaryFlag, Landing.FolioAddresses_FolioAddress_ProvinceState, Landing.FolioAddresses_FolioAddress_StreetDirectionSuffix, Landing.FolioAddresses_FolioAddress_StreetName, Landing.FolioAddresses_FolioAddress_StreetNumber, Landing.FolioAddresses_FolioAddress_StreetType, Landing.FolioAddresses_FolioAddress_UnitNumber, Landing.LandMeasurement_LandDepth, Landing.LandMeasurement_LandDimension, Landing.LandMeasurement_LandDimensionTypeDescription, Landing.LandMeasurement_LandWidth, Landing.FolioDescription_Neighbourhood_NeighbourhoodCode, 
					Landing.FolioDescription_Neighbourhood_NeighbourhoodDescription, Landing.RegionalDistrict_DistrictDescription, Landing.SchoolDistrict_DistrictDescription, Landing.LegalDescriptions_LegalDescription_Block, Landing.LegalDescriptions_LegalDescription_DistrictLot, Landing.LegalDescriptions_LegalDescription_ExceptPlan, Landing.LegalDescriptions_LegalDescription_FormattedLegalDescription, Landing.LegalDescriptions_LegalDescription_ID, Landing.LegalDescriptions_LegalDescription_LandDistrict, Landing.LegalDescriptions_LegalDescription_LandDistrictDescription, Landing.LegalDescriptions_LegalDescription_LeaseLicenceNumber, Landing.LegalDescriptions_LegalDescription_LegalText, Landing.LegalDescriptions_LegalDescription_Lot, Landing.LegalDescriptions_LegalDescription_Meridian, Landing.LegalDescriptions_LegalDescription_MeridianShort, Landing.LegalDescriptions_LegalDescription_Parcel, Landing.LegalDescriptions_LegalDescription_Part1, Landing.LegalDescriptions_LegalDescription_Part2, 
					Landing.LegalDescriptions_LegalDescription_Part3, Landing.LegalDescriptions_LegalDescription_Part4, Landing.LegalDescriptions_LegalDescription_PID, Landing.LegalDescriptions_LegalDescription_Plan, Landing.LegalDescriptions_LegalDescription_Portion, Landing.LegalDescriptions_LegalDescription_Range, Landing.LegalDescriptions_LegalDescription_Section, Landing.LegalDescriptions_LegalDescription_StrataLot, Landing.LegalDescriptions_LegalDescription_SubBlock, Landing.LegalDescriptions_LegalDescription_SubLot, Landing.LegalDescriptions_LegalDescription_LegalSubdivision, Landing.LegalDescriptions_LegalDescription_Township, Landing.Sales_Sale_ConveyanceDate, Landing.Sales_Sale_ConveyancePrice, Landing.Sales_Sale_ID
					)) AS HashBytes
			,@StartDate AS DateCreatedUTC
			,@StartDate AS LastModifiedDateUTC
			,BC.Code AS Code
		FROM StageLanding.BC_UPTO_DATE Landing WITH (NOLOCK)
		LEFT join SourceHistory.BC_ALL_Assessment BC ON
			ISNULL(BC.ActualUseDescription,'')	= ISNULL(Landing.ActualUseDescription,'')
			AND LTRIM(RTRIM(ISNULL(BC.AssessmentAreaDescription,'')))	= LTRIM(RTRIM(ISNULL(Landing.AssessmentAreaDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_City,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_City,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_PostalZip,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_PostalZip,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_ProvinceState,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_ProvinceState,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_StreetDirectionSuffix,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_StreetDirectionSuffix,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_StreetName,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_StreetName,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_StreetNumber,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_StreetNumber,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_StreetType,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_StreetType,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioAddresses_FolioAddress_UnitNumber,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioAddresses_FolioAddress_UnitNumber,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioDescription_Neighbourhood_NeighbourhoodCode,'')))	= LTRIM(RTRIM(ISNULL(Landing.FolioDescription_Neighbourhood_NeighbourhoodCode,'')))
			AND LTRIM(RTRIM(ISNULL(BC.FolioDescription_Neighbourhood_NeighbourhoodDescription,'')))	= LTRIM(RTRIM(ISNULL( Landing.FolioDescription_Neighbourhood_NeighbourhoodDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.JurisdictionCode,'')))	= LTRIM(RTRIM(ISNULL(Landing.JurisdictionCode,'')))
			AND LTRIM(RTRIM(ISNULL(BC.JurisdictionDescription,'')))	= LTRIM(RTRIM(ISNULL(Landing.JurisdictionDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LandMeasurement_LandDepth,'')))	= LTRIM(RTRIM(ISNULL(Landing.LandMeasurement_LandDepth,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LandMeasurement_LandDimension,'')))	= LTRIM(RTRIM(ISNULL(Landing.LandMeasurement_LandDimension,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LandMeasurement_LandDimensionTypeDescription,'')))	= LTRIM(RTRIM(ISNULL(Landing.LandMeasurement_LandDimensionTypeDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LandMeasurement_LandWidth,'')))	= LTRIM(RTRIM(ISNULL(Landing.LandMeasurement_LandWidth,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LegalDescriptions_LegalDescription_FormattedLegalDescription,'')))	= LTRIM(RTRIM(ISNULL( Landing.LegalDescriptions_LegalDescription_FormattedLegalDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LegalDescriptions_LegalDescription_PID,'')))	= LTRIM(RTRIM(ISNULL(Landing.LegalDescriptions_LegalDescription_PID,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LegalDescriptions_LegalDescription_Range,'')))	= LTRIM(RTRIM(ISNULL(Landing.LegalDescriptions_LegalDescription_Range,'')))
			AND LTRIM(RTRIM(ISNULL(BC.LegalDescriptions_LegalDescription_Township,'')))	= LTRIM(RTRIM(ISNULL(Landing.LegalDescriptions_LegalDescription_Township,'')))
			AND LTRIM(RTRIM(ISNULL(BC.RollNumber,'')))	= LTRIM(RTRIM(ISNULL(Landing.RollNumber,'')))
			AND LTRIM(RTRIM(ISNULL(BC.RollYear,'')))	= LTRIM(RTRIM(ISNULL(Landing.RollYear,'')))
			AND LTRIM(RTRIM(ISNULL(BC.Sales_Sale_ConveyanceDate,'')))	= LTRIM(RTRIM(ISNULL(Landing.Sales_Sale_ConveyanceDate,'')))
			AND LTRIM(RTRIM(ISNULL(BC.Sales_Sale_ConveyancePrice,'')))	= LTRIM(RTRIM(ISNULL(Landing.Sales_Sale_ConveyancePrice,'')))
			AND LTRIM(RTRIM(ISNULL(BC.SchoolDistrict_DistrictDescription,'')))	= LTRIM(RTRIM(ISNULL(Landing.SchoolDistrict_DistrictDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.TenureDescription,'')))	= LTRIM(RTRIM(ISNULL(Landing.TenureDescription,'')))
			AND LTRIM(RTRIM(ISNULL(BC.VacantFlag,'')))	= LTRIM(RTRIM(ISNULL(Landing.VacantFlag,'')))
			AND BC.IsDuplicate = 0
		WHERE NOT EXISTS (
				SELECT 1
				FROM DBO.BC_UPTO_DATE UPDT
				WHERE UPDT.HASHBYTES = HASHBYTES('SHA2_512', CONCAT_WS('|', 
								Landing.FolioRecord_ID, Landing.RollYear, Landing.AssessmentAreaDescription, Landing.JurisdictionCode, Landing.JurisdictionDescription, Landing.RollNumber, Landing.ActualUseDescription, Landing.VacantFlag, Landing.TenureDescription, Landing.FolioAddresses_FolioAddress_City, Landing.FolioAddresses_FolioAddress_ID, Landing.FolioAddresses_FolioAddress_PostalZip, Landing.FolioAddresses_FolioAddress_PrimaryFlag, Landing.FolioAddresses_FolioAddress_ProvinceState, Landing.FolioAddresses_FolioAddress_StreetDirectionSuffix, Landing.FolioAddresses_FolioAddress_StreetName, Landing.FolioAddresses_FolioAddress_StreetNumber, Landing.FolioAddresses_FolioAddress_StreetType, Landing.FolioAddresses_FolioAddress_UnitNumber, Landing.LandMeasurement_LandDepth, Landing.LandMeasurement_LandDimension, Landing.LandMeasurement_LandDimensionTypeDescription, Landing.LandMeasurement_LandWidth, Landing.FolioDescription_Neighbourhood_NeighbourhoodCode, 
								Landing.FolioDescription_Neighbourhood_NeighbourhoodDescription, Landing.RegionalDistrict_DistrictDescription, Landing.SchoolDistrict_DistrictDescription, Landing.LegalDescriptions_LegalDescription_Block, Landing.LegalDescriptions_LegalDescription_DistrictLot, Landing.LegalDescriptions_LegalDescription_ExceptPlan, Landing.LegalDescriptions_LegalDescription_FormattedLegalDescription, Landing.LegalDescriptions_LegalDescription_ID, Landing.LegalDescriptions_LegalDescription_LandDistrict, Landing.LegalDescriptions_LegalDescription_LandDistrictDescription, Landing.LegalDescriptions_LegalDescription_LeaseLicenceNumber, Landing.LegalDescriptions_LegalDescription_LegalText, Landing.LegalDescriptions_LegalDescription_Lot, Landing.LegalDescriptions_LegalDescription_Meridian, Landing.LegalDescriptions_LegalDescription_MeridianShort, Landing.LegalDescriptions_LegalDescription_Parcel, Landing.LegalDescriptions_LegalDescription_Part1, Landing.LegalDescriptions_LegalDescription_Part2, 
								Landing.LegalDescriptions_LegalDescription_Part3, Landing.LegalDescriptions_LegalDescription_Part4, Landing.LegalDescriptions_LegalDescription_PID, Landing.LegalDescriptions_LegalDescription_Plan, Landing.LegalDescriptions_LegalDescription_Portion, Landing.LegalDescriptions_LegalDescription_Range, Landing.LegalDescriptions_LegalDescription_Section, Landing.LegalDescriptions_LegalDescription_StrataLot, Landing.LegalDescriptions_LegalDescription_SubBlock, Landing.LegalDescriptions_LegalDescription_SubLot, Landing.LegalDescriptions_LegalDescription_LegalSubdivision, Landing.LegalDescriptions_LegalDescription_Township, Landing.Sales_Sale_ConveyanceDate, Landing.Sales_Sale_ConveyancePrice, Landing.Sales_Sale_ID
								))
				);
			
		--Update load date in table
		UPDATE dbo.BC_UPTO_DATE_LoadDate
		SET LoadDate = DATEADD (SS,1,@StartDate)
		Where ProcessName='BC_UPTO_DATE';

		COMMIT TRAN;
	END TRY

	BEGIN CATCH
		ROLLBACK TRAN;

		INSERT INTO ETLProcess.ETLStoredProcedureErrors (
			ProcessCategory
			,ProcessName
			,ErrorNumber
			,ErrorSeverity
			,ErrorState
			,ErrorProcedure
			,ErrorLine
			,ErrorMessage
			,ErrorDate
			)
		SELECT @ProcessCategory
			,@ProcessName
			,ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,@ErrorProcedure
			,ERROR_LINE() AS ErrorLine
			,ERROR_MESSAGE() AS ErrorMessage
			,GETDATE()
	END CATCH
END