CREATE PROCEDURE [ETLProcess].[BC_Add_MissingSales_Through_Weekly]
AS
BEGIN
	/****************************************************************************************
-- AUTHOR		: Shirish W.
-- DATE			: 09/07/2022
-- PURPOSE		: Add records into dbo.BC_Upto_Date through BC Weekly
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** --------------------------------------------------------------------------------------
** 09/25/2020	Shirish W.	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @StartDate DATETIME;
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @ProcessName VARCHAR(100) = 'BC AddFolio Through Weekly File' ; 
	DECLARE @ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';

	SET @StartDate = GETDATE();

	SELECT @ErrorProcedure = s.name + '.' + o.name
	FROM SYS.OBJECTS O
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID = O.SCHEMA_ID
	WHERE OBJECT_ID = @@PROCID;

	BEGIN TRY
		BEGIN TRAN

		DROP TABLE IF EXISTS #Sales_Weekly;
		DROP TABLE IF EXISTS #Sales_UptoDate;

			SELECT WK.FolioRecord_ID
				,WK.RollYear
				,WK.RollNumber
				,WK.FolioAddresses_FolioAddress_Action
				,WK.FolioAddresses_FolioAddress_ID
				,WK.LegalDescriptions_LegalDescription_Action
				,WK.LegalDescriptions_LegalDescription_ID
				,WK.Sales_Sale_ConveyanceDate
				,WK.Sales_Sale_ConveyancePrice
				,WK.Sales_Sale_ID
				,WK.Sales_Sale_Action
				,NULL AS IsPrimaryAddressesPresent
			INTO #Sales_Weekly
			FROM StageLanding.BC_ALL_Assessment_Weekly WK
			LEFT JOIN DBO.BC_UPTO_DATE UPTD ON UPTD.FolioRecord_ID = WK.FolioRecord_ID
				AND UPTD.Sales_Sale_ID = WK.Sales_Sale_ID
			WHERE Sales_Sale_Action = 'Add'
			AND UPTD.Sales_Sale_ID IS NULL;

		SELECT DISTINCT UPTD.FolioRecord_ID
			,WK.RollYear
			,UPTD.AssessmentAreaDescription
			,UPTD.JurisdictionCode
			,UPTD.JurisdictionDescription
			,UPTD.RollNumber
			,UPTD.ActualUseDescription
			,UPTD.VacantFlag
			,UPTD.TenureDescription
			,UPTD.FolioAddresses_FolioAddress_City
			,UPTD.FolioAddresses_FolioAddress_ID
			,UPTD.FolioAddresses_FolioAddress_PostalZip
			,UPTD.FolioAddresses_FolioAddress_PrimaryFlag
			,UPTD.FolioAddresses_FolioAddress_ProvinceState
			,UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
			,UPTD.FolioAddresses_FolioAddress_StreetName
			,UPTD.FolioAddresses_FolioAddress_StreetNumber
			,UPTD.FolioAddresses_FolioAddress_StreetType
			,UPTD.FolioAddresses_FolioAddress_UnitNumber
			,UPTD.LandMeasurement_LandDepth
			,UPTD.LandMeasurement_LandDimension
			,UPTD.LandMeasurement_LandDimensionTypeDescription
			,UPTD.LandMeasurement_LandWidth
			,UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
			,UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
			,UPTD.RegionalDistrict_DistrictDescription
			,UPTD.SchoolDistrict_DistrictDescription
			,UPTD.LegalDescriptions_LegalDescription_Block
			,UPTD.LegalDescriptions_LegalDescription_DistrictLot
			,UPTD.LegalDescriptions_LegalDescription_ExceptPlan
			,UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,UPTD.LegalDescriptions_LegalDescription_ID
			,UPTD.LegalDescriptions_LegalDescription_LandDistrict
			,UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription
			,UPTD.LegalDescriptions_LegalDescription_LeaseLicenceNumber
			,UPTD.LegalDescriptions_LegalDescription_LegalText
			,UPTD.LegalDescriptions_LegalDescription_Lot
			,UPTD.LegalDescriptions_LegalDescription_Meridian
			,UPTD.LegalDescriptions_LegalDescription_MeridianShort
			,UPTD.LegalDescriptions_LegalDescription_Parcel
			,UPTD.LegalDescriptions_LegalDescription_Part1
			,UPTD.LegalDescriptions_LegalDescription_Part2
			,UPTD.LegalDescriptions_LegalDescription_Part3
			,UPTD.LegalDescriptions_LegalDescription_Part4
			,UPTD.LegalDescriptions_LegalDescription_PID
			,UPTD.LegalDescriptions_LegalDescription_Plan
			,UPTD.LegalDescriptions_LegalDescription_Portion
			,UPTD.LegalDescriptions_LegalDescription_Range
			,UPTD.LegalDescriptions_LegalDescription_Section
			,UPTD.LegalDescriptions_LegalDescription_StrataLot
			,UPTD.LegalDescriptions_LegalDescription_SubBlock
			,UPTD.LegalDescriptions_LegalDescription_SubLot
			,UPTD.LegalDescriptions_LegalDescription_LegalSubdivision
			,UPTD.LegalDescriptions_LegalDescription_Township
			,WK.Sales_Sale_ConveyanceDate
			,WK.Sales_Sale_ConveyancePrice
			,WK.Sales_Sale_ID
			,HASHBYTES('SHA2_512', CONCAT_WS('|', UPTD.FolioRecord_ID, WK.RollYear, UPTD.AssessmentAreaDescription, UPTD.JurisdictionCode, UPTD.JurisdictionDescription, UPTD.RollNumber, UPTD.ActualUseDescription, UPTD.VacantFlag, UPTD.TenureDescription, UPTD.FolioAddresses_FolioAddress_City, UPTD.FolioAddresses_FolioAddress_ID, UPTD.FolioAddresses_FolioAddress_PostalZip, UPTD.FolioAddresses_FolioAddress_PrimaryFlag, UPTD.FolioAddresses_FolioAddress_ProvinceState, UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix, UPTD.FolioAddresses_FolioAddress_StreetName, UPTD.FolioAddresses_FolioAddress_StreetNumber, UPTD.FolioAddresses_FolioAddress_StreetType, UPTD.FolioAddresses_FolioAddress_UnitNumber, UPTD.LandMeasurement_LandDepth, UPTD.LandMeasurement_LandDimension, UPTD.LandMeasurement_LandDimensionTypeDescription, UPTD.LandMeasurement_LandWidth, UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode, UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription, UPTD.RegionalDistrict_DistrictDescription, 
					UPTD.SchoolDistrict_DistrictDescription, UPTD.LegalDescriptions_LegalDescription_Block, UPTD.LegalDescriptions_LegalDescription_DistrictLot, UPTD.LegalDescriptions_LegalDescription_ExceptPlan, UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription, UPTD.LegalDescriptions_LegalDescription_ID, UPTD.LegalDescriptions_LegalDescription_LandDistrict, UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription, UPTD.LegalDescriptions_LegalDescription_LeaseLicenceNumber, UPTD.LegalDescriptions_LegalDescription_LegalText, UPTD.LegalDescriptions_LegalDescription_Lot, UPTD.LegalDescriptions_LegalDescription_Meridian, UPTD.LegalDescriptions_LegalDescription_MeridianShort, UPTD.LegalDescriptions_LegalDescription_Parcel, UPTD.LegalDescriptions_LegalDescription_Part1, UPTD.LegalDescriptions_LegalDescription_Part2, UPTD.LegalDescriptions_LegalDescription_Part3, UPTD.LegalDescriptions_LegalDescription_Part4, UPTD.LegalDescriptions_LegalDescription_PID, UPTD.
					LegalDescriptions_LegalDescription_Plan, UPTD.LegalDescriptions_LegalDescription_Portion, UPTD.LegalDescriptions_LegalDescription_Range, UPTD.LegalDescriptions_LegalDescription_Section, UPTD.LegalDescriptions_LegalDescription_StrataLot, UPTD.LegalDescriptions_LegalDescription_SubBlock, UPTD.LegalDescriptions_LegalDescription_SubLot, UPTD.LegalDescriptions_LegalDescription_LegalSubdivision, UPTD.LegalDescriptions_LegalDescription_Township, WK.Sales_Sale_ConveyanceDate, WK.Sales_Sale_ConveyancePrice, WK.Sales_Sale_ID)) AS HashBytes
		INTO #Sales_UptoDate
		FROM dbo.BC_UPTO_DATE UPTD
		INNER JOIN #Sales_Weekly WK ON UPTD.FolioRecord_ID = WK.FolioRecord_ID
			AND UPTD.FolioAddresses_FolioAddress_PrimaryFlag = 'True'
			AND UPTD.RollYear = WK.RollYear - 1;

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
			)
		SELECT DISTINCT *
			,@StartDate AS DateCreatedUTC
			,@StartDate AS LastModifiedDateUTC
		FROM #Sales_UptoDate NF
		WHERE NOT EXISTS (
				SELECT 1
				FROM dbo.BC_UPTO_DATE uptd
				WHERE uptd.HashBytes = NF.HashBytes
				);

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