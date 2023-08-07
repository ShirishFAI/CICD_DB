







CREATE PROCEDURE [ETLProcess].[BC_DeleteFolio_FROM_UpToDate_Through_Weekly]
AS
BEGIN
	/****************************************************************************************
-- AUTHOR		: Shirish W.
-- DATE			: 09/14/2022
-- PURPOSE		: Delete records into dbo.BC_UPTO_DATE through BC Weekly
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** --------------------------------------------------------------------------------------
** 09/25/2020	Shirish W.	Original Version
******************************************************************************************/

SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @StartDate DATETIME;
	DECLARE @LastModifiedDateUTC DATETIME;
	DECLARE @ProcessName VARCHAR(100) = 'BC DeleteFolio Through Weekly File';
	DECLARE @ProcessCategory VARCHAR(100) = 'DTC_ExternalSource_ETL';
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @ProcessID INT;

	SET @ProcessID = (SELECT ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName='BC_ALL_Assessment')

	SET @StartDate = GETDATE();
	SET @LastModifiedDateUTC = (
			SELECT Max(LastModifiedDateUTC)
			FROM dbo.BC_UPTO_DATE
			);

	SELECT @ErrorProcedure = s.name + '.' + o.name
	FROM SYS.OBJECTS O
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID = O.SCHEMA_ID
	WHERE OBJECT_ID = @@PROCID;

	IF OBJECT_ID('tempdb..#DeleteFolios') IS NOT NULL
		DROP TABLE #DeleteFolios;

	IF OBJECT_ID('tempdb..#DeleteSale') IS NOT NULL
		DROP TABLE #DeleteSale;

	IF OBJECT_ID('tempdb..#DeleteLegal') IS NOT NULL
		DROP TABLE #DeleteLegal;

	IF OBJECT_ID('tempdb..#DeleteAddress') IS NOT NULL
		DROP TABLE #DeleteAddress;

	

	BEGIN TRY
		BEGIN TRAN

-- Step 1. Delete new folios from BC_Upto_Date
		SELECT *
		INTO #DeleteFolios
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE FolioAction_FolioDelete = '1';

		--DELETE UPTD
		--FROM dbo.BC_UPTO_DATE UPTD
		--INNER JOIN #DeleteFolios Folio ON UPTD.FolioRecord_ID = Folio.FolioRecord_ID

		UPDATE UPTD
		SET AssessmentAreaDescription=NULL
		,JurisdictionCode=NULL
		,JurisdictionDescription=NULL
		,RollNumber=NULL
		,ActualUseDescription=NULL
		,VacantFlag=NULL
		,TenureDescription=NULL
		,FolioRecord_ID=NULL
		,FolioAddresses_FolioAddress_City=NULL
		,FolioAddresses_FolioAddress_ID=NULL
		,FolioAddresses_FolioAddress_PostalZip=NULL
		,FolioAddresses_FolioAddress_PrimaryFlag=NULL
		,FolioAddresses_FolioAddress_ProvinceState=NULL
		,FolioAddresses_FolioAddress_StreetDirectionSuffix=NULL
		,FolioAddresses_FolioAddress_StreetName=NULL
		,FolioAddresses_FolioAddress_StreetNumber=NULL
		,FolioAddresses_FolioAddress_StreetType=NULL
		,FolioAddresses_FolioAddress_UnitNumber=NULL
		,LandMeasurement_LandDepth=NULL
		,LandMeasurement_LandDimension=NULL
		,LandMeasurement_LandDimensionTypeDescription=NULL
		,LandMeasurement_LandWidth=NULL
		,FolioDescription_Neighbourhood_NeighbourhoodCode=NULL
		,FolioDescription_Neighbourhood_NeighbourhoodDescription=NULL
		,RegionalDistrict_DistrictDescription=NULL
		,SchoolDistrict_DistrictDescription=NULL
		,LegalDescriptions_LegalDescription_ID=NULL
		,LegalDescriptions_LegalDescription_Block=NULL
		,LegalDescriptions_LegalDescription_DistrictLot=NULL
		,LegalDescriptions_LegalDescription_ExceptPlan=NULL
		,LegalDescriptions_LegalDescription_FormattedLegalDescription=NULL
		,LegalDescriptions_LegalDescription_LandDistrict=NULL
		,LegalDescriptions_LegalDescription_LandDistrictDescription=NULL
		,LegalDescriptions_LegalDescription_LeaseLicenceNumber=NULL
		,LegalDescriptions_LegalDescription_LegalText=NULL
		,LegalDescriptions_LegalDescription_Lot=NULL
		,LegalDescriptions_LegalDescription_Meridian=NULL
		,LegalDescriptions_LegalDescription_MeridianShort=NULL
		,LegalDescriptions_LegalDescription_Parcel=NULL
		,LegalDescriptions_LegalDescription_Part1=NULL
		,LegalDescriptions_LegalDescription_Part2=NULL
		,LegalDescriptions_LegalDescription_Part3=NULL
		,LegalDescriptions_LegalDescription_Part4=NULL
		,LegalDescriptions_LegalDescription_PID=NULL
		,LegalDescriptions_LegalDescription_Plan=NULL
		,LegalDescriptions_LegalDescription_Portion=NULL
		,LegalDescriptions_LegalDescription_Range=NULL
		,LegalDescriptions_LegalDescription_Section=NULL
		,LegalDescriptions_LegalDescription_StrataLot=NULL
		,LegalDescriptions_LegalDescription_SubBlock=NULL
		,LegalDescriptions_LegalDescription_SubLot=NULL
		,LegalDescriptions_LegalDescription_LegalSubdivision=NULL
		,LegalDescriptions_LegalDescription_Township=NULL
		,Sales_Sale_ConveyanceDate=NULL
		,Sales_Sale_ConveyancePrice=NULL
		,Sales_Sale_ID=NULL
		,HashBytes=NULL
		,Code=NULL
		,LastModifiedDateUTC=@StartDate
		FROM dbo.BC_UPTO_DATE UPTD
		INNER JOIN #DeleteFolios Folio ON UPTD.FolioRecord_ID = Folio.FolioRecord_ID

		-- Step 2. Deleting existing sales data from BC_Upto_Date
		SELECT *
		INTO #DeleteSale
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE Sales_Sale_Action = 'Delete';

		UPDATE UPTD
		SET UPTD.Sales_Sale_ConveyanceDate = NULL
			,UPTD.Sales_Sale_ConveyancePrice = NULL
			,UPTD.Sales_Sale_ID = NULL
			,UPTD.LastModifiedDateUTC = @StartDate
		FROM dbo.BC_UPTO_DATE UPTD
		INNER JOIN #DeleteSale DelSale ON UPTD.FolioRecord_ID = DelSale.FolioRecord_ID
			AND UPTD.Sales_Sale_ID = DelSale.Sales_Sale_ID;

		-- Step 3. Deleting existing legal data from BC_Upto_Date
		SELECT *
		INTO #DeleteLegal
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE LegalDescriptions_LegalDescription_Action = 'Delete';

		UPDATE UPTD
		SET UPTD.LegalDescriptions_LegalDescription_Block = NULL
			,UPTD.LegalDescriptions_LegalDescription_DistrictLot = NULL
			,UPTD.LegalDescriptions_LegalDescription_ExceptPlan = NULL
			,UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription = NULL
			,UPTD.LegalDescriptions_LegalDescription_LandDistrict = NULL
			,UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription = NULL
			,UPTD.LegalDescriptions_LegalDescription_LeaseLicenceNumber = NULL
			,UPTD.LegalDescriptions_LegalDescription_LegalText = NULL
			,UPTD.LegalDescriptions_LegalDescription_Lot = NULL
			,UPTD.LegalDescriptions_LegalDescription_Meridian = NULL
			,UPTD.LegalDescriptions_LegalDescription_MeridianShort = NULL
			,UPTD.LegalDescriptions_LegalDescription_Parcel = NULL
			,UPTD.LegalDescriptions_LegalDescription_Part1 = NULL
			,UPTD.LegalDescriptions_LegalDescription_Part2 = NULL
			,UPTD.LegalDescriptions_LegalDescription_Part3 = NULL
			,UPTD.LegalDescriptions_LegalDescription_Part4 = NULL
			,UPTD.LegalDescriptions_LegalDescription_PID = NULL
			,UPTD.LegalDescriptions_LegalDescription_Plan = NULL
			,UPTD.LegalDescriptions_LegalDescription_Portion = NULL
			,UPTD.LegalDescriptions_LegalDescription_Range = NULL
			,UPTD.LegalDescriptions_LegalDescription_Section = NULL
			,UPTD.LegalDescriptions_LegalDescription_StrataLot = NULL
			,UPTD.LegalDescriptions_LegalDescription_SubBlock = NULL
			,UPTD.LegalDescriptions_LegalDescription_SubLot = NULL
			,UPTD.LegalDescriptions_LegalDescription_LegalSubdivision = NULL
			,UPTD.LegalDescriptions_LegalDescription_Township = NULL
			,UPTD.LegalDescriptions_LegalDescription_ID = NULL
			,UPTD.LastModifiedDateUTC = @StartDate
		FROM dbo.BC_UPTO_DATE UPTD
		INNER JOIN #DeleteLegal DelLeg ON UPTD.FolioRecord_ID = DelLeg.FolioRecord_ID
			AND UPTD.LegalDescriptions_LegalDescription_ID = DelLeg.LegalDescriptions_LegalDescription_ID;

		-- Step 4. Deleting existing Address data from BC_Upto_Date
		SELECT *
		INTO #DeleteAddress
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE FolioAddresses_FolioAddress_Action = 'Delete';

		UPDATE UPTD
		SET UPTD.FolioAddresses_FolioAddress_City = NULL
			,UPTD.FolioAddresses_FolioAddress_PostalZip = NULL
			,UPTD.FolioAddresses_FolioAddress_PrimaryFlag = NULL
			,UPTD.FolioAddresses_FolioAddress_ProvinceState = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetName = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetNumber = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetType = NULL
			,UPTD.FolioAddresses_FolioAddress_UnitNumber = NULL
			,UPTD.FolioAddresses_FolioAddress_ID = NULL
			,UPTD.LastModifiedDateUTC = @StartDate
		FROM dbo.BC_UPTO_DATE UPTD
		INNER JOIN #DeleteAddress DelAdd ON UPTD.FolioRecord_ID = DelAdd.FolioRecord_ID
			AND UPTD.FolioAddresses_FolioAddress_ID = DelAdd.FolioAddresses_FolioAddress_ID;

		--Step 5. Update Hashbytes from BC_Upto_Date
		UPDATE dbo.BC_UPTO_DATE
		SET HASHBYTES = HASHBYTES('SHA2_512', CONCAT_WS('|', FolioRecord_ID, RollYear, AssessmentAreaDescription, JurisdictionCode, JurisdictionDescription, RollNumber, ActualUseDescription, VacantFlag, TenureDescription, FolioAddresses_FolioAddress_City, FolioAddresses_FolioAddress_ID, FolioAddresses_FolioAddress_PostalZip, FolioAddresses_FolioAddress_PrimaryFlag, FolioAddresses_FolioAddress_ProvinceState, FolioAddresses_FolioAddress_StreetDirectionSuffix, FolioAddresses_FolioAddress_StreetName, FolioAddresses_FolioAddress_StreetNumber, FolioAddresses_FolioAddress_StreetType, FolioAddresses_FolioAddress_UnitNumber, LandMeasurement_LandDepth, LandMeasurement_LandDimension, LandMeasurement_LandDimensionTypeDescription, LandMeasurement_LandWidth, FolioDescription_Neighbourhood_NeighbourhoodCode, FolioDescription_Neighbourhood_NeighbourhoodDescription, RegionalDistrict_DistrictDescription, SchoolDistrict_DistrictDescription, LegalDescriptions_LegalDescription_Block, LegalDescriptions_LegalDescription_DistrictLot, 
					LegalDescriptions_LegalDescription_ExceptPlan, LegalDescriptions_LegalDescription_FormattedLegalDescription, LegalDescriptions_LegalDescription_ID, LegalDescriptions_LegalDescription_LandDistrict, LegalDescriptions_LegalDescription_LandDistrictDescription, LegalDescriptions_LegalDescription_LeaseLicenceNumber, LegalDescriptions_LegalDescription_LegalText, LegalDescriptions_LegalDescription_Lot, LegalDescriptions_LegalDescription_Meridian, LegalDescriptions_LegalDescription_MeridianShort, LegalDescriptions_LegalDescription_Parcel, LegalDescriptions_LegalDescription_Part1, LegalDescriptions_LegalDescription_Part2, LegalDescriptions_LegalDescription_Part3, LegalDescriptions_LegalDescription_Part4, LegalDescriptions_LegalDescription_PID, LegalDescriptions_LegalDescription_Plan, LegalDescriptions_LegalDescription_Portion, LegalDescriptions_LegalDescription_Range, LegalDescriptions_LegalDescription_Section, LegalDescriptions_LegalDescription_StrataLot, 
					LegalDescriptions_LegalDescription_SubBlock, LegalDescriptions_LegalDescription_SubLot, LegalDescriptions_LegalDescription_LegalSubdivision, LegalDescriptions_LegalDescription_Township, Sales_Sale_ConveyanceDate, Sales_Sale_ConveyancePrice, Sales_Sale_ID))
		WHERE LastModifiedDateUTC = @StartDate

		COMMIT TRAN;
	END TRY

	BEGIN CATCH
		ROLLBACK TRAN

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