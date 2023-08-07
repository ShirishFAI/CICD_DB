
CREATE PROCEDURE [ETLProcess].[BC_DeleteFolio_Through_Weekly_Test]
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

	IF OBJECT_ID('tempdb..#DTC_DeleteFolioInfo') IS NOT NULL
		DROP TABLE #DTC_DeleteFolioInfo;

	IF OBJECT_ID('tempdb..#DTC_DeleteInfo') IS NOT NULL
		DROP TABLE #DTC_DeleteInfo;

	IF OBJECT_ID('tempdb..#BC_UPTO_DATE') IS NOT NULL
		DROP TABLE #BC_UPTO_DATE;

	IF OBJECT_ID('tempdb..#FolioDelete') IS NOT NULL
		DROP TABLE #FolioDelete;

	IF OBJECT_ID('tempdb..#AdressDelete') IS NOT NULL
		DROP TABLE #AdressDelete;

	IF OBJECT_ID('tempdb..#LegalDelete') IS NOT NULL
		DROP TABLE #LegalDelete;

	IF OBJECT_ID('tempdb..#SaleDelete') IS NOT NULL
		DROP TABLE #SaleDelete;

	IF OBJECT_ID('tempdb..#DTC_ALLAddress') IS NOT NULL
		DROP TABLE #DTC_ALLAddress;

	IF OBJECT_ID('tempdb..#Final') IS NOT NULL
		DROP TABLE #Final;

	IF OBJECT_ID('tempdb..#Legal_DTCDeleteInfo') IS NOT NULL
		DROP TABLE #Legal_DTCDeleteInfo;

	IF OBJECT_ID('tempdb..#Sales_DTCDeleteInfo') IS NOT NULL
		DROP TABLE #Sales_DTCDeleteInfo;

	IF OBJECT_ID('tempdb..#Address_DTC_DeleteInfo') IS NOT NULL
		DROP TABLE #Address_DTC_DeleteInfo;

	IF OBJECT_ID('tempdb..#FolioDelete_BasicFileds') IS NOT NULL
		DROP TABLE #FolioDelete_BasicFileds;

	BEGIN TRY
		BEGIN TRAN

		--********************************************************************************************
		-- Get All UptoDate Records into temp table
		SELECT UPTD.*
		INTO #BC_UPTO_DATE
		FROM dbo.BC_UPTO_DATE_DeletionTest UPTD
		INNER JOIN StageLanding.BC_ALL_Assessment_Weekly Wk ON UPTD.FolioRecord_ID = WK.FolioRecord_ID;

		-- Get All FolioDelete Records into temp table
		SELECT DISTINCT WK.Foliorecord_ID
			,WK.JurisdictionCode
			,WK.RollNumber
			,WK.FolioAction_FolioDelete
		INTO #FolioDelete
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE WK.FolioAction_FolioDelete = '1';

		-- Get All Address deleted Records into temp table
		SELECT DISTINCT WK.Foliorecord_ID
			,WK.JurisdictionCode
			,WK.RollNumber
			,WK.FolioAddresses_FolioAddress_ID
			,WK.FolioAddresses_FolioAddress_Action
		INTO #AdressDelete
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE WK.FolioAddresses_FolioAddress_Action = 'Delete';

		-- Get All Legal deleted Records into temp table
		SELECT DISTINCT WK.Foliorecord_ID
			,WK.JurisdictionCode
			,WK.RollNumber
			,WK.LegalDescriptions_LegalDescription_ID
			,WK.LegalDescriptions_LegalDescription_Action
		INTO #LegalDelete
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE WK.LegalDescriptions_LegalDescription_Action = 'Delete';

		-- Get All Sales deleted Records into temp table
		SELECT DISTINCT WK.Foliorecord_ID
			,WK.JurisdictionCode
			,WK.RollNumber
			,WK.Sales_Sale_ID
			,WK.Sales_Sale_Action
		INTO #SaleDelete
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE WK.Sales_Sale_Action = 'Delete';

		--********************************************************************************************
		-- Get addresses to delete
		SELECT *
		INTO #DTC_ALLAddress
		FROM (
			SELECT DISTINCT UPTD.Foliorecord_ID
				,UPTD.JurisdictionCode
				,UPTD.RollNumber
				,UPTD.FolioAddresses_FolioAddress_ProvinceState
				,UPTD.FolioAddresses_FolioAddress_City
				,UPTD.FolioAddresses_FolioAddress_ID
				,UPTD.FolioAddresses_FolioAddress_PostalZip
				,UPTD.FolioAddresses_FolioAddress_PrimaryFlag
				,UPTD.FolioAddresses_FolioAddress_StreetName
				,UPTD.FolioAddresses_FolioAddress_StreetNumber
				,UPTD.FolioAddresses_FolioAddress_StreetType
				,UPTD.FolioAddresses_FolioAddress_UnitNumber
				,WK.FolioAction_FolioDelete
				,'Delete' AS FolioAddresses_FolioAddress_Action
				,UPTD.LegalDescriptions_LegalDescription_ID
				,'Delete' AS LegalDescriptions_LegalDescription_Action
				,'Delete' AS Sales_Sale_Action
				,UPTD.Sales_Sale_ID
			FROM #BC_UPTO_DATE UPTD
			LEFT JOIN #FolioDelete Wk ON UPTD.FolioRecord_ID = WK.FolioRecord_ID
			WHERE (WK.FolioAction_FolioDelete = 1)
			
			UNION ALL
			
			SELECT DISTINCT UPTD.Foliorecord_ID
				,UPTD.JurisdictionCode
				,UPTD.RollNumber
				,UPTD.FolioAddresses_FolioAddress_ProvinceState
				,UPTD.FolioAddresses_FolioAddress_City
				,UPTD.FolioAddresses_FolioAddress_ID
				,UPTD.FolioAddresses_FolioAddress_PostalZip
				,UPTD.FolioAddresses_FolioAddress_PrimaryFlag
				,UPTD.FolioAddresses_FolioAddress_StreetName
				,UPTD.FolioAddresses_FolioAddress_StreetNumber
				,UPTD.FolioAddresses_FolioAddress_StreetType
				,UPTD.FolioAddresses_FolioAddress_UnitNumber
				,'' AS FolioAction_FolioDelete
				,WK1.FolioAddresses_FolioAddress_Action
				,WK2.LegalDescriptions_LegalDescription_ID
				,WK2.LegalDescriptions_LegalDescription_Action
				,WK3.Sales_Sale_Action
				,WK3.Sales_Sale_ID
			FROM #BC_UPTO_DATE UPTD
			LEFT JOIN #AdressDelete Wk1 ON UPTD.FolioRecord_ID = WK1.FolioRecord_ID
				AND UPTD.FolioAddresses_FolioAddress_ID = WK1.FolioAddresses_FolioAddress_ID
			LEFT JOIN #LegalDelete Wk2 ON UPTD.FolioRecord_ID = WK2.FolioRecord_ID
				AND UPTD.LegalDescriptions_LegalDescription_ID = WK2.LegalDescriptions_LegalDescription_ID
			LEFT JOIN #SaleDelete Wk3 ON UPTD.FolioRecord_ID = WK3.FolioRecord_ID
				AND UPTD.Sales_Sale_ID = WK3.Sales_Sale_ID
			WHERE (
					WK1.FolioAddresses_FolioAddress_Action = 'Delete'
					OR WK2.LegalDescriptions_LegalDescription_Action = 'Delete'
					OR WK3.Sales_Sale_Action = 'Delete'
					)
			) DTC;

		-- Get Legal and Sales Key Fields
		SELECT DISTINCT Address.*
			,UPTD.LegalDescriptions_LegalDescription_PID
			,UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,UPTD.LegalDescriptions_LegalDescription_Plan
			,UPTD.LegalDescriptions_LegalDescription_LandDistrict
			,UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription
			,UPTD1.Sales_Sale_ConveyanceDate
			,UPTD1.Sales_Sale_ConveyancePrice
		INTO #Final
		FROM #DTC_ALLAddress Address
		LEFT JOIN #BC_UPTO_DATE UPTD ON Address.FolioRecord_ID = UPTD.FolioRecord_ID
			AND Address.LegalDescriptions_LegalDescription_ID = UPTD.LegalDescriptions_LegalDescription_ID
			AND Address.LegalDescriptions_LegalDescription_ID IS NOT NULL
		LEFT JOIN #BC_UPTO_DATE UPTD1 ON Address.FolioRecord_ID = UPTD1.FolioRecord_ID
			AND Address.Sales_Sale_ID = UPTD1.Sales_Sale_ID
			AND Address.Sales_Sale_ID IS NOT NULL;

		-- Get code and MasterAddressID for address records to be deleted
		SELECT DISTINCT Address.Code AS Code
			,Address.MasterAddressID AS MasterAddressID
			,Pr.JurCode
			,Pr.ARN
			,Pr.ProvinceCode
			,DTC_Delete.Foliorecord_ID
			,DTC_Delete.JurisdictionCode
			,DTC_Delete.RollNumber
			,DTC_Delete.FolioAddresses_FolioAddress_ProvinceState
			,DTC_Delete.FolioAddresses_FolioAddress_City
			,DTC_Delete.FolioAddresses_FolioAddress_ID
			,DTC_Delete.FolioAddresses_FolioAddress_PostalZip
			,DTC_Delete.FolioAddresses_FolioAddress_PrimaryFlag
			,DTC_Delete.FolioAddresses_FolioAddress_StreetName
			,DTC_Delete.FolioAddresses_FolioAddress_StreetNumber
			,DTC_Delete.FolioAddresses_FolioAddress_StreetType
			,DTC_Delete.FolioAddresses_FolioAddress_UnitNumber
			,DTC_Delete.FolioAction_FolioDelete
			,DTC_Delete.FolioAddresses_FolioAddress_Action
		INTO #Address_DTC_DeleteInfo
		FROM #Final DTC_Delete
		INNER JOIN DBO.Property_DeletionTest Pr ON Pr.JurCode = DTC_Delete.JurisdictionCode
			AND Pr.ARN = DTC_Delete.RollNumber
			AND Pr.ProvinceCode = 'BC'
		INNER JOIN dbo.Address_DeletionTest Address ON Address.Code = Pr.Code
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_City, 'City') = ISNULL(Address.City, 'City')
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_PostalZip, 'PostalZip') = ISNULL(Address.PostalCode, 'PostalZip')
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_ProvinceState, 'BC') = ISNULL(Address.ProvinceCode, 'BC')
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_StreetName, 'StreetName') = ISNULL(Address.StreetName, 'StreetName')
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_StreetNumber, 'StreetNumber') = ISNULL(Address.StreetNumber, 'StreetNumber')
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_StreetType, 'StreetType') = ISNULL(Address.StreetType, 'StreetType')
			AND ISNULL(DTC_Delete.FolioAddresses_FolioAddress_UnitNumber, 'UnitNumber') = ISNULL(Address.UnitNumber, 'UnitNumber')
			AND ISNULL(DTC_Delete.JurisdictionCode, 'JurCode') = ISNULL(Address.JurCode, 'JurCode');

		-- Get Legal data to be deleted
		SELECT DISTINCT DTC.Foliorecord_ID
			,DTC.JurisdictionCode
			,DTC.RollNumber
			,DTC.FolioAction_FolioDelete
			,DTC.LegalDescriptions_LegalDescription_ID
			,DTC.LegalDescriptions_LegalDescription_Action
			,DTC.LegalDescriptions_LegalDescription_PID
			,DTC.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,DTC.LegalDescriptions_LegalDescription_Plan
			,DTC.LegalDescriptions_LegalDescription_LandDistrict
			,DTC.LegalDescriptions_LegalDescription_LandDistrictDescription
			,PIN.Code AS Legal_Code
		INTO #Legal_DTCDeleteInfo
		FROM #Final DTC
		INNER JOIN DBO.Property_DeletionTest Pr ON Pr.JurCode = DTC.JurisdictionCode
			AND Pr.ARN = DTC.RollNumber
			AND Pr.ProvinceCode = 'BC'
		LEFT JOIN dbo.Parcel_DeletionTest Parcel ON Parcel.Code = Pr.Code
			AND ISNULL(Parcel.LegalDescription, '') = ISNULL(DTC.LegalDescriptions_LegalDescription_FormattedLegalDescription, '')
			AND ISNULL(Parcel.PlanNumber, '') = ISNULL(DTC.LegalDescriptions_LegalDescription_Plan, '')
		LEFT JOIN dbo.PIN_DeletionTest PIN ON PIN.Code = Pr.Code AND PIN.Code = Parcel.Code
			AND ISNULL(PIN.PIN, '') = ISNULL(DTC.LegalDescriptions_LegalDescription_PID, '')
		LEFT JOIN dbo.Address_DeletionTest Address ON Address.Code = Pr.Code AND Address.Code = Parcel.Code AND Address.Code = PIN.Code
			AND ISNULL(Address.LandDistrict, '') = ISNULL(DTC.LegalDescriptions_LegalDescription_LandDistrict, '')
			AND ISNULL(Address.LandDistrictName, '') = ISNULL(DTC.LegalDescriptions_LegalDescription_LandDistrictDescription, '')
			AND PIN.Data_Source_ID = @ProcessID;

		-- Get Sales data to be deleted
		SELECT DTC.Foliorecord_ID
			,DTC.JurisdictionCode
			,DTC.RollNumber
			,DTC.FolioAction_FolioDelete
			,DTC.Sales_Sale_Action
			,DTC.Sales_Sale_ID
			,DTC.Sales_Sale_ConveyanceDate
			,DTC.Sales_Sale_ConveyancePrice
			,Sales.Code AS Sales_Code
		INTO #Sales_DTCDeleteInfo
		FROM #Final DTC
		INNER JOIN DBO.Property_DeletionTest Pr ON Pr.JurCode = DTC.JurisdictionCode
			AND Pr.ARN = DTC.RollNumber
			AND Pr.ProvinceCode = 'BC'
		INNER JOIN dbo.Sales_DeletionTest Sales ON Sales.Code = Pr.Code
			AND ISNULL(Sales.ClosingDate, '01/01/1900') = ISNULL(DTC.Sales_Sale_ConveyanceDate, '01/01/1900')
			AND ISNULL(Sales.PriceSold, 0) = ISNULL(DTC.Sales_Sale_ConveyancePrice, 0)
			AND Sales.Data_Source_ID = @ProcessID;

		--**************************************************************************************************************************************
		-- Start Deletion in DTC
		-- Step 1. Delete Address from DTC 
		-- Updates Address fiels from address to NULL
		UPDATE Address
		SET UnitNumber = NULL
			,StreetNumber = NULL
			,StreetName = NULL
			,StreetType = NULL
			,StreetDirection = NULL
			,City = NULL
			,PostalCode = NULL
			,ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Address_DeletionTest Address
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Address.Code = DTC_Address.Code
			AND Address.MasterAddressID = DTC_Address.MasterAddressID
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tAddress NOCHECK CONSTRAINT ALL;
		--UPDATE Address
		--SET UnitNumber = NULL
		--	,StreetNumber = NULL
		--	,StreetName = NULL
		--	,StreetType = NULL
		--	,StreetDirection = NULL
		--	,City = NULL
		--	,PostalCode = NULL
		--	,ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tAddress Address
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Address.Code = DTC_Address.Code
		--	AND Address.MasterAddressID = DTC_Address.MasterAddressID
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tAddress CHECK CONSTRAINT ALL;
		-- Updates Address fiels from Building to NULL
		UPDATE Building
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Building_DeletionTest Building
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Building.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tBuilding NOCHECK CONSTRAINT ALL;
		--UPDATE Building
		--SET ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tBuilding Building
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Building.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tBuilding CHECK CONSTRAINT ALL;
		-- Updates Address fiels from Business to NULL
		UPDATE Business
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Business_DeletionTest Business
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Business.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--UPDATE Business
		--SET ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--FROM Profisee.data.tBusiness Business
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Business.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		-- Updates Address fiels from Listing to NULL
		UPDATE Listing
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Listing_DeletionTest Listing
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Listing.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--UPDATE Listing
		--SET ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--FROM Profisee.data.tListing Listing
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Listing.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		-- Updates Address fiels from Parcel to NULL
		UPDATE Parcel
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Parcel_DeletionTest Parcel
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Parcel.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tParcel NOCHECK CONSTRAINT ALL;
		--UPDATE Parcel
		--SET ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tParcel Parcel
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Parcel.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tParcel CHECK CONSTRAINT ALL;
		-- Updates Address fiels from Permit to NULL
		UPDATE Permit
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Permit_DeletionTest Permit
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Permit.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tPermit NOCHECK CONSTRAINT ALL;
		--UPDATE Permit
		--SET ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tPermit Permit
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Permit.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tPermit CHECK CONSTRAINT ALL;
		-- Updates Address fiels from PIN to NULL
		UPDATE PIN
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.PIN_DeletionTest PIN
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON PIN.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tPIN NOCHECK CONSTRAINT ALL;
		--UPDATE PIN
		--SET ProvinceCode = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tPIN PIN
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON PIN.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tPIN CHECK CONSTRAINT ALL;
		-- Updates Address fiels from Property to NULL
		UPDATE Property
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Property_DeletionTest Property
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Property.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		-- Updates Address fiels from Taxation to NULL
		UPDATE Taxation
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Taxation_DeletionTest Taxation
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Taxation.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tTaxation NOCHECK CONSTRAINT ALL;
		--UPDATE Taxation
		--SET ProvinceCode = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tTaxation Taxation
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Taxation.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tTaxation CHECK CONSTRAINT ALL;
		-- Updates Address fiels from Valuation to NULL
		UPDATE Valuation
		SET ProvinceCode = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = NULL
		FROM dbo.Valuation_DeletionTest Valuation
		INNER JOIN #Address_DTC_DeleteInfo DTC_Address ON Valuation.Code = DTC_Address.Code
		WHERE (
				DTC_Address.FolioAction_FolioDelete = '1'
				OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
				);

		--UPDATE Valuation
		--SET ProvinceCode = NULL
		--	,MasterAddressID = NULL
		--FROM profisee.data.tValuation Valuation
		--INNER JOIN #DTC_DeleteInfo DTC_Address ON Valuation.Code = DTC_Address.Code
		--WHERE (
		--		DTC_Address.FolioAction_FolioDelete = '1'
		--		OR DTC_Address.FolioAddresses_FolioAddress_Action = 'Delete'
		--		);
		----------------------------------------------------------------------
		--Step 2. Delete Legal from DTC
		-- Updates Legal fiels from address to NULL
		UPDATE Address
		SET Township = NULL
			,Range = NULL
			,LandDistrict = NULL
			,LandDistrictName = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Legal.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		--Select Parcel.*,PIN.*
		FROM dbo.Address_DeletionTest Address
		INNER JOIN #Legal_DTCDeleteInfo Legal ON Address.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tAddress NOCHECK CONSTRAINT ALL;
		--UPDATE Address
		--SET Township = NULL
		--	,Range = NULL
		--	,LandDistrict = NULL
		--	,LandDistrictName = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tAddress Address
		--INNER JOIN #DTC_DeleteInfo Legal ON Address.Code = Legal.Code
		--WHERE (
		--		Legal.FolioAction_FolioDelete = '1'
		--		OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tAddress CHECK CONSTRAINT ALL;
		-- Updates Legal fiels from Building to NULL
		UPDATE Building
		SET PIN = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Legal.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		--Select Parcel.*
		FROM dbo.Building_DeletionTest Building
		INNER JOIN #Legal_DTCDeleteInfo Legal ON Building.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tBuilding NOCHECK CONSTRAINT ALL;
		--UPDATE Building
		--SET PIN = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tBuilding Building
		--INNER JOIN #DTC_DeleteInfo Legal ON Building.Code = Legal.Code
		--WHERE (
		--		Legal.FolioAction_FolioDelete = '1'
		--		OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tBuilding CHECK CONSTRAINT ALL;
		-- Updates Legal fiels from Listing to NULL
		UPDATE Listing
		SET PIN = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Legal.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		FROM dbo.Listing_DeletionTest Listing
		INNER JOIN #Legal_DTCDeleteInfo Legal ON Listing.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		--UPDATE Listing
		--SET PIN = NULL
		--FROM Profisee.data.tListing Listing
		--INNER JOIN #DTC_DeleteInfo Legal ON Listing.Code = Legal.Code
		--WHERE (
		--		Legal.FolioAction_FolioDelete = '1'
		--		OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
		--		);
		-- Updates Legal fiels from Parcel to NULL
		UPDATE Parcel
		SET PIN = NULL
			,PlanNumber = NULL
			,LegalDescription = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Legal.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		FROM dbo.Parcel_DeletionTest Parcel
		INNER JOIN #Legal_DTCDeleteInfo Legal ON Parcel.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tParcel NOCHECK CONSTRAINT ALL;
		--UPDATE Parcel
		--SET PIN = NULL
		--	,PlanNumber = NULL
		--	,LegalDescription = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tParcel Parcel
		--INNER JOIN #DTC_DeleteInfo Legal ON Parcel.Code = Legal.Code
		--WHERE (
		--		Legal.FolioAction_FolioDelete = '1'
		--		OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tParcel CHECK CONSTRAINT ALL;
		-- Updates Legal fiels from PIN to NULL
		UPDATE PIN
		SET PIN = NULL
			,OriginalPIN = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.PIN_DeletionTest PIN
		INNER JOIN #Legal_DTCDeleteInfo Legal ON PIN.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		--ALTER TABLE Profisee.data.tPIN NOCHECK CONSTRAINT ALL;
		--UPDATE PIN
		--SET PIN = NULL
		--	,OriginalPIN = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tPIN PIN
		--INNER JOIN #DTC_DeleteInfo Legal ON PIN.Code = Legal.Code
		--WHERE (
		--		Legal.FolioAction_FolioDelete = '1'
		--		OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
		--		);
		--ALTER TABLE Profisee.data.tPIN CHECK CONSTRAINT ALL;
		-- Updates Legal fiels from Property to NULL
		UPDATE Property
		SET PIN = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Legal.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		FROM dbo.Property_DeletionTest Property
		INNER JOIN #Legal_DTCDeleteInfo Legal ON Property.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		-- Updates Legal fiels from Valuation to NULL
		UPDATE Valuation
		SET PIN = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Legal.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		FROM dbo.Valuation_DeletionTest Valuation
		INNER JOIN #Legal_DTCDeleteInfo Legal ON Valuation.Code = Legal.Legal_Code
		WHERE (
				Legal.FolioAction_FolioDelete = '1'
				OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
				);

		--UPDATE Valuation
		--SET PIN = NULL
		--FROM Profisee.data.tValuation Valuation
		--INNER JOIN #DTC_DeleteInfo Legal ON Valuation.Code = Legal.Code
		--WHERE (
		--		Legal.FolioAction_FolioDelete = '1'
		--		OR Legal.LegalDescriptions_LegalDescription_Action = 'Delete'
		--		);
		----------------------------------------------------------------------
		--Step 3. Delete Sales from DTC
		-- Updates Sales fiels from Listing to NULL
		UPDATE Listing
		SET DateEnd = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Sales.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		FROM dbo.Listing_DeletionTest Listing
		INNER JOIN #Sales_DTCDeleteInfo Sales ON Listing.Code = Sales.Sales_Code
		WHERE (
				Sales.FolioAction_FolioDelete = '1'
				OR Sales.Sales_Sale_Action = 'Delete'
				);

		--UPDATE Listing
		--SET DateEnd = NULL
		--FROM Profisee.data.tListing Listing
		--INNER JOIN #DTC_DeleteInfo Sales ON Listing.Code = Sales.Code
		--WHERE (
		--		Sales.FolioAction_FolioDelete = '1'
		--		OR Sales.Sales_Sale_Action = 'Delete'
		--		);
		-- Updates Sales fiels from Sales to NULL
		UPDATE DTC_Sale
		SET PriceSold = NULL
			,ClosingDate = NULL
			,LastModifiedDateUTC = @StartDate
			,MasterAddressID = CASE WHEN Sales.FolioAction_FolioDelete = '1' THEN NULL ELSE MasterAddressID END
		FROM dbo.Sales_DeletionTest DTC_Sale
		INNER JOIN #Sales_DTCDeleteInfo Sales ON DTC_Sale.Code = Sales.Sales_Code
		WHERE (
				Sales.FolioAction_FolioDelete = '1'
				OR Sales.Sales_Sale_Action = 'Delete'
				);

		--UPDATE DTC_Sale
		--SET PriceSold = NULL
		--	,ClosingDate = NULL
		--FROM Profisee.data.tSales DTC_Sale
		--INNER JOIN #DTC_DeleteInfo Sales ON DTC_Sale.Code = Sales.Code
		--WHERE (
		--		Sales.FolioAction_FolioDelete = '1'
		--		OR Sales.Sales_Sale_Action = 'Delete'
		--		);
		------------------------------------------------------------------------------------
		-- Step 4. Delete the entire property details
		SELECT *
		INTO #FolioDelete_BasicFileds
		FROM (
			SELECT Code
				,FolioAction_FolioDelete
			FROM #Address_DTC_DeleteInfo
			WHERE FolioAction_FolioDelete = 1
			
			UNION
			
			SELECT Legal_Code AS Code
				,FolioAction_FolioDelete
			FROM #Legal_DTCDeleteInfo
			WHERE FolioAction_FolioDelete = 1
			
			UNION
			
			SELECT Sales_Code AS Code
				,FolioAction_FolioDelete
			FROM #Sales_DTCDeleteInfo
			WHERE FolioAction_FolioDelete = 1
			) BasicFields
		WHERE BasicFields.Code IS NOT NULL;

		-- Updates Basic fiels from Address to NULL
		UPDATE DTC_Address
		SET AreaDescription = NULL
			,JurCode = NULL
			,JurDescription = NULL
			,Neighbourhood = NULL
			,NeighbourhoodDescription = NULL
			,Region = NULL
			,SchoolDistrictDescription = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Address_DeletionTest DTC_Address
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON DTC_Address.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		--ALTER TABLE Profisee.data.tAddress NOCHECK CONSTRAINT ALL;
		--UPDATE DTC_Address
		--SET AreaDescription = NULL
		--	,JurCode = NULL
		--	,JurDescription = NULL
		--	,Neighbourhood = NULL
		--	,NeighbourhoodDescription = NULL
		--	,Region = NULL
		--	,SchoolDistrictDescription = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tAddress DTC_Address
		--INNER JOIN #FolioDelete_BasicFileds DTC_Del ON DTC_Address.Code = DTC_Del.Code
		--WHERE (DTC_Del.FolioAction_FolioDelete = '1');
		--ALTER TABLE Profisee.data.tAddress CHECK CONSTRAINT ALL;
		-- Updates Basic fiels from Listing to NULL
		UPDATE Listing
		SET ARN = NULL
			,JurCode = NULL
			,OwnershipType = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Listing_DeletionTest Listing
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Listing.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		--UPDATE Listing
		--SET ARN = NULL
		--	,JurCode = NULL
		--	,OwnershipType = NULL
		--FROM Profisee.data.tListing Listing
		--INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Listing.Code = DTC_Del.Code
		--WHERE (DTC_Del.FolioAction_FolioDelete = '1');
		-- Updates Basic fiels from Parcel to NULL
		UPDATE Parcel
		SET Acreage = NULL
			,IsVacantLand = NULL
			,LotDepth = NULL
			,LotFrontage = NULL
			,LotMeasureUnit = NULL
			,PropertyUse = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Parcel_DeletionTest Parcel
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Parcel.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		--ALTER TABLE Profisee.data.tParcel NOCHECK CONSTRAINT ALL;
		--UPDATE Parcel
		--SET Acreage = NULL
		--	,IsVacantLand = NULL
		--	,LotDepth = NULL
		--	,LotFrontage = NULL
		--	,LotMeasureUnit = NULL
		--	,PropertyUse = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tParcel Parcel
		--INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Parcel.Code = DTC_Del.Code
		--WHERE (DTC_Del.FolioAction_FolioDelete = '1');
		--ALTER TABLE Profisee.data.tParcel CHECK CONSTRAINT ALL;
		-- Updates Basic fiels from Permit to NULL
		UPDATE Permit
		SET ARN = NULL
			,JurCode = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Permit_DeletionTest Permit
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Permit.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		--ALTER TABLE Profisee.data.tPermit NOCHECK CONSTRAINT ALL;
		--UPDATE Permit
		--SET ARN = NULL
		--	,JurCode = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tPermit Permit
		--INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Permit.Code = DTC_Del.Code
		--WHERE (DTC_Del.FolioAction_FolioDelete = '1');
		--ALTER TABLE Profisee.data.tPermit CHECK CONSTRAINT ALL;
		-- Updates Basic fiels from Property to NULL
		UPDATE Property
		SET ARN = NULL
			,JurCode = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Property_DeletionTest Property
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Property.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		-- Updates Basic fiels from Taxation to NULL
		UPDATE Taxation
		SET ARN = NULL
			,JurCode = NULL
			,AssessmentYear = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Taxation_DeletionTest Taxation
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Taxation.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		--ALTER TABLE Profisee.data.tTaxation NOCHECK CONSTRAINT ALL;
		--UPDATE Taxation
		--SET ARN = NULL
		--	,JurCode = NULL
		--	,AssessmentYear = NULL
		--	,[Match Group] = NULL
		--	,[Match Score] = NULL
		--	,[Match Status] = NULL
		--	,[Record Source] = NULL
		--	,[Match Member] = NULL
		--	,[Match Strategy] = NULL
		--	,[Match DateTime] = NULL
		--	,[Match User] = NULL
		--	,[Match MultiGroup] = NULL
		--	,[Master] = NULL
		--	,[Approved Count] = NULL
		--	,[Proposed Count] = NULL
		--FROM Profisee.data.tTaxation Taxation
		--INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Taxation.Code = DTC_Del.Code
		--WHERE (DTC_Del.FolioAction_FolioDelete = '1');
		--ALTER TABLE Profisee.data.tTaxation CHECK CONSTRAINT ALL;
		-- Updates Basic fiels from Valuation to NULL
		UPDATE Valuation
		SET ARN = NULL
			,JurCode = NULL
			,LastModifiedDateUTC = @StartDate
		FROM dbo.Valuation_DeletionTest Valuation
		INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Valuation.Code = DTC_Del.Code
		WHERE (DTC_Del.FolioAction_FolioDelete = '1');

		--UPDATE Valuation
		--SET ARN = NULL
		--	,JurCode = NULL
		--FROM Profisee.data.tValuation Valuation
		--INNER JOIN #FolioDelete_BasicFileds DTC_Del ON Valuation.Code = DTC_Del.Code
		--WHERE (DTC_Del.FolioAction_FolioDelete = '1');
		------------------------------------------------------------------------------------
		-- Step 1. Delete new folios from BC_Upto_Date
		SELECT *
		INTO #DeleteFolios
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE FolioAction_FolioDelete = '1';

		DELETE UPTD
		FROM dbo.BC_UPTO_DATE_DeletionTest UPTD
		INNER JOIN #DeleteFolios Folio ON UPTD.FolioRecord_ID = Folio.FolioRecord_ID

		-- Step 2. Deleting existing sales data from BC_Upto_Date
		SELECT *
		INTO #DeleteSale
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE Sales_Sale_Action = 'Delete';

		UPDATE UPTD
		SET UPTD.Sales_Sale_ID = NULL
			,UPTD.Sales_Sale_ConveyanceDate = NULL
			,UPTD.Sales_Sale_ConveyancePrice = NULL
			,UPTD.LastModifiedDateUTC = @StartDate
		FROM dbo.BC_UPTO_DATE_DeletionTest UPTD
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
			,UPTD.LegalDescriptions_LegalDescription_ID = NULL
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
			,UPTD.LastModifiedDateUTC = @StartDate
		FROM dbo.BC_UPTO_DATE_DeletionTest UPTD
		INNER JOIN #DeleteLegal DelLeg ON UPTD.FolioRecord_ID = DelLeg.FolioRecord_ID
			AND UPTD.LegalDescriptions_LegalDescription_ID = DelLeg.LegalDescriptions_LegalDescription_ID;

		-- Step 4. Deleting existing Address data from BC_Upto_Date
		SELECT *
		INTO #DeleteAddress
		FROM StageLanding.BC_ALL_Assessment_Weekly WK
		WHERE FolioAddresses_FolioAddress_Action = 'Delete';

		UPDATE UPTD
		SET UPTD.FolioAddresses_FolioAddress_City = NULL
			,UPTD.FolioAddresses_FolioAddress_ID = NULL
			,UPTD.FolioAddresses_FolioAddress_PostalZip = NULL
			,UPTD.FolioAddresses_FolioAddress_PrimaryFlag = NULL
			,UPTD.FolioAddresses_FolioAddress_ProvinceState = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetName = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetNumber = NULL
			,UPTD.FolioAddresses_FolioAddress_StreetType = NULL
			,UPTD.FolioAddresses_FolioAddress_UnitNumber = NULL
			,UPTD.LastModifiedDateUTC = @StartDate
		FROM dbo.BC_UPTO_DATE_DeletionTest UPTD
		INNER JOIN #DeleteAddress DelAdd ON UPTD.FolioRecord_ID = DelAdd.FolioRecord_ID
			AND UPTD.FolioAddresses_FolioAddress_ID = DelAdd.FolioAddresses_FolioAddress_ID;

		--Step 5. Update Hashbytes from BC_Upto_Date
		UPDATE dbo.BC_UPTO_DATE
		SET HASHBYTES = HASHBYTES('SHA2_512', CONCAT_WS('|', FolioRecord_ID, RollYear, AssessmentAreaDescription, JurisdictionCode, JurisdictionDescription, RollNumber, ActualUseDescription, VacantFlag, TenureDescription, FolioAddresses_FolioAddress_City, FolioAddresses_FolioAddress_ID, FolioAddresses_FolioAddress_PostalZip, FolioAddresses_FolioAddress_PrimaryFlag, FolioAddresses_FolioAddress_ProvinceState, FolioAddresses_FolioAddress_StreetDirectionSuffix, FolioAddresses_FolioAddress_StreetName, FolioAddresses_FolioAddress_StreetNumber, FolioAddresses_FolioAddress_StreetType, FolioAddresses_FolioAddress_UnitNumber, LandMeasurement_LandDepth, LandMeasurement_LandDimension, LandMeasurement_LandDimensionTypeDescription, LandMeasurement_LandWidth, FolioDescription_Neighbourhood_NeighbourhoodCode, UPPER(FolioDescription_Neighbourhood_NeighbourhoodDescription), RegionalDistrict_DistrictDescription, SchoolDistrict_DistrictDescription, LegalDescriptions_LegalDescription_Block, LegalDescriptions_LegalDescription_DistrictLot, 
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