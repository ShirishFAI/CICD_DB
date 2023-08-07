




CREATE PROCEDURE [ETLProcess].[BC_AddFolio_Through_Weekly_OldProc]
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
	DECLARE @LastModifiedDateUTC DATETIME;
	DECLARE @ProcessName VARCHAR(100) = 'BC AddFolio Through Weekly File' ; 
	DECLARE @ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';
	DECLARE @ErrorProcedure VARCHAR(100);        

	SET @StartDate = GETDATE();
	SET @LastModifiedDateUTC = (Select Max(LastModifiedDateUTC) from dbo.BC_UPTO_DATE);

	SELECT         
	  @ErrorProcedure= s.name+'.'+o.name         
	 FROM         
	  SYS.OBJECTS O            
	  INNER JOIN SYS.SCHEMAS S         
	  ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	 IF OBJECT_ID('tempdb..#Sales_Weekly') IS NOT NULL DROP TABLE #Sales_Weekly;
	 IF OBJECT_ID('tempdb..#Sales_UptoDate') IS NOT NULL DROP TABLE #Sales_UptoDate;
	 IF OBJECT_ID('tempdb..#Address_ChangedToPrimary') IS NOT NULL DROP TABLE #Address_ChangedToPrimary;
	 IF OBJECT_ID('tempdb..#Address_WithUpdatedPrimary') IS NOT NULL DROP TABLE #Address_WithUpdatedPrimary;
	 IF OBJECT_ID('tempdb..#NewLegal_Final') IS NOT NULL DROP TABLE #NewLegal_Final;
	 IF OBJECT_ID('tempdb..#NewAddress_Primary') IS NOT NULL DROP TABLE #NewAddress_Primary;
	 IF OBJECT_ID('tempdb..#NewAddress_Primary_Final') IS NOT NULL DROP TABLE #NewAddress_Primary_Final;
	 IF OBJECT_ID('tempdb..#NewAddress_NonPrimary') IS NOT NULL DROP TABLE #NewAddress_NonPrimary;
	 

	BEGIN TRY
		BEGIN TRAN
	-- Step 1. Add new folios
--**************************************************************************************************************************


DROP TABLE IF EXISTS #NewFolios;
DROP TABLE IF EXISTS #NewFolios_Transformed;
DROP TABLE IF EXISTS #NewFolios_Transformed_Final;
DROP TABLE IF EXISTS #Folio_ChildCount;
DROP TABLE IF EXISTS #NewFolios_Final;
DROP TABLE IF EXISTS #Sales;
DROP TABLE IF EXISTS #Address;
DROP TABLE IF EXISTS #Legal;
DROP TABLE IF EXISTS #Basic;
DROP TABLE IF EXISTS #Final_MSM;
DROP TABLE IF EXISTS #Final_SMM;
DROP TABLE IF EXISTS #Final_MMS;
DROP TABLE IF EXISTS #Final_MMM;

-- Insert New Folio into Temp table
SELECT FolioRecord_ID
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
	,ROW_NUMBER() OVER (
		PARTITION BY Foliorecord_ID ORDER BY Foliorecord_ID
		) AS RowNum
INTO #NewFolios
FROM StageLanding.BC_ALL_Assessment_Weekly WK
WHERE FolioAction_FolioAdd = '1'
AND ISNULL(FolioAction_FolioDelete,'') != '1';

-- Check the count of Address, Legal and Sales for added folio
SELECT FolioRecord_ID
	,count(DISTINCT FolioAddresses_FolioAddress_ID) Address_Id
	,count(DISTINCT LegalDescriptions_LegalDescription_ID) LegalDescription_ID
	,count(DISTINCT Sales_Sale_ID) Sale_ID
	,Cast(NULL AS VARCHAR(20)) AS Combo
	,Cast(NULL AS VARCHAR(20)) AS Total
INTO #Folio_ChildCount
FROM #NewFolios
GROUP BY FolioRecord_ID;

-- Update schenario on the basis of Address, Legal and Sales count for added folio
UPDATE #Folio_ChildCount
SET Combo = CASE 
		WHEN address_ID > 1
			THEN 'M'
		ELSE 'S'
		END + CASE 
		WHEN LegalDescription_ID > 1
			THEN 'M'
		ELSE 'S'
		END + CASE 
		WHEN Sale_ID > 1
			THEN 'M'
		ELSE 'S'
		END
	,Total = Cast(Address_Id AS INT) + Cast(LegalDescription_ID AS INT) + Cast(Sale_ID AS INT);

--Create Temp1 table for holding the NewFolio Data
SELECT *
INTO #NewFolios_Transformed
FROM #NewFolios
WHERE 1 = 2;

--Create Temp2 table for holding the NewFolio Data
SELECT *
INTO #NewFolios_Transformed_Final
FROM #NewFolios
WHERE 1 = 2;

--Create Final Temp table for holding the NewFolio Data
SELECT *
	,Cast('' AS [binary] (64)) AS Hashbytes
INTO #NewFolios_Final
FROM #NewFolios
WHERE 1 = 2;

INSERT INTO #NewFolios_Transformed (
	FolioRecord_ID
	,RollYear
	,AssessmentAreaDescription
	,JurisdictionCode
	,JurisdictionDescription
	,RollNumber
	,ActualUseDescription
	,VacantFlag
	,TenureDescription
	,LandMeasurement_LandDepth
	,LandMeasurement_LandDimension
	,LandMeasurement_LandDimensionTypeDescription
	,LandMeasurement_LandWidth
	,FolioDescription_Neighbourhood_NeighbourhoodCode
	,FolioDescription_Neighbourhood_NeighbourhoodDescription
	,RegionalDistrict_DistrictDescription
	,SchoolDistrict_DistrictDescription
	,RowNum
	)
SELECT NF.FolioRecord_ID
	,RollYear
	,AssessmentAreaDescription
	,JurisdictionCode
	,JurisdictionDescription
	,RollNumber
	,ActualUseDescription
	,VacantFlag
	,TenureDescription
	,LandMeasurement_LandDepth
	,LandMeasurement_LandDimension
	,LandMeasurement_LandDimensionTypeDescription
	,LandMeasurement_LandWidth
	,FolioDescription_Neighbourhood_NeighbourhoodCode
	,FolioDescription_Neighbourhood_NeighbourhoodDescription
	,RegionalDistrict_DistrictDescription
	,SchoolDistrict_DistrictDescription
	,RowNum
FROM #NewFolios NF
INNER JOIN #Folio_ChildCount FC ON NF.FolioRecord_ID = FC.FolioRecord_ID
WHERE replace(FC.Combo, 'S', '') = 'MM';

UPDATE NFF
SET NFF.FolioAddresses_FolioAddress_City = NF.FolioAddresses_FolioAddress_City
	,NFF.FolioAddresses_FolioAddress_ID = NF.FolioAddresses_FolioAddress_ID
	,NFF.FolioAddresses_FolioAddress_PostalZip = NF.FolioAddresses_FolioAddress_PostalZip
	,NFF.FolioAddresses_FolioAddress_PrimaryFlag = NF.FolioAddresses_FolioAddress_PrimaryFlag
	,NFF.FolioAddresses_FolioAddress_ProvinceState = NF.FolioAddresses_FolioAddress_ProvinceState
	,NFF.FolioAddresses_FolioAddress_StreetDirectionSuffix = NF.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,NFF.FolioAddresses_FolioAddress_StreetName = NF.FolioAddresses_FolioAddress_StreetName
	,NFF.FolioAddresses_FolioAddress_StreetNumber = NF.FolioAddresses_FolioAddress_StreetNumber
	,NFF.FolioAddresses_FolioAddress_StreetType = NF.FolioAddresses_FolioAddress_StreetType
	,NFF.FolioAddresses_FolioAddress_UnitNumber = NF.FolioAddresses_FolioAddress_UnitNumber
FROM #NewFolios_Transformed NFF
INNER JOIN #NewFolios NF ON NFF.FolioRecord_ID = NF.FolioRecord_ID
INNER JOIN #Folio_ChildCount FC ON NF.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo = 'SMM';

UPDATE NFF
SET NFF.LegalDescriptions_LegalDescription_Block = NF.LegalDescriptions_LegalDescription_Block
	,NFF.LegalDescriptions_LegalDescription_DistrictLot = NF.LegalDescriptions_LegalDescription_DistrictLot
	,NFF.LegalDescriptions_LegalDescription_ExceptPlan = NF.LegalDescriptions_LegalDescription_ExceptPlan
	,NFF.LegalDescriptions_LegalDescription_FormattedLegalDescription = NF.LegalDescriptions_LegalDescription_FormattedLegalDescription
	,NFF.LegalDescriptions_LegalDescription_ID = NF.LegalDescriptions_LegalDescription_ID
	,NFF.LegalDescriptions_LegalDescription_LandDistrict = NF.LegalDescriptions_LegalDescription_LandDistrict
	,NFF.LegalDescriptions_LegalDescription_LandDistrictDescription = NF.LegalDescriptions_LegalDescription_LandDistrictDescription
	,NFF.LegalDescriptions_LegalDescription_LeaseLicenceNumber = NF.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	,NFF.LegalDescriptions_LegalDescription_LegalText = NF.LegalDescriptions_LegalDescription_LegalText
	,NFF.LegalDescriptions_LegalDescription_Lot = NF.LegalDescriptions_LegalDescription_Lot
	,NFF.LegalDescriptions_LegalDescription_Meridian = NF.LegalDescriptions_LegalDescription_Meridian
	,NFF.LegalDescriptions_LegalDescription_MeridianShort = NF.LegalDescriptions_LegalDescription_MeridianShort
	,NFF.LegalDescriptions_LegalDescription_Parcel = NF.LegalDescriptions_LegalDescription_Parcel
	,NFF.LegalDescriptions_LegalDescription_Part1 = NF.LegalDescriptions_LegalDescription_Part1
	,NFF.LegalDescriptions_LegalDescription_Part2 = NF.LegalDescriptions_LegalDescription_Part2
	,NFF.LegalDescriptions_LegalDescription_Part3 = NF.LegalDescriptions_LegalDescription_Part3
	,NFF.LegalDescriptions_LegalDescription_Part4 = NF.LegalDescriptions_LegalDescription_Part4
	,NFF.LegalDescriptions_LegalDescription_PID = NF.LegalDescriptions_LegalDescription_PID
	,NFF.LegalDescriptions_LegalDescription_Plan = NF.LegalDescriptions_LegalDescription_Plan
	,NFF.LegalDescriptions_LegalDescription_Portion = NF.LegalDescriptions_LegalDescription_Portion
	,NFF.LegalDescriptions_LegalDescription_Range = NF.LegalDescriptions_LegalDescription_Range
	,NFF.LegalDescriptions_LegalDescription_Section = NF.LegalDescriptions_LegalDescription_Section
	,NFF.LegalDescriptions_LegalDescription_StrataLot = NF.LegalDescriptions_LegalDescription_StrataLot
	,NFF.LegalDescriptions_LegalDescription_SubBlock = NF.LegalDescriptions_LegalDescription_SubBlock
	,NFF.LegalDescriptions_LegalDescription_SubLot = NF.LegalDescriptions_LegalDescription_SubLot
	,NFF.LegalDescriptions_LegalDescription_LegalSubdivision = NF.LegalDescriptions_LegalDescription_LegalSubdivision
	,NFF.LegalDescriptions_LegalDescription_Township = NF.LegalDescriptions_LegalDescription_Township
FROM #NewFolios_Transformed NFF
INNER JOIN #NewFolios NF ON NFF.FolioRecord_ID = NF.FolioRecord_ID
INNER JOIN #Folio_ChildCount FC ON NF.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo = 'MSM';

UPDATE NFF
SET NFF.Sales_Sale_ConveyanceDate = NF.Sales_Sale_ConveyanceDate
	,NFF.Sales_Sale_ConveyancePrice = NF.Sales_Sale_ConveyancePrice
	,NFF.Sales_Sale_ID = NF.Sales_Sale_ID
FROM #NewFolios_Transformed NFF
INNER JOIN #NewFolios NF ON NFF.FolioRecord_ID = NF.FolioRecord_ID
INNER JOIN #Folio_ChildCount FC ON NF.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo = 'MMS';

--**********************************************************************************************
--Get Sales Data
SELECT DISTINCT FolioRecord_ID AS FolioRecord_ID_Sales
	,Sales_Sale_ConveyanceDate
	,Sales_Sale_ConveyancePrice
	,Sales_Sale_ID
INTO #Sales
FROM #NewFolios;

--Get Address Data
SELECT DISTINCT FolioRecord_ID AS FolioRecord_ID_Address
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
INTO #Address
FROM #NewFolios;

--Get Legal Data
SELECT DISTINCT FolioRecord_ID AS FolioRecord_ID_Legal
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
INTO #Legal
FROM #NewFolios;

--Get Basic Data
SELECT DISTINCT FolioRecord_ID AS FolioRecord_ID_Basic
	,RollYear
	,AssessmentAreaDescription
	,JurisdictionCode
	,JurisdictionDescription
	,RollNumber
	,ActualUseDescription
	,VacantFlag
	,TenureDescription
	,LandMeasurement_LandDepth
	,LandMeasurement_LandDimension
	,LandMeasurement_LandDimensionTypeDescription
	,LandMeasurement_LandWidth
	,FolioDescription_Neighbourhood_NeighbourhoodCode
	,FolioDescription_Neighbourhood_NeighbourhoodDescription
	,RegionalDistrict_DistrictDescription
	,SchoolDistrict_DistrictDescription
INTO #Basic
FROM #NewFolios;

--***********************************************************************************************************

-- Add Final MSM Data
SELECT DISTINCT A.FolioAddresses_FolioAddress_City
	,A.FolioAddresses_FolioAddress_ID
	,A.FolioAddresses_FolioAddress_PostalZip
	,A.FolioAddresses_FolioAddress_PrimaryFlag
	,A.FolioAddresses_FolioAddress_ProvinceState
	,A.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,A.FolioAddresses_FolioAddress_StreetName
	,A.FolioAddresses_FolioAddress_StreetNumber
	,A.FolioAddresses_FolioAddress_StreetType
	,A.FolioAddresses_FolioAddress_UnitNumber
	,S.Sales_Sale_ConveyanceDate
	,S.Sales_Sale_ConveyancePrice
	,S.Sales_Sale_ID
	,coalesce(A.FolioRecord_ID_Address, S.FolioRecord_ID_Sales) FolioRecord_ID
	,ROW_NUMBER() OVER (
		PARTITION BY coalesce(A.FolioRecord_ID_Address, S.FolioRecord_ID_Sales) ORDER BY coalesce(A.FolioRecord_ID_Address, S.FolioRecord_ID_Sales)
		) RowNum
INTO #Final_MSM
FROM #Address A
FULL OUTER JOIN #Sales s ON s.Sales_Sale_ID = A.FolioRecord_ID_Address
INNER JOIN #Folio_ChildCount FC ON FC.FolioRecord_ID = coalesce(A.FolioRecord_ID_Address, S.FolioRecord_ID_Sales)
WHERE fC.Combo = 'MSM';

INSERT INTO #NewFolios_Transformed_Final (
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
	)
SELECT DISTINCT Fn.FolioRecord_ID
	,RollYear
	,AssessmentAreaDescription
	,JurisdictionCode
	,JurisdictionDescription
	,RollNumber
	,ActualUseDescription
	,VacantFlag
	,TenureDescription
	,Fn.FolioAddresses_FolioAddress_City
	,Fn.FolioAddresses_FolioAddress_ID
	,Fn.FolioAddresses_FolioAddress_PostalZip
	,Fn.FolioAddresses_FolioAddress_PrimaryFlag
	,Fn.FolioAddresses_FolioAddress_ProvinceState
	,Fn.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,Fn.FolioAddresses_FolioAddress_StreetName
	,Fn.FolioAddresses_FolioAddress_StreetNumber
	,Fn.FolioAddresses_FolioAddress_StreetType
	,Fn.FolioAddresses_FolioAddress_UnitNumber
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
	,Fn.Sales_Sale_ConveyanceDate
	,Fn.Sales_Sale_ConveyancePrice
	,Fn.Sales_Sale_ID
FROM #NewFolios_Transformed F
INNER JOIN #Final_MSM Fn ON F.FolioRecord_ID = Fn.FolioRecord_ID
	AND Fn.RowNum = F.RowNum
INNER JOIN #Folio_ChildCount FC ON Fn.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo = 'MSM';

-- Add Final SMM Data
SELECT DISTINCT LegalDescriptions_LegalDescription_Block
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
	,S.Sales_Sale_ConveyanceDate
	,S.Sales_Sale_ConveyancePrice
	,S.Sales_Sale_ID
	,coalesce(L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales) FolioRecord_ID
	,ROW_NUMBER() OVER (
		PARTITION BY coalesce(L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales) ORDER BY coalesce(L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales)
		) RowNum
INTO #Final_SMM
FROM #Legal L
FULL OUTER JOIN #Sales s ON s.Sales_Sale_ID = L.FolioRecord_ID_Legal
INNER JOIN #Folio_ChildCount FC ON FC.FolioRecord_ID = coalesce(L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales)
WHERE fC.Combo = 'SMM';

INSERT INTO #NewFolios_Transformed_Final (
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
	)
SELECT DISTINCT Fn.FolioRecord_ID
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
	,Fn.LegalDescriptions_LegalDescription_Block
	,Fn.LegalDescriptions_LegalDescription_DistrictLot
	,Fn.LegalDescriptions_LegalDescription_ExceptPlan
	,Fn.LegalDescriptions_LegalDescription_FormattedLegalDescription
	,Fn.LegalDescriptions_LegalDescription_ID
	,Fn.LegalDescriptions_LegalDescription_LandDistrict
	,Fn.LegalDescriptions_LegalDescription_LandDistrictDescription
	,Fn.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	,Fn.LegalDescriptions_LegalDescription_LegalText
	,Fn.LegalDescriptions_LegalDescription_Lot
	,Fn.LegalDescriptions_LegalDescription_Meridian
	,Fn.LegalDescriptions_LegalDescription_MeridianShort
	,Fn.LegalDescriptions_LegalDescription_Parcel
	,Fn.LegalDescriptions_LegalDescription_Part1
	,Fn.LegalDescriptions_LegalDescription_Part2
	,Fn.LegalDescriptions_LegalDescription_Part3
	,Fn.LegalDescriptions_LegalDescription_Part4
	,Fn.LegalDescriptions_LegalDescription_PID
	,Fn.LegalDescriptions_LegalDescription_Plan
	,Fn.LegalDescriptions_LegalDescription_Portion
	,Fn.LegalDescriptions_LegalDescription_Range
	,Fn.LegalDescriptions_LegalDescription_Section
	,Fn.LegalDescriptions_LegalDescription_StrataLot
	,Fn.LegalDescriptions_LegalDescription_SubBlock
	,Fn.LegalDescriptions_LegalDescription_SubLot
	,Fn.LegalDescriptions_LegalDescription_LegalSubdivision
	,Fn.LegalDescriptions_LegalDescription_Township
	,Fn.Sales_Sale_ConveyanceDate
	,Fn.Sales_Sale_ConveyancePrice
	,Fn.Sales_Sale_ID
FROM #NewFolios_Transformed F
INNER JOIN #Final_SMM Fn ON F.FolioRecord_ID = Fn.FolioRecord_ID
	AND Fn.RowNum = F.RowNum
INNER JOIN #Folio_ChildCount FC ON Fn.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo = 'SMM';

-- Add Final MMS Data
SELECT DISTINCT A.FolioAddresses_FolioAddress_City
	,A.FolioAddresses_FolioAddress_ID
	,A.FolioAddresses_FolioAddress_PostalZip
	,A.FolioAddresses_FolioAddress_PrimaryFlag
	,A.FolioAddresses_FolioAddress_ProvinceState
	,A.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,A.FolioAddresses_FolioAddress_StreetName
	,A.FolioAddresses_FolioAddress_StreetNumber
	,A.FolioAddresses_FolioAddress_StreetType
	,A.FolioAddresses_FolioAddress_UnitNumber
	,L.LegalDescriptions_LegalDescription_Block
	,L.LegalDescriptions_LegalDescription_DistrictLot
	,L.LegalDescriptions_LegalDescription_ExceptPlan
	,L.LegalDescriptions_LegalDescription_FormattedLegalDescription
	,L.LegalDescriptions_LegalDescription_ID
	,L.LegalDescriptions_LegalDescription_LandDistrict
	,L.LegalDescriptions_LegalDescription_LandDistrictDescription
	,L.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	,L.LegalDescriptions_LegalDescription_LegalText
	,L.LegalDescriptions_LegalDescription_Lot
	,L.LegalDescriptions_LegalDescription_Meridian
	,L.LegalDescriptions_LegalDescription_MeridianShort
	,L.LegalDescriptions_LegalDescription_Parcel
	,L.LegalDescriptions_LegalDescription_Part1
	,L.LegalDescriptions_LegalDescription_Part2
	,L.LegalDescriptions_LegalDescription_Part3
	,L.LegalDescriptions_LegalDescription_Part4
	,L.LegalDescriptions_LegalDescription_PID
	,L.LegalDescriptions_LegalDescription_Plan
	,L.LegalDescriptions_LegalDescription_Portion
	,L.LegalDescriptions_LegalDescription_Range
	,L.LegalDescriptions_LegalDescription_Section
	,L.LegalDescriptions_LegalDescription_StrataLot
	,L.LegalDescriptions_LegalDescription_SubBlock
	,L.LegalDescriptions_LegalDescription_SubLot
	,L.LegalDescriptions_LegalDescription_LegalSubdivision
	,L.LegalDescriptions_LegalDescription_Township
	,coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal) FolioRecord_ID
	,ROW_NUMBER() OVER (
		PARTITION BY coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal) ORDER BY coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal)
		) RowNum
INTO #Final_MMS
FROM #Address A
FULL OUTER JOIN #Legal L ON 1 = 2
INNER JOIN #Folio_ChildCount FC ON FC.FolioRecord_ID = coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal)
WHERE fC.Combo = 'MMS';

INSERT INTO #NewFolios_Transformed_Final (
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
	)
SELECT DISTINCT Fn.FolioRecord_ID
	,RollYear
	,AssessmentAreaDescription
	,JurisdictionCode
	,JurisdictionDescription
	,RollNumber
	,ActualUseDescription
	,VacantFlag
	,TenureDescription
	,Fn.FolioAddresses_FolioAddress_City
	,Fn.FolioAddresses_FolioAddress_ID
	,Fn.FolioAddresses_FolioAddress_PostalZip
	,Fn.FolioAddresses_FolioAddress_PrimaryFlag
	,Fn.FolioAddresses_FolioAddress_ProvinceState
	,Fn.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,Fn.FolioAddresses_FolioAddress_StreetName
	,Fn.FolioAddresses_FolioAddress_StreetNumber
	,Fn.FolioAddresses_FolioAddress_StreetType
	,Fn.FolioAddresses_FolioAddress_UnitNumber
	,LandMeasurement_LandDepth
	,LandMeasurement_LandDimension
	,LandMeasurement_LandDimensionTypeDescription
	,LandMeasurement_LandWidth
	,FolioDescription_Neighbourhood_NeighbourhoodCode
	,FolioDescription_Neighbourhood_NeighbourhoodDescription
	,RegionalDistrict_DistrictDescription
	,SchoolDistrict_DistrictDescription
	,Fn.LegalDescriptions_LegalDescription_Block
	,Fn.LegalDescriptions_LegalDescription_DistrictLot
	,Fn.LegalDescriptions_LegalDescription_ExceptPlan
	,Fn.LegalDescriptions_LegalDescription_FormattedLegalDescription
	,Fn.LegalDescriptions_LegalDescription_ID
	,Fn.LegalDescriptions_LegalDescription_LandDistrict
	,Fn.LegalDescriptions_LegalDescription_LandDistrictDescription
	,Fn.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	,Fn.LegalDescriptions_LegalDescription_LegalText
	,Fn.LegalDescriptions_LegalDescription_Lot
	,Fn.LegalDescriptions_LegalDescription_Meridian
	,Fn.LegalDescriptions_LegalDescription_MeridianShort
	,Fn.LegalDescriptions_LegalDescription_Parcel
	,Fn.LegalDescriptions_LegalDescription_Part1
	,Fn.LegalDescriptions_LegalDescription_Part2
	,Fn.LegalDescriptions_LegalDescription_Part3
	,Fn.LegalDescriptions_LegalDescription_Part4
	,Fn.LegalDescriptions_LegalDescription_PID
	,Fn.LegalDescriptions_LegalDescription_Plan
	,Fn.LegalDescriptions_LegalDescription_Portion
	,Fn.LegalDescriptions_LegalDescription_Range
	,Fn.LegalDescriptions_LegalDescription_Section
	,Fn.LegalDescriptions_LegalDescription_StrataLot
	,Fn.LegalDescriptions_LegalDescription_SubBlock
	,Fn.LegalDescriptions_LegalDescription_SubLot
	,Fn.LegalDescriptions_LegalDescription_LegalSubdivision
	,Fn.LegalDescriptions_LegalDescription_Township
	,Sales_Sale_ConveyanceDate
	,Sales_Sale_ConveyancePrice
	,Sales_Sale_ID
FROM #NewFolios_Transformed F
INNER JOIN #Final_MMS Fn ON F.FolioRecord_ID = Fn.FolioRecord_ID
	AND Fn.RowNum = F.RowNum
INNER JOIN #Folio_ChildCount FC ON Fn.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo = 'MMS';

-- Add Final MMM Data
SELECT DISTINCT A.FolioAddresses_FolioAddress_City
	,A.FolioAddresses_FolioAddress_ID
	,A.FolioAddresses_FolioAddress_PostalZip
	,A.FolioAddresses_FolioAddress_PrimaryFlag
	,A.FolioAddresses_FolioAddress_ProvinceState
	,A.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,A.FolioAddresses_FolioAddress_StreetName
	,A.FolioAddresses_FolioAddress_StreetNumber
	,A.FolioAddresses_FolioAddress_StreetType
	,A.FolioAddresses_FolioAddress_UnitNumber
	,L.LegalDescriptions_LegalDescription_Block
	,L.LegalDescriptions_LegalDescription_DistrictLot
	,L.LegalDescriptions_LegalDescription_ExceptPlan
	,L.LegalDescriptions_LegalDescription_FormattedLegalDescription
	,L.LegalDescriptions_LegalDescription_ID
	,L.LegalDescriptions_LegalDescription_LandDistrict
	,L.LegalDescriptions_LegalDescription_LandDistrictDescription
	,L.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	,L.LegalDescriptions_LegalDescription_LegalText
	,L.LegalDescriptions_LegalDescription_Lot
	,L.LegalDescriptions_LegalDescription_Meridian
	,L.LegalDescriptions_LegalDescription_MeridianShort
	,L.LegalDescriptions_LegalDescription_Parcel
	,L.LegalDescriptions_LegalDescription_Part1
	,L.LegalDescriptions_LegalDescription_Part2
	,L.LegalDescriptions_LegalDescription_Part3
	,L.LegalDescriptions_LegalDescription_Part4
	,L.LegalDescriptions_LegalDescription_PID
	,L.LegalDescriptions_LegalDescription_Plan
	,L.LegalDescriptions_LegalDescription_Portion
	,L.LegalDescriptions_LegalDescription_Range
	,L.LegalDescriptions_LegalDescription_Section
	,L.LegalDescriptions_LegalDescription_StrataLot
	,L.LegalDescriptions_LegalDescription_SubBlock
	,L.LegalDescriptions_LegalDescription_SubLot
	,L.LegalDescriptions_LegalDescription_LegalSubdivision
	,L.LegalDescriptions_LegalDescription_Township
	,S.Sales_Sale_ConveyanceDate
	,S.Sales_Sale_ConveyancePrice
	,S.Sales_Sale_ID
	,coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales) FolioRecord_ID
	,ROW_NUMBER() OVER (
		PARTITION BY coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales) ORDER BY coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal)
		) RowNum
INTO #Final_MMM
FROM #Address A
FULL OUTER JOIN #Legal L ON 1 = 2
FULL OUTER JOIN #Sales S ON 1 = 2
INNER JOIN #Folio_ChildCount FC ON FC.FolioRecord_ID = coalesce(A.FolioRecord_ID_Address, L.FolioRecord_ID_Legal, S.FolioRecord_ID_Sales)
WHERE fC.Combo = 'MMM';

INSERT INTO #NewFolios_Final (
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
	,Hashbytes
	)
SELECT DISTINCT Fn.FolioRecord_ID
	,RollYear
	,AssessmentAreaDescription
	,JurisdictionCode
	,JurisdictionDescription
	,RollNumber
	,ActualUseDescription
	,VacantFlag
	,TenureDescription
	,Fn.FolioAddresses_FolioAddress_City
	,Fn.FolioAddresses_FolioAddress_ID
	,Fn.FolioAddresses_FolioAddress_PostalZip
	,Fn.FolioAddresses_FolioAddress_PrimaryFlag
	,Fn.FolioAddresses_FolioAddress_ProvinceState
	,Fn.FolioAddresses_FolioAddress_StreetDirectionSuffix
	,Fn.FolioAddresses_FolioAddress_StreetName
	,Fn.FolioAddresses_FolioAddress_StreetNumber
	,Fn.FolioAddresses_FolioAddress_StreetType
	,Fn.FolioAddresses_FolioAddress_UnitNumber
	,LandMeasurement_LandDepth
	,LandMeasurement_LandDimension
	,LandMeasurement_LandDimensionTypeDescription
	,LandMeasurement_LandWidth
	,FolioDescription_Neighbourhood_NeighbourhoodCode
	,FolioDescription_Neighbourhood_NeighbourhoodDescription
	,RegionalDistrict_DistrictDescription
	,SchoolDistrict_DistrictDescription
	,Fn.LegalDescriptions_LegalDescription_Block
	,Fn.LegalDescriptions_LegalDescription_DistrictLot
	,Fn.LegalDescriptions_LegalDescription_ExceptPlan
	,Fn.LegalDescriptions_LegalDescription_FormattedLegalDescription
	,Fn.LegalDescriptions_LegalDescription_ID
	,Fn.LegalDescriptions_LegalDescription_LandDistrict
	,Fn.LegalDescriptions_LegalDescription_LandDistrictDescription
	,Fn.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	,Fn.LegalDescriptions_LegalDescription_LegalText
	,Fn.LegalDescriptions_LegalDescription_Lot
	,Fn.LegalDescriptions_LegalDescription_Meridian
	,Fn.LegalDescriptions_LegalDescription_MeridianShort
	,Fn.LegalDescriptions_LegalDescription_Parcel
	,Fn.LegalDescriptions_LegalDescription_Part1
	,Fn.LegalDescriptions_LegalDescription_Part2
	,Fn.LegalDescriptions_LegalDescription_Part3
	,Fn.LegalDescriptions_LegalDescription_Part4
	,Fn.LegalDescriptions_LegalDescription_PID
	,Fn.LegalDescriptions_LegalDescription_Plan
	,Fn.LegalDescriptions_LegalDescription_Portion
	,Fn.LegalDescriptions_LegalDescription_Range
	,Fn.LegalDescriptions_LegalDescription_Section
	,Fn.LegalDescriptions_LegalDescription_StrataLot
	,Fn.LegalDescriptions_LegalDescription_SubBlock
	,Fn.LegalDescriptions_LegalDescription_SubLot
	,Fn.LegalDescriptions_LegalDescription_LegalSubdivision
	,Fn.LegalDescriptions_LegalDescription_Township
	,Fn.Sales_Sale_ConveyanceDate
	,Fn.Sales_Sale_ConveyancePrice
	,Fn.Sales_Sale_ID
	,Cast('' AS [binary] (64)) AS Hashbytes
FROM #NewFolios F
INNER JOIN #Folio_ChildCount FC ON F.FolioRecord_ID = FC.FolioRecord_ID
INNER JOIN #Final_MMM Fn ON F.FolioRecord_ID = Fn.FolioRecord_ID
	AND Fn.RowNum = F.RowNum
WHERE FC.Combo = 'MMM';

-- Insert Folios that has multiple Address/Legal/Sales
INSERT INTO #NewFolios_Final (
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
	,Hashbytes
	)
SELECT FolioRecord_ID
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
	,Cast('' AS [binary] (64)) AS Hashbytes
FROM #NewFolios_Transformed_Final;

-- Insert Folios that has single Address/Legal/Sales
INSERT INTO #NewFolios_Final (
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
	,Hashbytes
	)
SELECT NF.FolioRecord_ID
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
	,Cast('' AS [binary] (64)) AS Hashbytes
FROM #NewFolios NF
INNER JOIN #Folio_ChildCount FC ON NF.FolioRecord_ID = FC.FolioRecord_ID
WHERE FC.Combo IN (
		'SSS'
		,'SSM'
		,'SMS'
		,'MSS'
		);

-- Modify datatype of Hashbyte column to Binary
--ALTER TABLE #NewFolios_Final

--ALTER COLUMN Hashbytes [binary] (64);

-- Update Hashbyte column
UPDATE #NewFolios_Final
SET Hashbytes = HASHBYTES('SHA2_512', CONCAT_WS('|', FolioRecord_ID, RollYear, AssessmentAreaDescription, JurisdictionCode, JurisdictionDescription, RollNumber, ActualUseDescription, VacantFlag, TenureDescription, FolioAddresses_FolioAddress_City, FolioAddresses_FolioAddress_ID, FolioAddresses_FolioAddress_PostalZip, FolioAddresses_FolioAddress_PrimaryFlag, FolioAddresses_FolioAddress_ProvinceState, FolioAddresses_FolioAddress_StreetDirectionSuffix, FolioAddresses_FolioAddress_StreetName, FolioAddresses_FolioAddress_StreetNumber, FolioAddresses_FolioAddress_StreetType, FolioAddresses_FolioAddress_UnitNumber, LandMeasurement_LandDepth, LandMeasurement_LandDimension, LandMeasurement_LandDimensionTypeDescription, LandMeasurement_LandWidth, FolioDescription_Neighbourhood_NeighbourhoodCode, FolioDescription_Neighbourhood_NeighbourhoodDescription, RegionalDistrict_DistrictDescription, SchoolDistrict_DistrictDescription, LegalDescriptions_LegalDescription_Block, LegalDescriptions_LegalDescription_DistrictLot, 
			LegalDescriptions_LegalDescription_ExceptPlan, LegalDescriptions_LegalDescription_FormattedLegalDescription, LegalDescriptions_LegalDescription_ID, LegalDescriptions_LegalDescription_LandDistrict, LegalDescriptions_LegalDescription_LandDistrictDescription, LegalDescriptions_LegalDescription_LeaseLicenceNumber, LegalDescriptions_LegalDescription_LegalText, LegalDescriptions_LegalDescription_Lot, LegalDescriptions_LegalDescription_Meridian, LegalDescriptions_LegalDescription_MeridianShort, LegalDescriptions_LegalDescription_Parcel, LegalDescriptions_LegalDescription_Part1, LegalDescriptions_LegalDescription_Part2, LegalDescriptions_LegalDescription_Part3, LegalDescriptions_LegalDescription_Part4, LegalDescriptions_LegalDescription_PID, LegalDescriptions_LegalDescription_Plan, LegalDescriptions_LegalDescription_Portion, LegalDescriptions_LegalDescription_Range, LegalDescriptions_LegalDescription_Section, LegalDescriptions_LegalDescription_StrataLot, LegalDescriptions_LegalDescription_SubBlock, 
			LegalDescriptions_LegalDescription_SubLot, LegalDescriptions_LegalDescription_LegalSubdivision, LegalDescriptions_LegalDescription_Township, Sales_Sale_ConveyanceDate, Sales_Sale_ConveyancePrice, Sales_Sale_ID));

--**************************************************************************************************************************

--Drop column RowNum
ALTER TABLE #NewFolios_Final DROP COLUMN RowNum;

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
	FROM #NewFolios_Final NF
	WHERE NOT EXISTS (
			SELECT 1
			FROM dbo.BC_UPTO_DATE uptd
			WHERE uptd.HashBytes = NF.HashBytes
			);

	-- END of New Folio Addition

--*****************************************************************************************
	
	-- Start of new FolioAddress, Legal and Sales

	DECLARE @StartDate_ALS DATETIME;      
	SET @StartDate_ALS = GETDATE();
	
	--Step 2A. FolioAddresses_FolioAddress_PrimaryFlag of new address is true
	SELECT FolioRecord_ID
		,RollYear
		,RollNumber
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
	INTO #NewAddress_Primary
	FROM StageLanding.BC_ALL_Assessment_Weekly WK
	WHERE FolioAddresses_FolioAddress_Action = 'Add'
		AND FolioAddresses_FolioAddress_PrimaryFlag = 'True';

	SELECT DISTINCT UPTD.FolioRecord_ID
		,WK.RollYear
		,UPTD.AssessmentAreaDescription
		,UPTD.JurisdictionCode
		,UPTD.JurisdictionDescription
		,UPTD.RollNumber
		,UPTD.ActualUseDescription
		,UPTD.VacantFlag
		,UPTD.TenureDescription
		,WK.FolioAddresses_FolioAddress_City
		,WK.FolioAddresses_FolioAddress_ID
		,WK.FolioAddresses_FolioAddress_PostalZip
		,WK.FolioAddresses_FolioAddress_PrimaryFlag
		,WK.FolioAddresses_FolioAddress_ProvinceState
		,WK.FolioAddresses_FolioAddress_StreetDirectionSuffix
		,WK.FolioAddresses_FolioAddress_StreetName
		,WK.FolioAddresses_FolioAddress_StreetNumber
		,WK.FolioAddresses_FolioAddress_StreetType
		,WK.FolioAddresses_FolioAddress_UnitNumber
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
		,UPTD.Sales_Sale_ConveyanceDate
		,UPTD.Sales_Sale_ConveyancePrice
		,UPTD.Sales_Sale_ID
		,HASHBYTES('SHA2_512', CONCAT_WS('|', UPTD.FolioRecord_ID, WK.RollYear, UPTD.AssessmentAreaDescription, UPTD.JurisdictionCode, UPTD.JurisdictionDescription, UPTD.RollNumber, UPTD.ActualUseDescription, UPTD.VacantFlag, UPTD.TenureDescription, WK.FolioAddresses_FolioAddress_City, WK.FolioAddresses_FolioAddress_ID, WK.FolioAddresses_FolioAddress_PostalZip, WK.FolioAddresses_FolioAddress_PrimaryFlag, WK.FolioAddresses_FolioAddress_ProvinceState, WK.FolioAddresses_FolioAddress_StreetDirectionSuffix, WK.FolioAddresses_FolioAddress_StreetName, WK.FolioAddresses_FolioAddress_StreetNumber, WK.FolioAddresses_FolioAddress_StreetType, WK.FolioAddresses_FolioAddress_UnitNumber, UPTD.LandMeasurement_LandDepth, UPTD.LandMeasurement_LandDimension, UPTD.LandMeasurement_LandDimensionTypeDescription, UPTD.LandMeasurement_LandWidth, UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode, UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription, UPTD.RegionalDistrict_DistrictDescription, UPTD.
				SchoolDistrict_DistrictDescription, UPTD.LegalDescriptions_LegalDescription_Block, UPTD.LegalDescriptions_LegalDescription_DistrictLot, UPTD.LegalDescriptions_LegalDescription_ExceptPlan, UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription, UPTD.LegalDescriptions_LegalDescription_ID, UPTD.LegalDescriptions_LegalDescription_LandDistrict, UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription, UPTD.LegalDescriptions_LegalDescription_LeaseLicenceNumber, UPTD.LegalDescriptions_LegalDescription_LegalText, UPTD.LegalDescriptions_LegalDescription_Lot, UPTD.LegalDescriptions_LegalDescription_Meridian, UPTD.LegalDescriptions_LegalDescription_MeridianShort, UPTD.LegalDescriptions_LegalDescription_Parcel, UPTD.LegalDescriptions_LegalDescription_Part1, UPTD.LegalDescriptions_LegalDescription_Part2, UPTD.LegalDescriptions_LegalDescription_Part3, UPTD.LegalDescriptions_LegalDescription_Part4, UPTD.LegalDescriptions_LegalDescription_PID, UPTD.
				LegalDescriptions_LegalDescription_Plan, UPTD.LegalDescriptions_LegalDescription_Portion, UPTD.LegalDescriptions_LegalDescription_Range, UPTD.LegalDescriptions_LegalDescription_Section, UPTD.LegalDescriptions_LegalDescription_StrataLot, UPTD.LegalDescriptions_LegalDescription_SubBlock, UPTD.LegalDescriptions_LegalDescription_SubLot, UPTD.LegalDescriptions_LegalDescription_LegalSubdivision, UPTD.LegalDescriptions_LegalDescription_Township, UPTD.Sales_Sale_ConveyanceDate, UPTD.Sales_Sale_ConveyancePrice, UPTD.Sales_Sale_ID)) AS HashBytes
	INTO #NewAddress_Primary_Final
	FROM dbo.BC_UPTO_DATE UPTD
	INNER JOIN #NewAddress_Primary WK ON UPTD.FolioRecord_ID = WK.FolioRecord_ID;

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
		,@StartDate_ALS AS DateCreatedUTC
		,@StartDate_ALS AS LastModifiedDateUTC
	FROM #NewAddress_Primary_Final NF
	WHERE NOT EXISTS (
			SELECT 1
			FROM dbo.BC_UPTO_DATE uptd
			WHERE uptd.HashBytes = NF.HashBytes
			);

	--Step 2B. FolioAddresses_FolioAddress_PrimaryFlag of new address is false

	SELECT DISTINCT WK.FolioRecord_ID
		,WK.RollYear
		,UPTD.AssessmentAreaDescription
		,UPTD.JurisdictionCode
		,UPTD.JurisdictionDescription
		,UPTD.RollNumber
		,UPTD.ActualUseDescription
		,UPTD.VacantFlag
		,UPTD.TenureDescription
		,WK.FolioAddresses_FolioAddress_City
		,WK.FolioAddresses_FolioAddress_ID
		,WK.FolioAddresses_FolioAddress_PostalZip
		,WK.FolioAddresses_FolioAddress_PrimaryFlag
		,WK.FolioAddresses_FolioAddress_ProvinceState
		,WK.FolioAddresses_FolioAddress_StreetDirectionSuffix
		,WK.FolioAddresses_FolioAddress_StreetName
		,WK.FolioAddresses_FolioAddress_StreetNumber
		,WK.FolioAddresses_FolioAddress_StreetType
		,WK.FolioAddresses_FolioAddress_UnitNumber
		,UPTD.LandMeasurement_LandDepth
		,UPTD.LandMeasurement_LandDimension
		,UPTD.LandMeasurement_LandDimensionTypeDescription
		,UPTD.LandMeasurement_LandWidth
		,UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
		,UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
		,UPTD.RegionalDistrict_DistrictDescription
		,UPTD.SchoolDistrict_DistrictDescription
		,WK.LegalDescriptions_LegalDescription_Block
		,WK.LegalDescriptions_LegalDescription_DistrictLot
		,WK.LegalDescriptions_LegalDescription_ExceptPlan
		,WK.LegalDescriptions_LegalDescription_FormattedLegalDescription
		,WK.LegalDescriptions_LegalDescription_ID
		,WK.LegalDescriptions_LegalDescription_LandDistrict
		,WK.LegalDescriptions_LegalDescription_LandDistrictDescription
		,WK.LegalDescriptions_LegalDescription_LeaseLicenceNumber
		,WK.LegalDescriptions_LegalDescription_LegalText
		,WK.LegalDescriptions_LegalDescription_Lot
		,WK.LegalDescriptions_LegalDescription_Meridian
		,WK.LegalDescriptions_LegalDescription_MeridianShort
		,WK.LegalDescriptions_LegalDescription_Parcel
		,WK.LegalDescriptions_LegalDescription_Part1
		,WK.LegalDescriptions_LegalDescription_Part2
		,WK.LegalDescriptions_LegalDescription_Part3
		,WK.LegalDescriptions_LegalDescription_Part4
		,WK.LegalDescriptions_LegalDescription_PID
		,WK.LegalDescriptions_LegalDescription_Plan
		,WK.LegalDescriptions_LegalDescription_Portion
		,WK.LegalDescriptions_LegalDescription_Range
		,WK.LegalDescriptions_LegalDescription_Section
		,WK.LegalDescriptions_LegalDescription_StrataLot
		,WK.LegalDescriptions_LegalDescription_SubBlock
		,WK.LegalDescriptions_LegalDescription_SubLot
		,WK.LegalDescriptions_LegalDescription_LegalSubdivision
		,WK.LegalDescriptions_LegalDescription_Township
		,WK.Sales_Sale_ConveyanceDate
		,WK.Sales_Sale_ConveyancePrice
		,WK.Sales_Sale_ID
		,HASHBYTES('SHA2_512', CONCAT_WS('|', WK.FolioRecord_ID,WK.RollYear,UPTD.AssessmentAreaDescription,UPTD.JurisdictionCode,UPTD.JurisdictionDescription,UPTD.RollNumber,UPTD.ActualUseDescription,UPTD.VacantFlag,UPTD.TenureDescription,WK.FolioAddresses_FolioAddress_City,WK.FolioAddresses_FolioAddress_ID,WK.FolioAddresses_FolioAddress_PostalZip,WK.FolioAddresses_FolioAddress_PrimaryFlag,WK.FolioAddresses_FolioAddress_ProvinceState,WK.FolioAddresses_FolioAddress_StreetDirectionSuffix,WK.FolioAddresses_FolioAddress_StreetName,WK.FolioAddresses_FolioAddress_StreetNumber,WK.FolioAddresses_FolioAddress_StreetType,WK.FolioAddresses_FolioAddress_UnitNumber,UPTD.LandMeasurement_LandDepth,UPTD.LandMeasurement_LandDimension,UPTD.LandMeasurement_LandDimensionTypeDescription,UPTD.LandMeasurement_LandWidth,UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode,UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription,UPTD.RegionalDistrict_DistrictDescription,UPTD.SchoolDistrict_DistrictDescription,WK.LegalDescriptions_LegalDescription_Block,WK.LegalDescriptions_LegalDescription_DistrictLot,WK.LegalDescriptions_LegalDescription_ExceptPlan,WK.LegalDescriptions_LegalDescription_FormattedLegalDescription,WK.LegalDescriptions_LegalDescription_ID,WK.LegalDescriptions_LegalDescription_LandDistrict,WK.LegalDescriptions_LegalDescription_LandDistrictDescription,WK.LegalDescriptions_LegalDescription_LeaseLicenceNumber,WK.LegalDescriptions_LegalDescription_LegalText,WK.LegalDescriptions_LegalDescription_Lot,WK.LegalDescriptions_LegalDescription_Meridian,WK.LegalDescriptions_LegalDescription_MeridianShort,WK.LegalDescriptions_LegalDescription_Parcel,WK.LegalDescriptions_LegalDescription_Part1,WK.LegalDescriptions_LegalDescription_Part2,WK.LegalDescriptions_LegalDescription_Part3,WK.LegalDescriptions_LegalDescription_Part4,WK.LegalDescriptions_LegalDescription_PID,WK.LegalDescriptions_LegalDescription_Plan,WK.LegalDescriptions_LegalDescription_Portion,WK.LegalDescriptions_LegalDescription_Range,WK.LegalDescriptions_LegalDescription_Section,WK.LegalDescriptions_LegalDescription_StrataLot,WK.LegalDescriptions_LegalDescription_SubBlock,WK.LegalDescriptions_LegalDescription_SubLot,WK.LegalDescriptions_LegalDescription_LegalSubdivision,WK.LegalDescriptions_LegalDescription_Township,WK.Sales_Sale_ConveyanceDate,WK.Sales_Sale_ConveyancePrice,WK.Sales_Sale_ID)) AS HashBytes
	INTO #NewAddress_NonPrimary
	FROM StageLanding.BC_ALL_Assessment_Weekly WK
	INNER JOIN dbo.BC_UPTO_DATE UPTD ON WK.FolioRecord_ID = UPTD.FolioRecord_ID 
	AND FolioAddresses_FolioAddress_Action = 'Add'
	AND WK.FolioAddresses_FolioAddress_PrimaryFlag = 'False';
		
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
		,@StartDate_ALS AS DateCreatedUTC
		,@StartDate_ALS AS LastModifiedDateUTC
	FROM #NewAddress_NonPrimary NF
	WHERE NOT EXISTS (
			SELECT 1
			FROM dbo.BC_UPTO_DATE uptd
			WHERE uptd.HashBytes = NF.HashBytes
			);
			
--Step 3. Address with updated primary flag
	SELECT FolioRecord_ID
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
	INTO #Address_ChangedToPrimary
	FROM StageLanding.BC_ALL_Assessment_Weekly WK
	WHERE FolioAddresses_FolioAddress_Action = 'Change'
		AND FolioAddresses_FolioAddress_PrimaryFlag_Action = 'Change'
		AND FolioAddresses_FolioAddress_PrimaryFlag = 'True'
		AND FolioAddresses_FolioAddress_PrimaryFlag_OldValue = 'False';

	SELECT DISTINCT UPTD.FolioRecord_ID
		,WK.RollYear
		,UPTD.AssessmentAreaDescription
		,UPTD.JurisdictionCode
		,UPTD.JurisdictionDescription
		,UPTD.RollNumber
		,UPTD.ActualUseDescription
		,UPTD.VacantFlag
		,UPTD.TenureDescription
		,WK.FolioAddresses_FolioAddress_City
		,WK.FolioAddresses_FolioAddress_ID
		,WK.FolioAddresses_FolioAddress_PostalZip
		,WK.FolioAddresses_FolioAddress_PrimaryFlag
		,WK.FolioAddresses_FolioAddress_ProvinceState
		,WK.FolioAddresses_FolioAddress_StreetDirectionSuffix
		,WK.FolioAddresses_FolioAddress_StreetName
		,WK.FolioAddresses_FolioAddress_StreetNumber
		,WK.FolioAddresses_FolioAddress_StreetType
		,WK.FolioAddresses_FolioAddress_UnitNumber
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
		,UPTD.Sales_Sale_ConveyanceDate
		,UPTD.Sales_Sale_ConveyancePrice
		,UPTD.Sales_Sale_ID
		,HASHBYTES('SHA2_512', CONCAT_WS('|', UPTD.FolioRecord_ID, WK.RollYear, UPTD.AssessmentAreaDescription, UPTD.JurisdictionCode, UPTD.JurisdictionDescription, UPTD.RollNumber, UPTD.ActualUseDescription, UPTD.VacantFlag, UPTD.TenureDescription, WK.FolioAddresses_FolioAddress_City, WK.FolioAddresses_FolioAddress_ID, WK.FolioAddresses_FolioAddress_PostalZip, WK.FolioAddresses_FolioAddress_PrimaryFlag, WK.FolioAddresses_FolioAddress_ProvinceState, WK.FolioAddresses_FolioAddress_StreetDirectionSuffix, WK.FolioAddresses_FolioAddress_StreetName, WK.FolioAddresses_FolioAddress_StreetNumber, WK.FolioAddresses_FolioAddress_StreetType, WK.FolioAddresses_FolioAddress_UnitNumber, UPTD.LandMeasurement_LandDepth, UPTD.LandMeasurement_LandDimension, UPTD.LandMeasurement_LandDimensionTypeDescription, UPTD.LandMeasurement_LandWidth, UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode, UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription, UPTD.RegionalDistrict_DistrictDescription, UPTD.
				SchoolDistrict_DistrictDescription, UPTD.LegalDescriptions_LegalDescription_Block, UPTD.LegalDescriptions_LegalDescription_DistrictLot, UPTD.LegalDescriptions_LegalDescription_ExceptPlan, UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription, UPTD.LegalDescriptions_LegalDescription_ID, UPTD.LegalDescriptions_LegalDescription_LandDistrict, UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription, UPTD.LegalDescriptions_LegalDescription_LeaseLicenceNumber, UPTD.LegalDescriptions_LegalDescription_LegalText, UPTD.LegalDescriptions_LegalDescription_Lot, UPTD.LegalDescriptions_LegalDescription_Meridian, UPTD.LegalDescriptions_LegalDescription_MeridianShort, UPTD.LegalDescriptions_LegalDescription_Parcel, UPTD.LegalDescriptions_LegalDescription_Part1, UPTD.LegalDescriptions_LegalDescription_Part2, UPTD.LegalDescriptions_LegalDescription_Part3, UPTD.LegalDescriptions_LegalDescription_Part4, UPTD.LegalDescriptions_LegalDescription_PID, UPTD.
				LegalDescriptions_LegalDescription_Plan, UPTD.LegalDescriptions_LegalDescription_Portion, UPTD.LegalDescriptions_LegalDescription_Range, UPTD.LegalDescriptions_LegalDescription_Section, UPTD.LegalDescriptions_LegalDescription_StrataLot, UPTD.LegalDescriptions_LegalDescription_SubBlock, UPTD.LegalDescriptions_LegalDescription_SubLot, UPTD.LegalDescriptions_LegalDescription_LegalSubdivision, UPTD.LegalDescriptions_LegalDescription_Township, UPTD.Sales_Sale_ConveyanceDate, UPTD.Sales_Sale_ConveyancePrice, UPTD.Sales_Sale_ID)) AS HashBytes
	INTO #Address_WithUpdatedPrimary
	FROM dbo.BC_UPTO_DATE UPTD
	INNER JOIN #Address_ChangedToPrimary WK ON UPTD.FolioRecord_ID = WK.FolioRecord_ID
	AND WK.FolioAddresses_FolioAddress_ID = UPTD.FolioAddresses_FolioAddress_ID
	AND WK.RollYear = UPTD.RollYear-1;

	INSERT INTO dbo.BC_UPTO_DATE(
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
		,@StartDate_ALS AS DateCreatedUTC
		,@StartDate_ALS AS LastModifiedDateUTC
	FROM #Address_WithUpdatedPrimary NF
	WHERE NOT EXISTS (
			SELECT 1
			FROM dbo.BC_UPTO_DATE uptd
			WHERE uptd.HashBytes = NF.HashBytes
			);

--*******************************************************************************************************************

-- Step 4. Add New Sales data
	SELECT FolioRecord_ID
		,RollYear
		,RollNumber
		,FolioAddresses_FolioAddress_Action
		,FolioAddresses_FolioAddress_ID
		,LegalDescriptions_LegalDescription_Action
		,LegalDescriptions_LegalDescription_ID
		,Sales_Sale_ConveyanceDate
		,Sales_Sale_ConveyancePrice
		,Sales_Sale_ID
		,Sales_Sale_Action
		,NULL AS IsPrimaryAddressesPresent
	INTO #Sales_Weekly
	FROM StageLanding.BC_ALL_Assessment_Weekly WK
	WHERE Sales_Sale_Action = 'Add';
	
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
		,HASHBYTES('SHA2_512', CONCAT_WS('|', UPTD.FolioRecord_ID, WK.RollYear, UPTD.AssessmentAreaDescription, UPTD.JurisdictionCode, UPTD.JurisdictionDescription, UPTD.RollNumber, UPTD.ActualUseDescription, UPTD.VacantFlag, UPTD.TenureDescription, UPTD.FolioAddresses_FolioAddress_City, UPTD.FolioAddresses_FolioAddress_ID, UPTD.FolioAddresses_FolioAddress_PostalZip, UPTD.FolioAddresses_FolioAddress_PrimaryFlag, UPTD.FolioAddresses_FolioAddress_ProvinceState, UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix, UPTD.FolioAddresses_FolioAddress_StreetName, UPTD.FolioAddresses_FolioAddress_StreetNumber, UPTD.FolioAddresses_FolioAddress_StreetType, UPTD.FolioAddresses_FolioAddress_UnitNumber, UPTD.LandMeasurement_LandDepth, UPTD.LandMeasurement_LandDimension, UPTD.LandMeasurement_LandDimensionTypeDescription, UPTD.LandMeasurement_LandWidth, UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode, UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription, UPTD.RegionalDistrict_DistrictDescription, UPTD.
				SchoolDistrict_DistrictDescription, UPTD.LegalDescriptions_LegalDescription_Block, UPTD.LegalDescriptions_LegalDescription_DistrictLot, UPTD.LegalDescriptions_LegalDescription_ExceptPlan, UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription, UPTD.LegalDescriptions_LegalDescription_ID, UPTD.LegalDescriptions_LegalDescription_LandDistrict, UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription, UPTD.LegalDescriptions_LegalDescription_LeaseLicenceNumber, UPTD.LegalDescriptions_LegalDescription_LegalText, UPTD.LegalDescriptions_LegalDescription_Lot, UPTD.LegalDescriptions_LegalDescription_Meridian, UPTD.LegalDescriptions_LegalDescription_MeridianShort, UPTD.LegalDescriptions_LegalDescription_Parcel, UPTD.LegalDescriptions_LegalDescription_Part1, UPTD.LegalDescriptions_LegalDescription_Part2, UPTD.LegalDescriptions_LegalDescription_Part3, UPTD.LegalDescriptions_LegalDescription_Part4, UPTD.LegalDescriptions_LegalDescription_PID, UPTD.
				LegalDescriptions_LegalDescription_Plan, UPTD.LegalDescriptions_LegalDescription_Portion, UPTD.LegalDescriptions_LegalDescription_Range, UPTD.LegalDescriptions_LegalDescription_Section, UPTD.LegalDescriptions_LegalDescription_StrataLot, UPTD.LegalDescriptions_LegalDescription_SubBlock, UPTD.LegalDescriptions_LegalDescription_SubLot, UPTD.LegalDescriptions_LegalDescription_LegalSubdivision, UPTD.LegalDescriptions_LegalDescription_Township, WK.Sales_Sale_ConveyanceDate, WK.Sales_Sale_ConveyancePrice, WK.Sales_Sale_ID)) AS HashBytes
	INTO #Sales_UptoDate
	FROM dbo.BC_UPTO_DATE UPTD
	INNER JOIN #Sales_Weekly WK ON UPTD.FolioRecord_ID = WK.FolioRecord_ID AND UPTD.FolioAddresses_FolioAddress_PrimaryFlag = 'True' AND UPTD.RollYear=WK.RollYear-1;

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
		,@StartDate_ALS AS DateCreatedUTC
		,@StartDate_ALS AS LastModifiedDateUTC
	FROM #Sales_UptoDate NF
	WHERE NOT EXISTS (
			SELECT 1
			FROM dbo.BC_UPTO_DATE uptd
			WHERE uptd.HashBytes = NF.HashBytes
			);


	--Step 5. New legal
	SELECT FolioRecord_ID
		,RollYear
		,RollNumber
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
	INTO #NewLegal
	FROM StageLanding.BC_ALL_Assessment_Weekly WK
	WHERE LegalDescriptions_LegalDescription_Action = 'Add'

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
		,WK.LegalDescriptions_LegalDescription_Block
		,WK.LegalDescriptions_LegalDescription_DistrictLot
		,WK.LegalDescriptions_LegalDescription_ExceptPlan
		,WK.LegalDescriptions_LegalDescription_FormattedLegalDescription
		,WK.LegalDescriptions_LegalDescription_ID
		,WK.LegalDescriptions_LegalDescription_LandDistrict
		,WK.LegalDescriptions_LegalDescription_LandDistrictDescription
		,WK.LegalDescriptions_LegalDescription_LeaseLicenceNumber
		,WK.LegalDescriptions_LegalDescription_LegalText
		,WK.LegalDescriptions_LegalDescription_Lot
		,WK.LegalDescriptions_LegalDescription_Meridian
		,WK.LegalDescriptions_LegalDescription_MeridianShort
		,WK.LegalDescriptions_LegalDescription_Parcel
		,WK.LegalDescriptions_LegalDescription_Part1
		,WK.LegalDescriptions_LegalDescription_Part2
		,WK.LegalDescriptions_LegalDescription_Part3
		,WK.LegalDescriptions_LegalDescription_Part4
		,WK.LegalDescriptions_LegalDescription_PID
		,WK.LegalDescriptions_LegalDescription_Plan
		,WK.LegalDescriptions_LegalDescription_Portion
		,WK.LegalDescriptions_LegalDescription_Range
		,WK.LegalDescriptions_LegalDescription_Section
		,WK.LegalDescriptions_LegalDescription_StrataLot
		,WK.LegalDescriptions_LegalDescription_SubBlock
		,WK.LegalDescriptions_LegalDescription_SubLot
		,WK.LegalDescriptions_LegalDescription_LegalSubdivision
		,WK.LegalDescriptions_LegalDescription_Township
		,UPTD.Sales_Sale_ConveyanceDate
		,UPTD.Sales_Sale_ConveyancePrice
		,UPTD.Sales_Sale_ID
		,HASHBYTES('SHA2_512', CONCAT_WS('|', UPTD.FolioRecord_ID, WK.RollYear, UPTD.AssessmentAreaDescription, UPTD.JurisdictionCode, UPTD.JurisdictionDescription, UPTD.RollNumber, UPTD.ActualUseDescription, UPTD.VacantFlag, UPTD.TenureDescription, UPTD.FolioAddresses_FolioAddress_City, UPTD.FolioAddresses_FolioAddress_ID, UPTD.FolioAddresses_FolioAddress_PostalZip, UPTD.FolioAddresses_FolioAddress_PrimaryFlag, UPTD.FolioAddresses_FolioAddress_ProvinceState, UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix, UPTD.FolioAddresses_FolioAddress_StreetName, UPTD.FolioAddresses_FolioAddress_StreetNumber, UPTD.FolioAddresses_FolioAddress_StreetType, UPTD.FolioAddresses_FolioAddress_UnitNumber, UPTD.LandMeasurement_LandDepth, UPTD.LandMeasurement_LandDimension, UPTD.LandMeasurement_LandDimensionTypeDescription, UPTD.LandMeasurement_LandWidth, UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode, UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription, UPTD.RegionalDistrict_DistrictDescription, UPTD.
				SchoolDistrict_DistrictDescription, WK.LegalDescriptions_LegalDescription_Block, WK.LegalDescriptions_LegalDescription_DistrictLot, WK.LegalDescriptions_LegalDescription_ExceptPlan, WK.LegalDescriptions_LegalDescription_FormattedLegalDescription, WK.LegalDescriptions_LegalDescription_ID, WK.LegalDescriptions_LegalDescription_LandDistrict, WK.LegalDescriptions_LegalDescription_LandDistrictDescription, WK.LegalDescriptions_LegalDescription_LeaseLicenceNumber, WK.LegalDescriptions_LegalDescription_LegalText, WK.LegalDescriptions_LegalDescription_Lot, WK.LegalDescriptions_LegalDescription_Meridian, WK.LegalDescriptions_LegalDescription_MeridianShort, WK.LegalDescriptions_LegalDescription_Parcel, WK.LegalDescriptions_LegalDescription_Part1, WK.LegalDescriptions_LegalDescription_Part2, WK.LegalDescriptions_LegalDescription_Part3, WK.LegalDescriptions_LegalDescription_Part4, WK.LegalDescriptions_LegalDescription_PID, WK.LegalDescriptions_LegalDescription_Plan, WK.
				LegalDescriptions_LegalDescription_Portion, WK.LegalDescriptions_LegalDescription_Range, WK.LegalDescriptions_LegalDescription_Section, WK.LegalDescriptions_LegalDescription_StrataLot, WK.LegalDescriptions_LegalDescription_SubBlock, WK.LegalDescriptions_LegalDescription_SubLot, WK.LegalDescriptions_LegalDescription_LegalSubdivision, WK.LegalDescriptions_LegalDescription_Township, UPTD.Sales_Sale_ConveyanceDate, UPTD.Sales_Sale_ConveyancePrice, UPTD.Sales_Sale_ID)) AS HashBytes
	INTO #NewLegal_Final
	FROM dbo.BC_UPTO_DATE UPTD
	INNER JOIN #NewLegal WK ON UPTD.FolioRecord_ID = WK.FolioRecord_ID AND UPTD.FolioAddresses_FolioAddress_PrimaryFlag = 'True' AND UPTD.RollYear=WK.RollYear-1;

	INSERT INTO dbo.BC_UPTO_DATE(
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
		,@StartDate_ALS AS DateCreatedUTC
		,@StartDate_ALS AS LastModifiedDateUTC
	FROM #NewLegal_Final NF
	WHERE NOT EXISTS (
			SELECT 1
			FROM dbo.BC_UPTO_DATE uptd
			WHERE uptd.HashBytes = NF.HashBytes
			);

	UPDATE UPTD
	SET LegalDescriptions_LegalDescription_Block = NULL
		,LegalDescriptions_LegalDescription_DistrictLot = NULL
		,LegalDescriptions_LegalDescription_ExceptPlan = NULL
		,LegalDescriptions_LegalDescription_FormattedLegalDescription = NULL
		,LegalDescriptions_LegalDescription_ID = NULL
		,LegalDescriptions_LegalDescription_LandDistrict = NULL
		,LegalDescriptions_LegalDescription_LandDistrictDescription = NULL
		,LegalDescriptions_LegalDescription_LeaseLicenceNumber = NULL
		,LegalDescriptions_LegalDescription_LegalText = NULL
		,LegalDescriptions_LegalDescription_Lot = NULL
		,LegalDescriptions_LegalDescription_Meridian = NULL
		,LegalDescriptions_LegalDescription_MeridianShort = NULL
		,LegalDescriptions_LegalDescription_Parcel = NULL
		,LegalDescriptions_LegalDescription_Part1 = NULL
		,LegalDescriptions_LegalDescription_Part2 = NULL
		,LegalDescriptions_LegalDescription_Part3 = NULL
		,LegalDescriptions_LegalDescription_Part4 = NULL
		,LegalDescriptions_LegalDescription_PID = NULL
		,LegalDescriptions_LegalDescription_Plan = NULL
		,LegalDescriptions_LegalDescription_Portion = NULL
		,LegalDescriptions_LegalDescription_Range = NULL
		,LegalDescriptions_LegalDescription_Section = NULL
		,LegalDescriptions_LegalDescription_StrataLot = NULL
		,LegalDescriptions_LegalDescription_SubBlock = NULL
		,LegalDescriptions_LegalDescription_SubLot = NULL
		,LegalDescriptions_LegalDescription_LegalSubdivision = NULL
		,LegalDescriptions_LegalDescription_Township = NULL
		,Sales_Sale_ConveyanceDate = NULL
		,Sales_Sale_ConveyancePrice = NULL
		,Sales_Sale_ID = NULL
		FROM dbo.BC_UPTO_DATE UPTD
		WHERE UPTD.FolioAddresses_FolioAddress_PrimaryFlag='false'
		AND LastModifiedDateUTC=@StartDate_ALS;


		-- Step 6. Insert records into StageLanding.BC_ALL_Assessment(DTC)
	INSERT INTO StageLanding.BC_ALL_Assessment (
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
		)
	SELECT DISTINCT FolioRecord_ID
		,RollYear
		,AssessmentAreaDescription
		,JurisdictionCode
		,JurisdictionDescription
		,RollNumber
		,ActualUseDescription
		,VacantFlag
		,TenureDescription
		,FolioAddresses_FolioAddress_City
		,FolioAddresses_FolioAddress_PostalZip
		,FolioAddresses_FolioAddress_PrimaryFlag
		,FolioAddresses_FolioAddress_ProvinceState
		,FolioAddresses_FolioAddress_StreetDirectionSuffix
		,FolioAddresses_FolioAddress_StreetName
		,FolioAddresses_FolioAddress_StreetNumber
		,FolioAddresses_FolioAddress_StreetType
		,FolioAddresses_FolioAddress_UnitNumber
		,CASE WHEN LTRIM(RTRIM(LandMeasurement_LandDepth)) = '' THEN NULL ELSE LandMeasurement_LandDepth END AS LandMeasurement_LandDepth
		,CASE WHEN LTRIM(RTRIM(LandMeasurement_LandDimension)) = '' THEN NULL ELSE LandMeasurement_LandDimension END AS LandMeasurement_LandDimension
		,LandMeasurement_LandDimensionTypeDescription
		,CASE WHEN LTRIM(RTRIM(LandMeasurement_LandWidth)) = '' THEN NULL ELSE LandMeasurement_LandWidth END AS LandMeasurement_LandWidth
		,FolioDescription_Neighbourhood_NeighbourhoodCode
		,FolioDescription_Neighbourhood_NeighbourhoodDescription
		,RegionalDistrict_DistrictDescription
		,SchoolDistrict_DistrictDescription
		,LegalDescriptions_LegalDescription_Block
		,LegalDescriptions_LegalDescription_DistrictLot
		,LegalDescriptions_LegalDescription_ExceptPlan
		,LegalDescriptions_LegalDescription_FormattedLegalDescription
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
	FROM dbo.BC_UPTO_DATE
	WHERE LastModifiedDateUTC >= @StartDate
	AND	LastModifiedDateUTC<=@StartDate_ALS;


	/*

	-- Step 7. Insert previious invalid records that turned valid now

	INSERT INTO StageLanding.BC_ALL_Assessment (
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
		)
Select DISTINCT UPTD.FolioRecord_ID
		,WK.RollYear
		,UPTD.AssessmentAreaDescription
		,UPTD.JurisdictionCode
		,UPTD.JurisdictionDescription
		,UPTD.RollNumber
		,UPTD.ActualUseDescription
		,UPTD.VacantFlag
		,UPTD.TenureDescription
		,WK.FolioAddresses_FolioAddress_City
		,WK.FolioAddresses_FolioAddress_PostalZip
		,WK.FolioAddresses_FolioAddress_PrimaryFlag
		,WK.FolioAddresses_FolioAddress_ProvinceState
		,WK.FolioAddresses_FolioAddress_StreetDirectionSuffix
		,WK.FolioAddresses_FolioAddress_StreetName
		,WK.FolioAddresses_FolioAddress_StreetNumber
		,WK.FolioAddresses_FolioAddress_StreetType
		,WK.FolioAddresses_FolioAddress_UnitNumber
		,CASE WHEN LTRIM(RTRIM(UPTD.LandMeasurement_LandDepth)) = '' THEN NULL ELSE UPTD.LandMeasurement_LandDepth END AS LandMeasurement_LandDepth
		,CASE WHEN LTRIM(RTRIM(UPTD.LandMeasurement_LandDimension)) = '' THEN NULL ELSE UPTD.LandMeasurement_LandDimension END AS LandMeasurement_LandDimension
		,UPTD.LandMeasurement_LandDimensionTypeDescription
		,CASE WHEN LTRIM(RTRIM(UPTD.LandMeasurement_LandWidth)) = '' THEN NULL ELSE UPTD.LandMeasurement_LandWidth END AS LandMeasurement_LandWidth
		,UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
		,UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
		,UPTD.RegionalDistrict_DistrictDescription
		,UPTD.SchoolDistrict_DistrictDescription
		,UPTD.LegalDescriptions_LegalDescription_Block
		,UPTD.LegalDescriptions_LegalDescription_DistrictLot
		,UPTD.LegalDescriptions_LegalDescription_ExceptPlan
		,UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription
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
		,UPTD.Sales_Sale_ConveyanceDate
		,UPTD.Sales_Sale_ConveyancePrice 
from StageLanding.BC_ALL_Assessment_Weekly WK
Inner join dbo.BC_UPTO_DATE UPTD ON WK.FolioRecord_ID = UPTD.FolioRecord_ID AND WK.RollNumber=UPTD.RollNumber AND WK.JurisdictionCode=UPTD.JurisdictionCode AND WK.FolioAddresses_FolioAddress_PrimaryFlag = UPTD.FolioAddresses_FolioAddress_PrimaryFlag
AND  UPTD.RollYear = WK.RollYear - 1 
Where 
	(
		(
			WK.FolioAddresses_FolioAddress_StreetName_Action='Change'
			AND WK.FolioAddresses_FolioAddress_StreetName_OldValue IS NULL
			AND WK.FolioAddresses_FolioAddress_StreetName IS NOT NULL
		)
		OR
		(
			WK.FolioAddresses_FolioAddress_StreetNumber_Action='Change'
			AND WK.FolioAddresses_FolioAddress_StreetNumber_OldValue IS NULL
			AND WK.FolioAddresses_FolioAddress_StreetNumber IS NOT NULL
		)
	)
Union

Select DISTINCT UPTD.FolioRecord_ID
		,WK.RollYear
		,UPTD.AssessmentAreaDescription
		,UPTD.JurisdictionCode
		,UPTD.JurisdictionDescription
		,UPTD.RollNumber
		,UPTD.ActualUseDescription
		,UPTD.VacantFlag
		,UPTD.TenureDescription
		,UPTD.FolioAddresses_FolioAddress_City
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
		,WK.LegalDescriptions_LegalDescription_Block
		,WK.LegalDescriptions_LegalDescription_DistrictLot
		,WK.LegalDescriptions_LegalDescription_ExceptPlan
		,WK.LegalDescriptions_LegalDescription_FormattedLegalDescription
		,WK.LegalDescriptions_LegalDescription_LandDistrict
		,WK.LegalDescriptions_LegalDescription_LandDistrictDescription
		,WK.LegalDescriptions_LegalDescription_LeaseLicenceNumber
		,WK.LegalDescriptions_LegalDescription_LegalText
		,WK.LegalDescriptions_LegalDescription_Lot
		,WK.LegalDescriptions_LegalDescription_Meridian
		,WK.LegalDescriptions_LegalDescription_MeridianShort
		,WK.LegalDescriptions_LegalDescription_Parcel
		,WK.LegalDescriptions_LegalDescription_Part1
		,WK.LegalDescriptions_LegalDescription_Part2
		,WK.LegalDescriptions_LegalDescription_Part3
		,WK.LegalDescriptions_LegalDescription_Part4
		,WK.LegalDescriptions_LegalDescription_PID
		,WK.LegalDescriptions_LegalDescription_Plan
		,WK.LegalDescriptions_LegalDescription_Portion
		,WK.LegalDescriptions_LegalDescription_Range
		,WK.LegalDescriptions_LegalDescription_Section
		,WK.LegalDescriptions_LegalDescription_StrataLot
		,WK.LegalDescriptions_LegalDescription_SubBlock
		,WK.LegalDescriptions_LegalDescription_SubLot
		,WK.LegalDescriptions_LegalDescription_LegalSubdivision
		,WK.LegalDescriptions_LegalDescription_Township
		,UPTD.Sales_Sale_ConveyanceDate
		,UPTD.Sales_Sale_ConveyancePrice
from StageLanding.BC_ALL_Assessment_Weekly WK
Inner join dbo.BC_UPTO_DATE UPTD ON WK.FolioRecord_ID = UPTD.FolioRecord_ID AND WK.LegalDescriptions_LegalDescription_ID = UPTD.LegalDescriptions_LegalDescription_ID AND Wk.FolioAddresses_FolioAddress_PrimaryFlag = UPTD.FolioAddresses_FolioAddress_PrimaryFlag
Where WK.LegalDescriptions_LegalDescription_PID_Action='Change'
AND WK.LegalDescriptions_LegalDescription_PID_OldValue IS NULL
AND WK.LegalDescriptions_LegalDescription_PID IS NOT NULL
AND UPTD.FolioAddresses_FolioAddress_PrimaryFlag='True'

Union


Select DISTINCT UPTD.FolioRecord_ID
		,WK.RollYear
		,UPTD.AssessmentAreaDescription
		,UPTD.JurisdictionCode
		,UPTD.JurisdictionDescription
		,WK.RollNumber
		,UPTD.ActualUseDescription
		,UPTD.VacantFlag
		,UPTD.TenureDescription
		,UPTD.FolioAddresses_FolioAddress_City
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
		,UPTD.Sales_Sale_ConveyanceDate
		,UPTD.Sales_Sale_ConveyancePrice 
from StageLanding.BC_ALL_Assessment_Weekly WK
Inner join dbo.BC_UPTO_DATE UPTD ON WK.FolioRecord_ID = UPTD.FolioRecord_ID
Where WK.RollNumber_Action='Change'
AND WK.RollNumber_OldValue IS NULL
AND WK.RollNumber IS NOT NULL;
	
	*/

		COMMIT TRAN;
	END TRY

	BEGIN CATCH  
		ROLLBACK TRAN;

		INSERT INTO ETLProcess.ETLStoredProcedureErrors        
       (        
        ProcessCategory        
       , ProcessName        
       , ErrorNumber        
       , ErrorSeverity        
       , ErrorState        
       , ErrorProcedure        
       , ErrorLine        
       , ErrorMessage        
       , ErrorDate        
       )        
       SELECT          
        @ProcessCategory        
       , @ProcessName        
       , ERROR_NUMBER() AS ErrorNumber          
       , ERROR_SEVERITY() AS ErrorSeverity          
       , ERROR_STATE() AS ErrorState          
       , @ErrorProcedure        
       , ERROR_LINE() AS ErrorLine          
       , ERROR_MESSAGE() AS ErrorMessage        
       , GETDATE()  

	END CATCH

END