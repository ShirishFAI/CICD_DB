

CREATE PROCEDURE [ETLProcess].[BC_UPDATE_DTC_Through_UPTD]
AS
BEGIN
	/****************************************************************************************
-- AUTHOR		: Shirish W.
-- DATE			: 11/22/2022
-- PURPOSE		: Update records into DTC entities through BC UPTO Date
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** --------------------------------------------------------------------------------------
** 09/25/2020	Shirish W.	Original Version
******************************************************************************************/

DECLARE @StartDate DATETIME;
DECLARE @BC_UPTD_LastLoadDate DATETIME;
SET @StartDate = GETDATE();
SET @BC_UPTD_LastLoadDate = (Select LoadDate from dbo.BC_UPTO_DATE_LoadDate WHERE processName='BC_UPTO_DATE');

	DECLARE @ProcessName VARCHAR(100) = 'BC Update DTC Through BC UPTD';
	DECLARE @ProcessCategory VARCHAR(100) = 'DTC_ExternalSource_ETL';
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @ProcessID INT;

	SET @ProcessID = (SELECT ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName='BC_ALL_Assessment')

	SELECT @ErrorProcedure = s.name + '.' + o.name
	FROM SYS.OBJECTS O
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID = O.SCHEMA_ID
	WHERE OBJECT_ID = @@PROCID;

	BEGIN TRY
		BEGIN TRAN

--  Deleting Duplicate Records from BC_UPTO_DATE
  SELECT DISTINCT ID,CODE
INTO #DuplicateRecords
FROM
(
SELECT *
		,ROW_NUMBER() OVER (
			Partition By code order by code
			) RNK
	FROM dbo.BC_UPTO_DATE
	WHERE CODE IS NOT NULL
)A
WHERE RNK>1;

DELETE FROM dbo.BC_UPTO_DATE
WHERE ID IN (SELECT ID FROM #DuplicateRecords);

-- Step 1. Delete Address from DTC 

		SELECT *,row_number() over(order by (Select 1)) as ID1
		INTO #BC_UPTO_DATE
		FROM dbo.BC_UPTO_DATE UPTD WITH(NOLOCK)
		WHERE UPTD.Code IS NOT NULL 
		AND UPTD.LastModifiedDateUTC > @BC_UPTD_LastLoadDate

		DECLARE @maxBatchId BIGINT
		,@fromBatchId BIGINT = 1
		,@toBatchId BIGINT
		,@incrmt BIGINT = 100000
		,@dupcount BIGINT
		,@TotDelCount BIGINT = 0
		,@CurStatus VARCHAR(100)

		SELECT @maxBatchId = Max(ID1) FROM #BC_UPTO_DATE

		SET @toBatchId = @incrmt

	WHILE (@fromBatchId <= @maxBatchId)
	  BEGIN
		--IF OBJECT_ID(N'tempdb..#Address_Update') IS NOT NULL
		--	BEGIN
		--		DROP TABLE #Address_Update
		--	END

		--SELECT UPTD.*
		--INTO #Address_Update
		--FROM #BC_UPTO_DATE UPTD
		--INNER JOIN DBO.Address ADDR WITH(NOLOCK) ON UPTD.Code = ADDR.Code
		--WHERE 
		--(
		--	ADDR.StreetNumber != UPTD.FolioAddresses_FolioAddress_StreetNumber
		--OR	ADDR.StreetName != UPTD.FolioAddresses_FolioAddress_StreetName
		--OR	ADDR.UnitNumber != UPTD.FolioAddresses_FolioAddress_UnitNumber
		--OR	ADDR.StreetType != UPTD.FolioAddresses_FolioAddress_StreetType
		--OR	ADDR.StreetDirection != UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
		--OR	ADDR.PostalCode != UPTD.FolioAddresses_FolioAddress_PostalZip
		--OR	ADDR.City != UPTD.FolioAddresses_FolioAddress_City
		--)
		--AND (UPTD.ID >= @fromBatchId AND UPTD.ID <= @toBatchId)
		
		UPDATE Address
		SET Address.AreaDescription = UPTD.AssessmentAreaDescription
			,Address.JurCode = UPTD.JurisdictionCode
			,Address.JurDescription = UPTD.JurisdictionDescription
			,Address.LandDistrict = UPTD.LegalDescriptions_LegalDescription_LandDistrict
			,Address.LandDistrictName = UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription
			,Address.Neighbourhood = UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
			,Address.NeighbourhoodDescription = UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
			,Address.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Address.Range = UPTD.LegalDescriptions_LegalDescription_Range
			,Address.Region = UPTD.RegionalDistrict_DistrictDescription
			,Address.SchoolDistrictDescription = UPTD.SchoolDistrict_DistrictDescription
			,Address.Township = UPTD.LegalDescriptions_LegalDescription_Township
			,Address.LastModifiedDateUTC = @StartDate
			,Address.City = UPTD.FolioAddresses_FolioAddress_City
			,Address.PostalCode = UPTD.FolioAddresses_FolioAddress_PostalZip
			,Address.StreetDirection = UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
			,Address.StreetName = UPTD.FolioAddresses_FolioAddress_StreetName
			,Address.StreetNumber = UPTD.FolioAddresses_FolioAddress_StreetNumber
			,Address.StreetType = UPTD.FolioAddresses_FolioAddress_StreetType
			,Address.UnitNumber = UPTD.FolioAddresses_FolioAddress_UnitNumber
		FROM dbo.Address Address 
		INNER JOIN #BC_UPTO_DATE UPTD ON Address.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)

		--UPDATE Address
		--SET MasterAddressID = NULL
		--	,IsMADSent=NULL
		--	,MADSentDateUTC=NULL
		--	,MADReceivedDateUTC=NULL
		--	,IsMADReceived=NULL
		--FROM dbo.Address Address
		--INNER JOIN #Address_Update UPTD ON Address.Code = UPTD.Code

		--Update in Invalid and re-process
		UPDATE Address_Invalid
		SET Address_Invalid.AreaDescription = UPTD.AssessmentAreaDescription
			,Address_Invalid.City = UPTD.FolioAddresses_FolioAddress_City
			,Address_Invalid.JurCode = UPTD.JurisdictionCode
			,Address_Invalid.JurDescription = UPTD.JurisdictionDescription
			,Address_Invalid.LandDistrict = UPTD.LegalDescriptions_LegalDescription_LandDistrict
			,Address_Invalid.LandDistrictName = UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription
			,Address_Invalid.Neighbourhood = UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
			,Address_Invalid.NeighbourhoodDescription = UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
			,Address_Invalid.PostalCode = UPTD.FolioAddresses_FolioAddress_PostalZip
			,Address_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Address_Invalid.Range = UPTD.LegalDescriptions_LegalDescription_Range
			,Address_Invalid.Region = UPTD.RegionalDistrict_DistrictDescription
			,Address_Invalid.SchoolDistrictDescription = UPTD.SchoolDistrict_DistrictDescription
			,Address_Invalid.StreetDirection = UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
			,Address_Invalid.StreetName = UPTD.FolioAddresses_FolioAddress_StreetName
			,Address_Invalid.StreetNumber = UPTD.FolioAddresses_FolioAddress_StreetNumber
			,Address_Invalid.StreetType = UPTD.FolioAddresses_FolioAddress_StreetType
			,Address_Invalid.Township = UPTD.LegalDescriptions_LegalDescription_Township
			,Address_Invalid.UnitNumber = UPTD.FolioAddresses_FolioAddress_UnitNumber
			,Address_Invalid.IsPermanentlyInvalid = 0
			,Address_Invalid.ReProcess=1
			--,Address_Invalid.IsDuplicate=0
			,Address_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Address_Invalid Address_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Address_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)

		ALTER TABLE Profisee.data.tAddress NOCHECK CONSTRAINT ALL;

		--UPDATE tAddress
		--SET MasterAddressID = NULL
		--FROM Profisee.data.tAddress tAddress
		--INNER JOIN #Address_Update UPTD ON tAddress.Code = UPTD.Code

		UPDATE tAddress
		SET tAddress.AreaDescription = UPTD.AssessmentAreaDescription
			,tAddress.JurCode = UPTD.JurisdictionCode
			,tAddress.JurDescription = UPTD.JurisdictionDescription
			,tAddress.LandDistrict = UPTD.LegalDescriptions_LegalDescription_LandDistrict
			,tAddress.LandDistrictName = UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription
			,tAddress.Neighbourhood = UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
			,tAddress.NeighbourhoodDescription = UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
			,tAddress.PostalCode = UPTD.FolioAddresses_FolioAddress_PostalZip
			,tAddress.Range = UPTD.LegalDescriptions_LegalDescription_Range
			,tAddress.Region = UPTD.RegionalDistrict_DistrictDescription
			,tAddress.SchoolDistrictDescription = UPTD.SchoolDistrict_DistrictDescription
			--,tAddress.MasterAddressID =NULL
			,tAddress.[Match Group]=NULL     
			,tAddress.[Match Score]=NULL     
			,tAddress.[Match Status]=NULL    
			,tAddress.[Record Source]=NULL   
			,tAddress.[Match Member]=NULL    
			,tAddress.[Match Strategy]=NULL  
			,tAddress.[Match DateTime]=NULL  
			,tAddress.[Match User]=NULL      
			,tAddress.[Match MultiGroup]=NULL
			,tAddress.[Master]=NULL          
			,tAddress.[Approved Count]=NULL  
			,tAddress.[Proposed Count]=NULL  
			,tAddress.LastModifiedDateUTC = @StartDate
			,tAddress.City = UPTD.FolioAddresses_FolioAddress_City
			,tAddress.StreetDirection = UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
			,tAddress.StreetName = UPTD.FolioAddresses_FolioAddress_StreetName
			,tAddress.StreetNumber = UPTD.FolioAddresses_FolioAddress_StreetNumber
			,tAddress.StreetType = UPTD.FolioAddresses_FolioAddress_StreetType
			,tAddress.UnitNumber = UPTD.FolioAddresses_FolioAddress_UnitNumber
			,taddress.Township = UPTD.LegalDescriptions_LegalDescription_Township
			,taddress.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
		FROM Profisee.data.tAddress tAddress
		INNER JOIN #BC_UPTO_DATE UPTD ON tAddress.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)

		UPDATE tAddress
		SET tAddress.AreaDescription = NULL
			,tAddress.JurCode = NULL
			,tAddress.JurDescription = NULL
			,tAddress.LandDistrict = NULL
			,tAddress.LandDistrictName = NULL
			,tAddress.Neighbourhood = NULL
			,tAddress.NeighbourhoodDescription = NULL
			,tAddress.PostalCode = NULL
			,tAddress.Range = NULL
			,tAddress.Region = NULL
			,tAddress.SchoolDistrictDescription = NULL
			,tAddress.MasterAddressID =NULL
			,tAddress.[Match Group]=NULL     
			,tAddress.[Match Score]=NULL     
			,tAddress.[Match Status]=NULL    
			,tAddress.[Record Source]=NULL   
			,tAddress.[Match Member]=NULL    
			,tAddress.[Match Strategy]=NULL  
			,tAddress.[Match DateTime]=NULL  
			,tAddress.[Match User]=NULL      
			,tAddress.[Match MultiGroup]=NULL
			,tAddress.[Master]=NULL          
			,tAddress.[Approved Count]=NULL  
			,tAddress.[Proposed Count]=NULL  
			,tAddress.LastModifiedDateUTC = @StartDate
			,tAddress.City = NULL
			,tAddress.StreetDirection = NULL
			,tAddress.StreetName = NULL
			,tAddress.StreetNumber = NULL
			,tAddress.StreetType = NULL
			,tAddress.UnitNumber = NULL
			,taddress.Township = NULL
			,taddress.ProvinceCode = NULL
		from dbo.address_Invalid a 
		inner join profisee.data.taddress tAddress on a.code = tAddress.code
		where a.streetnumber is null
		and a.StreetName is null
		and a.City is null 
		and a.PostalCode is null
		and tAddress.StreetName is not null
		and tAddress.StreetNumber is not null
		and tAddress.City is not null;

		UPDATE tAddress
		SET MasterAddressID = NULL,FullAddress=NULL, Latitude=NULL, Longitude = NULL
		FROM Profisee.data.tAddress tAddress
		WHERE StreetName IS NULL
		AND StreetNumber IS NULL
		AND City IS NULL
		AND LastModifiedDateUTC = @StartDate

		UPDATE tAddress
		SET FullAddress = LTRIM(RTRIM(CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') )))
		FROM Profisee.data.tAddress tAddress
		WHERE LastModifiedDateUTC = @StartDate
		AND StreetName IS NOT NULL
		AND StreetNumber IS NOT NULL

		ALTER TABLE Profisee.data.tAddress CHECK CONSTRAINT ALL;

		-- Updates Address fiels from Building to NULL
		
		UPDATE Building
		SET Building.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Building.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Building.LastModifiedDateUTC = @StartDate
		FROM dbo.Building Building
		INNER JOIN #BC_UPTO_DATE UPTD ON Building.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)

		UPDATE Building_Invalid
		SET Building_Invalid.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Building_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Building_Invalid.ReProcess=1
			,Building_Invalid.IsPermanentlyInvalid = 0
			--,Building_Invalid.IsDuplicate=0
			,Building_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Building_Invalid Building_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Building_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		--Update in Invalid and re-process
		UPDATE Building_Invalid
		SET Building_Invalid.LastModifiedDateUTC = @StartDate
			,Building_Invalid.IsPermanentlyInvalid = 0
			,Building_Invalid.ReProcess=1
			--,Building_Invalid.IsDuplicate=0
		FROM dbo.Building_Invalid Building_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Building_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;
		--WHERE UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;

		ALTER TABLE Profisee.data.tBuilding NOCHECK CONSTRAINT ALL;
		UPDATE tBuilding
		SET tBuilding.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,tBuilding.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			--,tBuilding.MasterAddressID = NULL
			,tBuilding.[Match Group] = NULL
			,tBuilding.[Match Score] = NULL
			,tBuilding.[Match Status] = NULL
			,tBuilding.[Record Source] = NULL
			,tBuilding.[Match Member] = NULL
			,tBuilding.[Match Strategy] = NULL
			,tBuilding.[Match DateTime] = NULL
			,tBuilding.[Match User] = NULL
			,tBuilding.[Match MultiGroup] = NULL
			,tBuilding.[Master] = NULL
			,tBuilding.[Approved Count] = NULL
			,tBuilding.[Proposed Count] = NULL
			,tBuilding.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tBuilding tBuilding
		INNER JOIN #BC_UPTO_DATE UPTD ON tBuilding.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)


		ALTER TABLE Profisee.data.tBuilding CHECK CONSTRAINT ALL;

		-- Updates Address fiels from Business to NULL
		UPDATE Business
		SET Business.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Business.LastModifiedDateUTC = @StartDate
		FROM dbo.Business Business
		INNER JOIN #BC_UPTO_DATE UPTD ON Business.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)

		--Update in Invalid and re-process

		UPDATE Business_Invalid
		SET Business_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Business_Invalid.IsPermanentlyInvalid = 0
			,Business_Invalid.ReProcess=1
			--,Business_Invalid.IsDuplicate=0
			,Business_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Business_Invalid Business_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Business_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId)

		UPDATE Business_Invalid
		SET Business_Invalid.IsPermanentlyInvalid = 0
			,Business_Invalid.ReProcess=1
			--,Business_Invalid.IsDuplicate=0
			,Business_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Business_Invalid Business_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Business_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;

		UPDATE tBusiness
		SET tBusiness.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,tBusiness.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tBusiness tBusiness
		INNER JOIN #BC_UPTO_DATE UPTD ON tBusiness.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		
		-- Updates Address fiels from Parcel to NULL
		UPDATE Parcel
		SET Parcel.IsVacantLand = UPTD.VacantFlag
			,Parcel.LegalDescription = UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,Parcel.LegalDescriptionBlock = UPTD.LegalDescriptions_LegalDescription_Block
			,Parcel.LegalDescriptionDistrictLot = UPTD.LegalDescriptions_LegalDescription_DistrictLot
			,Parcel.LegalDescriptionExceptPlan = UPTD.LegalDescriptions_LegalDescription_ExceptPlan
			,Parcel.LegalDescriptionLegalSubdivision = UPTD.LegalDescriptions_LegalDescription_LegalSubdivision
			,Parcel.LegalDescriptionLegalText = UPTD.LegalDescriptions_LegalDescription_LegalText
			,Parcel.LegalDescriptionLot = UPTD.LegalDescriptions_LegalDescription_Lot
			,Parcel.LegalDescriptionParcel = UPTD.LegalDescriptions_LegalDescription_Parcel
			,Parcel.LegalDescriptionPart1 = UPTD.LegalDescriptions_LegalDescription_Part1
			,Parcel.LegalDescriptionPart2 = UPTD.LegalDescriptions_LegalDescription_Part2
			,Parcel.LegalDescriptionPart3 = UPTD.LegalDescriptions_LegalDescription_Part3
			,Parcel.LegalDescriptionPart4 = UPTD.LegalDescriptions_LegalDescription_Part4
			,Parcel.LegalDescriptionPortion = UPTD.LegalDescriptions_LegalDescription_Portion
			,Parcel.LegalDescriptionSection = UPTD.LegalDescriptions_LegalDescription_Section
			,Parcel.LegalDescriptionStrataLot = UPTD.LegalDescriptions_LegalDescription_StrataLot
			,Parcel.LegalDescriptionSubBlock = UPTD.LegalDescriptions_LegalDescription_SubBlock
			,Parcel.LegalDescriptionSubLot = UPTD.LegalDescriptions_LegalDescription_SubLot
			,Parcel.LotDepth = UPTD.LandMeasurement_LandDepth
			,Parcel.LotFrontage = UPTD.LandMeasurement_LandWidth
			,Parcel.LotMeasureUnit = UPTD.LandMeasurement_LandDimensionTypeDescription
			,Parcel.LotSize = UPTD.LandMeasurement_LandDimension
			,Parcel.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Parcel.PlanNumber = UPTD.LegalDescriptions_LegalDescription_Plan
			,Parcel.PropertyUse = UPTD.ActualUseDescription
			,Parcel.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Parcel.LastModifiedDateUTC = @StartDate
			--,MasterAddressID = NULL
		FROM dbo.Parcel Parcel
		INNER JOIN #BC_UPTO_DATE UPTD ON Parcel.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);


		--Update in Invalid and re-process

		UPDATE Parcel_Invalid
		SET Parcel_Invalid.IsVacantLand = UPTD.VacantFlag
			,Parcel_Invalid.LegalDescription = UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,Parcel_Invalid.LegalDescriptionBlock = UPTD.LegalDescriptions_LegalDescription_Block
			,Parcel_Invalid.LegalDescriptionDistrictLot = UPTD.LegalDescriptions_LegalDescription_DistrictLot
			,Parcel_Invalid.LegalDescriptionExceptPlan = UPTD.LegalDescriptions_LegalDescription_ExceptPlan
			,Parcel_Invalid.LegalDescriptionLegalSubdivision = UPTD.LegalDescriptions_LegalDescription_LegalSubdivision
			,Parcel_Invalid.LegalDescriptionLegalText = UPTD.LegalDescriptions_LegalDescription_LegalText
			,Parcel_Invalid.LegalDescriptionLot = UPTD.LegalDescriptions_LegalDescription_Lot
			,Parcel_Invalid.LegalDescriptionParcel = UPTD.LegalDescriptions_LegalDescription_Parcel
			,Parcel_Invalid.LegalDescriptionPart1 = UPTD.LegalDescriptions_LegalDescription_Part1
			,Parcel_Invalid.LegalDescriptionPart2 = UPTD.LegalDescriptions_LegalDescription_Part2
			,Parcel_Invalid.LegalDescriptionPart3 = UPTD.LegalDescriptions_LegalDescription_Part3
			,Parcel_Invalid.LegalDescriptionPart4 = UPTD.LegalDescriptions_LegalDescription_Part4
			,Parcel_Invalid.LegalDescriptionPortion = UPTD.LegalDescriptions_LegalDescription_Portion
			,Parcel_Invalid.LegalDescriptionSection = UPTD.LegalDescriptions_LegalDescription_Section
			,Parcel_Invalid.LegalDescriptionStrataLot = UPTD.LegalDescriptions_LegalDescription_StrataLot
			,Parcel_Invalid.LegalDescriptionSubBlock = UPTD.LegalDescriptions_LegalDescription_SubBlock
			,Parcel_Invalid.LegalDescriptionSubLot = UPTD.LegalDescriptions_LegalDescription_SubLot
			,Parcel_Invalid.LotDepth = UPTD.LandMeasurement_LandDepth
			,Parcel_Invalid.LotFrontage = UPTD.LandMeasurement_LandWidth
			,Parcel_Invalid.LotMeasureUnit = UPTD.LandMeasurement_LandDimensionTypeDescription
			,Parcel_Invalid.LotSize = UPTD.LandMeasurement_LandDimension
			,Parcel_Invalid.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Parcel_Invalid.PlanNumber = UPTD.LegalDescriptions_LegalDescription_Plan
			,Parcel_Invalid.PropertyUse = UPTD.ActualUseDescription
			,Parcel_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Parcel_Invalid.IsPermanentlyInvalid = 0
			,Parcel_Invalid.ReProcess=1
			--,Parcel_Invalid.IsDuplicate=0
			,Parcel_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Parcel_Invalid Parcel_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Parcel_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		UPDATE Parcel_Invalid
		SET Parcel_Invalid.IsPermanentlyInvalid = 0
			,Parcel_Invalid.ReProcess=1
			--,Parcel_Invalid.IsDuplicate=0
			,Parcel_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Parcel_Invalid Parcel_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Parcel_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;


		ALTER TABLE Profisee.data.tParcel NOCHECK CONSTRAINT ALL;
		UPDATE tParcel
		SET tParcel.IsVacantLand = UPTD.VacantFlag
			,tParcel.LegalDescription = UPTD.LegalDescriptions_LegalDescription_FormattedLegalDescription
			,tParcel.LegalDescriptionBlock = UPTD.LegalDescriptions_LegalDescription_Block
			,tParcel.LegalDescriptionDistrictLot = UPTD.LegalDescriptions_LegalDescription_DistrictLot
			,tParcel.LegalDescriptionExceptPlan = UPTD.LegalDescriptions_LegalDescription_ExceptPlan
			,tParcel.LegalDescriptionLegalSubdivision = UPTD.LegalDescriptions_LegalDescription_LegalSubdivision
			,tParcel.LegalDescriptionLegalText = UPTD.LegalDescriptions_LegalDescription_LegalText
			,tParcel.LegalDescriptionLot = UPTD.LegalDescriptions_LegalDescription_Lot
			,tParcel.LegalDescriptionParcel = UPTD.LegalDescriptions_LegalDescription_Parcel
			,tParcel.LegalDescriptionPart1 = UPTD.LegalDescriptions_LegalDescription_Part1
			,tParcel.LegalDescriptionPart2 = UPTD.LegalDescriptions_LegalDescription_Part2
			,tParcel.LegalDescriptionPart3 = UPTD.LegalDescriptions_LegalDescription_Part3
			,tParcel.LegalDescriptionPart4 = UPTD.LegalDescriptions_LegalDescription_Part4
			,tParcel.LegalDescriptionPortion = UPTD.LegalDescriptions_LegalDescription_Portion
			,tParcel.LegalDescriptionSection = UPTD.LegalDescriptions_LegalDescription_Section
			,tParcel.LegalDescriptionStrataLot = UPTD.LegalDescriptions_LegalDescription_StrataLot
			,tParcel.LegalDescriptionSubBlock = UPTD.LegalDescriptions_LegalDescription_SubBlock
			,tParcel.LegalDescriptionSubLot = UPTD.LegalDescriptions_LegalDescription_SubLot
			,tParcel.LotDepth = UPTD.LandMeasurement_LandDepth
			,tParcel.LotFrontage = UPTD.LandMeasurement_LandWidth
			,tParcel.LotMeasureUnit = UPTD.LandMeasurement_LandDimensionTypeDescription
			,tParcel.LotSize = UPTD.LandMeasurement_LandDimension
			,tParcel.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,tParcel.PlanNumber = UPTD.LegalDescriptions_LegalDescription_Plan
			,tParcel.PropertyUse = UPTD.ActualUseDescription
			,tParcel.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			--,tParcel.MasterAddressID = NULL
			,tParcel.[Match Group] = NULL
			,tParcel.[Match Score] = NULL
			,tParcel.[Match Status] = NULL
			,tParcel.[Record Source] = NULL
			,tParcel.[Match Member] = NULL
			,tParcel.[Match Strategy] = NULL
			,tParcel.[Match DateTime] = NULL
			,tParcel.[Match User] = NULL
			,tParcel.[Match MultiGroup] = NULL
			,tParcel.[Master] = NULL
			,tParcel.[Approved Count] = NULL
			,tParcel.[Proposed Count] = NULL
			,tParcel.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tParcel tParcel
		INNER JOIN #BC_UPTO_DATE UPTD ON tParcel.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);


		ALTER TABLE Profisee.data.tParcel CHECK CONSTRAINT ALL;

		-- Updates Address fiels from Permit to NULL
		UPDATE Permit
		SET Permit.ARN = UPTD.RollNumber
			,Permit.JurCode = UPTD.JurisdictionCode
			,Permit.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Permit.LastModifiedDateUTC = @StartDate
			--,MasterAddressID = NULL
		FROM dbo.Permit Permit
		INNER JOIN #BC_UPTO_DATE UPTD ON Permit.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		ALTER TABLE Profisee.data.tPermit NOCHECK CONSTRAINT ALL;
		UPDATE tPermit
		SET tPermit.ARN = UPTD.RollNumber
			,tPermit.JurCode = UPTD.JurisdictionCode
			,tPermit.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			--,tPermit.MasterAddressID = NULL
			,tPermit.[Match Group] = NULL
			,tPermit.[Match Score] = NULL
			,tPermit.[Match Status] = NULL
			,tPermit.[Record Source] = NULL
			,tPermit.[Match Member] = NULL
			,tPermit.[Match Strategy] = NULL
			,tPermit.[Match DateTime] = NULL
			,tPermit.[Match User] = NULL
			,tPermit.[Match MultiGroup] = NULL
			,tPermit.[Master] = NULL
			,tPermit.[Approved Count] = NULL
			,tPermit.[Proposed Count] = NULL
			,tPermit.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tPermit tPermit
		INNER JOIN #BC_UPTO_DATE UPTD ON tPermit.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		ALTER TABLE Profisee.data.tPermit CHECK CONSTRAINT ALL;


		-- Updates Address fiels from PIN to NULL
		UPDATE PIN
		SET PIN.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,PIN.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,PIN.LastModifiedDateUTC = @StartDate
		FROM dbo.PIN PIN
		INNER JOIN #BC_UPTO_DATE UPTD ON PIN.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		--Update in Invalid and re-process
		UPDATE PIN_Invalid
		SET PIN_Invalid.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,PIN_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,PIN_Invalid.IsPermanentlyInvalid=0
			,PIN_Invalid.ReProcess=1
			--,PIN_Invalid.IsDuplicate=0
			,PIN_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.PIN_Invalid PIN_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON PIN_Invalid.Code = UPTD.Code
		WHERE UPTD.LegalDescriptions_LegalDescription_PID IS NOT NULL
		AND (UPTD.ID >= @fromBatchId AND UPTD.ID <= @toBatchId);


		ALTER TABLE Profisee.data.tPIN NOCHECK CONSTRAINT ALL;
		UPDATE tPIN
		SET tPIN.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,tPIN.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,tPIN.MatchKeyMember = ISNULL(PIN,'')+':'+ISNULL(ProvinceCode,'')
			,tPIN.[Match Group] = NULL
			,tPIN.[Match Score] = NULL
			,tPIN.[Match Status] = NULL
			,tPIN.[Record Source] = NULL
			,tPIN.[Match Member] = NULL
			,tPIN.[Match Strategy] = NULL
			,tPIN.[Match DateTime] = NULL
			,tPIN.[Match User] = NULL
			,tPIN.[Match MultiGroup] = NULL
			,tPIN.[Master] = NULL
			,tPIN.[Approved Count] = NULL
			,tPIN.[Proposed Count] = NULL
			,tPIN.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tPIN tPIN
		INNER JOIN #BC_UPTO_DATE UPTD ON tPIN.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		ALTER TABLE Profisee.data.tPIN CHECK CONSTRAINT ALL;

		-- Updates Address fiels from Property to NULL
		UPDATE Property
		SET Property.ARN = UPTD.RollNumber
			,Property.JurCode = UPTD.JurisdictionCode
			,Property.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Property.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Property.LastModifiedDateUTC = @StartDate
			--,MasterAddressID = NULL
		FROM dbo.Property Property
		INNER JOIN #BC_UPTO_DATE UPTD ON Property.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);


		--Update in Invalid and re-process

		UPDATE Property_Invalid
		SET Property_Invalid.ARN = UPTD.RollNumber
			,Property_Invalid.JurCode = UPTD.JurisdictionCode
			,Property_Invalid.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Property_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Property_Invalid.IsPermanentlyInvalid=0
			,Property_Invalid.ReProcess=1
			--,Property_Invalid.IsDuplicate=0
			,Property_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Property_Invalid Property_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Property_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		UPDATE Property_Invalid
		SET Property_Invalid.IsPermanentlyInvalid=0
			,Property_Invalid.ReProcess=1
			--,Property_Invalid.IsDuplicate=0
			,Property_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Property_Invalid Property_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Property_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;
		--WHERE UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;

		UPDATE Property_Invalid
		SET Property_Invalid.IsPermanentlyInvalid=0
			,Property_Invalid.ReProcess=1
			--,Property_Invalid.IsDuplicate=0
			,Property_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Property_Invalid Property_Invalid
		INNER JOIN dbo.PIN_Invalid PIN_Invalid ON Property_Invalid.Code = PIN_Invalid.Code
		WHERE PIN_Invalid.IsPermanentlyInvalid=0
		AND PIN_Invalid.ReProcess=1;
		--WHERE UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;


		-- Updates Address fiels from Taxation to NULL
		UPDATE Taxation
		SET Taxation.ARN = UPTD.RollNumber
			,Taxation.AssessmentYear = UPTD.RollYear
			,Taxation.JurCode = UPTD.JurisdictionCode
			,Taxation.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Taxation.LastModifiedDateUTC = @StartDate
		FROM dbo.Taxation Taxation
		INNER JOIN #BC_UPTO_DATE UPTD ON Taxation.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		ALTER TABLE Profisee.data.tTaxation NOCHECK CONSTRAINT ALL;
		UPDATE tTaxation
		SET tTaxation.ARN = UPTD.RollNumber
			,tTaxation.AssessmentYear = UPTD.RollYear
			,tTaxation.JurCode = UPTD.JurisdictionCode
			,tTaxation.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,tTaxation.[Match Group] = NULL
			,tTaxation.[Match Score] = NULL
			,tTaxation.[Match Status] = NULL
			,tTaxation.[Record Source] = NULL
			,tTaxation.[Match Member] = NULL
			,tTaxation.[Match Strategy] = NULL
			,tTaxation.[Match DateTime] = NULL
			,tTaxation.[Match User] = NULL
			,tTaxation.[Match MultiGroup] = NULL
			,tTaxation.[Master] = NULL
			,tTaxation.[Approved Count] = NULL
			,tTaxation.[Proposed Count] = NULL
			,tTaxation.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tTaxation tTaxation
		INNER JOIN #BC_UPTO_DATE UPTD ON tTaxation.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		ALTER TABLE Profisee.data.tTaxation CHECK CONSTRAINT ALL;

		-- Updates Address fiels from Valuation to NULL
		UPDATE Valuation
		SET Valuation.ARN = UPTD.RollNumber
			,Valuation.JurCode = UPTD.JurisdictionCode
			,Valuation.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Valuation.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Valuation.LastModifiedDateUTC = @StartDate
			--,MasterAddressID = NULL
		FROM dbo.Valuation Valuation
		INNER JOIN #BC_UPTO_DATE UPTD ON Valuation.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		--Update in Invalid and re-process

		UPDATE Valuation_Invalid
		SET Valuation_Invalid.IsPermanentlyInvalid=0
			,Valuation_Invalid.ReProcess=1
			--,Valuation_Invalid.IsDuplicate=0
			,Valuation_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Valuation_Invalid Valuation_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Valuation_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;

		UPDATE Valuation_Invalid
		SET Valuation_Invalid.ARN = UPTD.RollNumber
			,Valuation_Invalid.JurCode = UPTD.JurisdictionCode
			,Valuation_Invalid.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Valuation_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Valuation_Invalid.IsPermanentlyInvalid=0
			,Valuation_Invalid.ReProcess=1
			--,Valuation_Invalid.IsDuplicate=0
			,Valuation_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Valuation_Invalid Valuation_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Valuation_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		UPDATE Valuation_Invalid
		SET Valuation_Invalid.IsPermanentlyInvalid=0
			,Valuation_Invalid.ReProcess=1
			--,Valuation_Invalid.IsDuplicate=0
			,Valuation_Invalid.LastModifiedDateUTC=@StartDate
		FROM dbo.Valuation_Invalid Valuation_Invalid
		INNER JOIN dbo.PIN_Invalid PIN_Invalid ON Valuation_Invalid.Code = PIN_Invalid.Code
		WHERE Valuation_Invalid.IsPermanentlyInvalid=0
		AND Valuation_Invalid.ReProcess=1;
		--WHERE UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;


		UPDATE tValuation
		SET tValuation.ARN = UPTD.RollNumber
			,tValuation.JurCode = UPTD.JurisdictionCode
			,tValuation.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,tValuation.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			--,tValuation.MasterAddressID = NULL
			,tValuation.LastModifiedDateUTC = @StartDate
		FROM profisee.data.tValuation tValuation
		INNER JOIN #BC_UPTO_DATE UPTD ON tValuation.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		-- Updates Address fiels from Listing to NULL
		UPDATE Listing
		SET Listing.ARN = UPTD.RollNumber
			,Listing.DateEnd = UPTD.Sales_Sale_ConveyanceDate
			,Listing.JurCode = UPTD.JurisdictionCode
			,Listing.OwnershipType = UPTD.TenureDescription
			,Listing.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Listing.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Listing.LastModifiedDateUTC = @StartDate
			--,MasterAddressID = NULL
		FROM dbo.Listing Listing
		INNER JOIN #BC_UPTO_DATE UPTD ON Listing.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);


		--Update in Invalid and re-process

		UPDATE Listing_Invalid
		SET Listing_Invalid.ARN = UPTD.RollNumber
			,Listing_Invalid.DateEnd = UPTD.Sales_Sale_ConveyanceDate
			,Listing_Invalid.JurCode = UPTD.JurisdictionCode
			,Listing_Invalid.OwnershipType = UPTD.TenureDescription
			,Listing_Invalid.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,Listing_Invalid.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Listing_Invalid.LastModifiedDateUTC = @StartDate
			,Listing_Invalid.IsPermanentlyInvalid=0
			,Listing_Invalid.ReProcess=1
			--,Listing_Invalid.IsDuplicate=0
		FROM dbo.Listing_Invalid Listing_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Listing_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		UPDATE Listing_Invalid
		SET Listing_Invalid.LastModifiedDateUTC = @StartDate
			,Listing_Invalid.IsPermanentlyInvalid=0
			,Listing_Invalid.ReProcess=1
			--,Listing_Invalid.IsDuplicate=0
		FROM dbo.Listing_Invalid Listing_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Listing_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;
		--WHERE UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;

		UPDATE Listing_Invalid
		SET Listing_Invalid.LastModifiedDateUTC = @StartDate
			,Listing_Invalid.IsPermanentlyInvalid=0
			,Listing_Invalid.ReProcess=1
			--,Listing_Invalid.IsDuplicate=0
		FROM dbo.Listing_Invalid Listing_Invalid
		INNER JOIN dbo.PIN_Invalid PIN_Invalid ON Listing_Invalid.Code = PIN_Invalid.Code
		WHERE Listing_Invalid.IsPermanentlyInvalid=0
		AND Listing_Invalid.ReProcess=1;
		--AND UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;


		UPDATE tListing
		SET tListing.ARN = UPTD.RollNumber
			,tListing.DateEnd = UPTD.Sales_Sale_ConveyanceDate
			,tListing.JurCode = UPTD.JurisdictionCode
			,tListing.OwnershipType = UPTD.TenureDescription
			,tListing.PIN = UPTD.LegalDescriptions_LegalDescription_PID
			,tListing.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,tListing.LastModifiedDateUTC = @StartDate
		FROM Profisee.data.tListing tListing
		INNER JOIN #BC_UPTO_DATE UPTD ON tListing.Code = UPTD.Code
		--WHERE UPTD.LastModifiedDateUTC>@BC_UPTD_LastLoadDate;

		UPDATE Sales
		SET Sales.ClosingDate = UPTD.Sales_Sale_ConveyanceDate
			,Sales.PriceSold = UPTD.Sales_Sale_ConveyancePrice
			,Sales.LastModifiedDateUTC = @StartDate
		FROM dbo.Sales Sales
		INNER JOIN #BC_UPTO_DATE UPTD ON Sales.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);


		--Update in Invalid and re-process

		UPDATE Sales_Invalid
		SET Sales_Invalid.ClosingDate = UPTD.Sales_Sale_ConveyanceDate
			,Sales_Invalid.PriceSold = UPTD.Sales_Sale_ConveyancePrice
			,Sales_Invalid.IsPermanentlyInvalid = 0
			,Sales_Invalid.ReProcess=1
			--,Sales_Invalid.IsDuplicate=0
			,Sales_Invalid.LastModifiedDateUTC = @StartDate
		FROM dbo.Sales_Invalid Sales_Invalid
		INNER JOIN #BC_UPTO_DATE UPTD ON Sales_Invalid.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		UPDATE Sales_Invalid
		SET Sales_Invalid.LastModifiedDateUTC = @StartDate
			,Sales_Invalid.IsPermanentlyInvalid = 0
			,Sales_Invalid.ReProcess=1
			--,Sales_Invalid.IsDuplicate=0
		FROM dbo.Sales_Invalid Sales_Invalid
		INNER JOIN dbo.Address_Invalid Address_Invalid ON Sales_Invalid.Code = Address_Invalid.Code
		WHERE Address_Invalid.IsPermanentlyInvalid=0
		AND Address_Invalid.ReProcess=1;


		UPDATE tSales
		SET tSales.ClosingDate = UPTD.Sales_Sale_ConveyanceDate
			,tSales.PriceSold = UPTD.Sales_Sale_ConveyancePrice
			,tSales.LastModifiedDateUTC = @StartDate
		FROM profisee.data.tSales tSales
		INNER JOIN #BC_UPTO_DATE UPTD ON tSales.Code = UPTD.Code
		WHERE (UPTD.ID1 >= @fromBatchId AND UPTD.ID1 <= @toBatchId);

		SET @fromBatchId = @fromBatchId + @incrmt
		SET @toBatchId = @toBatchId + @incrmt
	END

		-- Update MasterAddressID in entities

		UPDATE tBuilding
		SET tBuilding.MasterAddressID = tAddress.MasterAddressID
			,tBuilding.MatchKeyMember = ISNULL(tAddress.MasterAddressID,'')+':'+ISNULL(tBuilding.PIN,'')+':'+ISNULL(tBuilding.ProvinceCode,'')
		FROM Profisee.data.tBuilding tBuilding
		INNER JOIN Profisee.data.tAddress tAddress ON tBuilding.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;
		
		UPDATE tBusiness
		SET tBusiness.MasterAddressID = tAddress.MasterAddressID
		FROM Profisee.data.tBusiness tBusiness
		INNER JOIN Profisee.data.tAddress tAddress ON tBusiness.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;

		UPDATE tParcel
		SET tParcel.MasterAddressID = tAddress.MasterAddressID
			,tParcel.MatchKeyMember = ISNULL(tAddress.MasterAddressID,'')+':'+ISNULL(tParcel.PIN,'')+':'+ISNULL(tParcel.ProvinceCode,'')+':'+ISNULL(tParcel.Sequence ,'')
		FROM Profisee.data.tParcel tParcel
		INNER JOIN Profisee.data.tAddress tAddress ON tParcel.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;

		UPDATE tPermit
		SET tPermit.MasterAddressID = tAddress.MasterAddressID
			,tPermit.MatchKeyMember = ISNULL(tAddress.MasterAddressID,'')+':'+ISNULL(tPermit.ARN,'')+':'+ISNULL(tPermit.JurCode,'')
		FROM Profisee.data.tPermit tPermit
		INNER JOIN Profisee.data.tAddress tAddress ON tPermit.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;

		UPDATE tValuation
		SET tValuation.MasterAddressID = tAddress.MasterAddressID
		FROM Profisee.data.tValuation tValuation
		INNER JOIN Profisee.data.tAddress tAddress ON tValuation.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;

		UPDATE tListing
		SET tListing.MasterAddressID = tAddress.MasterAddressID
		FROM Profisee.data.tListing tListing
		INNER JOIN Profisee.data.tAddress tAddress ON tListing.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;
		
		UPDATE tSales
		SET tSales.MasterAddressID = tAddress.MasterAddressID
		FROM Profisee.data.tSales tSales
		INNER JOIN Profisee.data.tAddress tAddress ON tSales.Code = tAddress.Code
		WHERE tAddress.LastModifiedDateUTC = @StartDate;

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