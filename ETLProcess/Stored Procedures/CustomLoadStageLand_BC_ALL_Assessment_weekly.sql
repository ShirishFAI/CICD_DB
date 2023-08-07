



/****************************************************************************************      
 AUTHOR  : Shirish W.      
 DATE   : 10/15/2022      
 PURPOSE  : BCA Weekly Source File - Load to StageLanding.      
 DEPENDENCIES :       
      
 VERSION HISTORY:      
** ----------------------------------------------------------------------------------------      
** 02/18/2022 Shirish W. Original Version      
******************************************************************************************/  
CREATE PROCEDURE [ETLProcess].[CustomLoadStageLand_BC_ALL_Assessment_weekly]
	@ExternalFileName VARCHAR(500)   
AS
BEGIN
	
	SET NOCOUNT ON;
	DECLARE @Params NVARCHAR(500)='@ExternalFileName VARCHAR(500),@ExternalDataSourceName VARCHAR(100)';
	DECLARE @DynamicSQL NVARCHAR(MAX)='';                          
    DECLARE @ExternalDataSourceName VARCHAR(100)='DTCDataSetExternal';                                    
	--declare @ExternalFileName VARCHAR(500) ='20220102_WEEKLY_0156.xml'
      
	DROP TABLE IF EXISTS #DataAdvice;
	DROP TABLE IF EXISTS #FolioRecordsLegalDescription;
	DROP TABLE IF EXISTS #FolioRecordsSchoolDistrict;
	DROP TABLE IF EXISTS #FolioRecordsSales;
	DROP TABLE IF EXISTS #FolioRecordsLandMesaurement;
	DROP TABLE IF EXISTS #RegionalDistrict;
	DROP TABLE IF EXISTS #FolioRecordsAddresses;
	DROP TABLE IF EXISTS #FolioRecordsNeighbourhood;
	DROP TABLE IF EXISTS #FolioRecords;
	DROP TABLE IF EXISTS #FolioDescription;
	DROP TABLE IF EXISTS #FolioActionFolioAdd;
	DROP TABLE IF EXISTS #FolioActionFolioDelete;
	

	  IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_NAME = N'BCAWeeklyXML' and TABLE_SCHEMA=N'StageLanding')
		BEGIN
		  TRUNCATE TABLE StageLanding.BCAWeeklyXML
		END
		
	 
		SET @DynamicSQL = @DynamicSQL+   
		'INSERT INTO StageLanding.BCAWeeklyXML
		(XMLData, LoadedDateTime)
		SELECT BulkColumn, GETDATE()
		FROM OPENROWSET 
	   (
		 BULK  '''+@ExternalFileName+''''+'
		,SINGLE_BLOB
		,DATA_SOURCE =''' + @ExternalDataSourceName +''''+'
	   ) AS DataFile;'

   --Select @DynamicSQL
		EXECUTE sp_executesql  @DynamicSQL,@Params,@ExternalFileName,@ExternalDataSourceName
	
	DECLARE @XML XML;
	DECLARE @docHandle INT;

	SET @XML = (SELECT XMLData FROM StageLanding.BCAWeeklyXML);

	--SELECT @XML

	EXEC sp_xml_preparedocument @docHandle OUTPUT, @XML;
	
-- Basic Data	
SELECT *
INTO #DataAdvice
FROM OPENXML(@docHandle, N'/DataAdvice') WITH (
		RunDate VARCHAR(500) '(@RunDate)'
		,StartDate VARCHAR(500) 'StartDate'
		,EndDate VARCHAR(500) 'EndDate'
		,RollYear VARCHAR(500) 'RollYear'
		,OwnershipYear VARCHAR(500) 'OwnershipYear'
		,RunType VARCHAR(500) 'RunType'
		);

--RegionalDistrict Data
SELECT *
INTO #RegionalDistrict
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/RegionalDistrict') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'
		,RegionalDistrict_DistrictDescription VARCHAR(500) 'DistrictDescription[1]'
		,RegionalDistrict_DistrictDescription_Action VARCHAR(500) 'DistrictDescription[1]/@Action'
		,RegionalDistrict_DistrictDescription_OldValue VARCHAR(500) 'DistrictDescription[1]/@OldValue'
		);

--LegalDescription Data
SELECT *
INTO #FolioRecordsLegalDescription
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/LegalDescriptions/LegalDescription') WITH (
		FolioRecord_ID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'

		,LegalDescriptions_LegalDescription_ID VARCHAR(500) '@ID'

		,LegalDescriptions_LegalDescription_Action VARCHAR(500) '@Action'

		,LegalDescriptions_LegalDescription_Block VARCHAR(500) 'Block[1]'
		,LegalDescriptions_LegalDescription_Block_Action VARCHAR(500) 'Block[1]/@Action'
		,LegalDescriptions_LegalDescription_Block_OldValue VARCHAR(500) 'Block[1]/@OldValue'

		,LegalDescriptions_LegalDescription_DistrictLot VARCHAR(500) 'DistrictLot[1]'
		,LegalDescriptions_LegalDescription_DistrictLot_Action VARCHAR(500) 'DistrictLot[1]/@Action'
		,LegalDescriptions_LegalDescription_DistrictLot_OldValue VARCHAR(500) 'DistrictLot[1]/@OldValue'

		,LegalDescriptions_LegalDescription_ExceptPlan VARCHAR(500) 'ExceptPlan[1]'
		,LegalDescriptions_LegalDescription_ExceptPlan_Action VARCHAR(500) 'ExceptPlan[1]/@Action'
		,LegalDescriptions_LegalDescription_ExceptPlan_OldValue VARCHAR(500) 'ExceptPlan[1]/@OldValue'

		,LegalDescriptions_LegalDescription_FormattedLegalDescription VARCHAR(MAX) 'FormattedLegalDescription[1]'
		,LegalDescriptions_LegalDescription_FormattedLegalDescription_Action VARCHAR(500) 'FormattedLegalDescription[1]/@Action'
		,LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue VARCHAR(MAX) 'FormattedLegalDescription[1]/@OldValue'

		,LegalDescriptions_LegalDescription_LandDistrict VARCHAR(500) 'LandDistrict[1]'
		,LegalDescriptions_LegalDescription_LandDistrict_Action VARCHAR(500) 'LandDistrict[1]/@Action'
		,LegalDescriptions_LegalDescription_LandDistrict_OldValue VARCHAR(500) 'LandDistrict[1]/@OldValue'

		,LegalDescriptions_LegalDescription_LandDistrictDescription VARCHAR(500) 'LandDistrictDescription[1]'
		,LegalDescriptions_LegalDescription_LandDistrictDescription_Action VARCHAR(500) 'LandDistrictDescription[1]/@Action'
		,LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue VARCHAR(500) 'LandDistrictDescription[1]/@OldValue'

		,LegalDescriptions_LegalDescription_LeaseLicenceNumber VARCHAR(500) 'LeaseLicenceNumber[1]'
		,LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action VARCHAR(500) 'LeaseLicenceNumber[1]/@Action'
		,LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue VARCHAR(500) 'LeaseLicenceNumber[1]/@OldValue'

		,LegalDescriptions_LegalDescription_LegalText VARCHAR(MAX) 'LegalText[1]'
		,LegalDescriptions_LegalDescription_LegalText_Action VARCHAR(500) 'LegalText[1]/@Action'
		,LegalDescriptions_LegalDescription_LegalText_OldValue VARCHAR(MAX) 'LegalText[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Lot VARCHAR(500) 'Lot[1]'
		,LegalDescriptions_LegalDescription_Lot_Action VARCHAR(500) 'Lot[1]/@Action'
		,LegalDescriptions_LegalDescription_Lot_OldValue VARCHAR(500) 'Lot[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Meridian VARCHAR(500) 'Meridian[1]'
		,LegalDescriptions_LegalDescription_Meridian_Action VARCHAR(500) 'Meridian[1]/@Action'
		,LegalDescriptions_LegalDescription_Meridian_OldValue VARCHAR(500) 'Meridian[1]/@OldValue'

		,LegalDescriptions_LegalDescription_MeridianShort VARCHAR(500) 'MeridianShort[1]'
		,LegalDescriptions_LegalDescription_MeridianShort_Action VARCHAR(500) 'MeridianShort[1]/@Action'
		,LegalDescriptions_LegalDescription_MeridianShort_OldValue VARCHAR(500) 'MeridianShort[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Parcel VARCHAR(MAX) 'Parcel[1]'
		,LegalDescriptions_LegalDescription_Parcel_Action VARCHAR(500) 'Parcel[1]/@Action'
		,LegalDescriptions_LegalDescription_Parcel_OldValue VARCHAR(MAX) 'Parcel[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Part1 VARCHAR(MAX) 'Part1[1]'
		,LegalDescriptions_LegalDescription_Part1_Action VARCHAR(500) 'Part1[1]/@Action'
		,LegalDescriptions_LegalDescription_Part1_OldValue VARCHAR(MAX) 'Part1[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Part2 VARCHAR(500) 'Part2[1]'
		,LegalDescriptions_LegalDescription_Part2_Action VARCHAR(500) 'Part2[1]/@Action'
		,LegalDescriptions_LegalDescription_Part2_OldValue VARCHAR(500) 'Part2[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Part3 VARCHAR(500) 'Part3[1]'
		,LegalDescriptions_LegalDescription_Part3_Action VARCHAR(500) 'Part3[1]/@Action'
		,LegalDescriptions_LegalDescription_Part3_OldValue VARCHAR(500) 'Part3[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Part4 VARCHAR(500) 'Part4[1]'
		,LegalDescriptions_LegalDescription_Part4_Action VARCHAR(500) 'Part4[1]/@Action'
		,LegalDescriptions_LegalDescription_Part4_OldValue VARCHAR(500) 'Part4[1]/@OldValue'

		,LegalDescriptions_LegalDescription_PID VARCHAR(500) 'PID[1]'
		,LegalDescriptions_LegalDescription_PID_Action VARCHAR(500) 'PID[1]/@Action'
		,LegalDescriptions_LegalDescription_PID_OldValue VARCHAR(500) 'PID[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Plan VARCHAR(500) 'Plan[1]'
		,LegalDescriptions_LegalDescription_Plan_Action VARCHAR(500) 'Plan[1]/@Action'
		,LegalDescriptions_LegalDescription_Plan_OldValue VARCHAR(500) 'Plan[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Portion VARCHAR(500) 'Portion[1]'
		,LegalDescriptions_LegalDescription_Portion_Action VARCHAR(500) 'Portion[1]/@Action'
		,LegalDescriptions_LegalDescription_Portion_OldValue VARCHAR(500) 'Portion[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Range VARCHAR(500) 'Range[1]'
		,LegalDescriptions_LegalDescription_Range_Action VARCHAR(500) 'Range[1]/@Action'
		,LegalDescriptions_LegalDescription_Range_OldValue VARCHAR(500) 'Range[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Section VARCHAR(500) 'Section[1]'
		,LegalDescriptions_LegalDescription_Section_Action VARCHAR(500) 'Section[1]/@Action'
		,LegalDescriptions_LegalDescription_Section_OldValue VARCHAR(500) 'Section[1]/@OldValue'

		,LegalDescriptions_LegalDescription_StrataLot VARCHAR(500) 'StrataLot[1]'
		,LegalDescriptions_LegalDescription_StrataLot_Action VARCHAR(500) 'StrataLot[1]/@Action'
		,LegalDescriptions_LegalDescription_StrataLot_OldValue VARCHAR(500) 'StrataLot[1]/@OldValue'

		,LegalDescriptions_LegalDescription_SubBlock VARCHAR(500) 'SubBlock[1]'
		,LegalDescriptions_LegalDescription_SubBlock_Action VARCHAR(500) 'SubBlock[1]/@Action'
		,LegalDescriptions_LegalDescription_SubBlock_OldValue VARCHAR(500) 'SubBlock[1]/@OldValue'

		,LegalDescriptions_LegalDescription_SubLot VARCHAR(500) 'SubLot[1]'
		,LegalDescriptions_LegalDescription_SubLot_Action VARCHAR(500) 'SubLot[1]/@Action'
		,LegalDescriptions_LegalDescription_SubLot_OldValue VARCHAR(500) 'SubLot[1]/@OldValue'

		,LegalDescriptions_LegalDescription_Township VARCHAR(500) 'Township[1]'
		,LegalDescriptions_LegalDescription_Township_Action VARCHAR(500) 'Township[1]/@Action'
		,LegalDescriptions_LegalDescription_Township_OldValue VARCHAR(500) 'Township[1]/@OldValue'

		,LegalDescriptions_LegalDescription_LegalSubdivision VARCHAR(500) 'LegalSubdivision[1]'
		,LegalDescriptions_LegalDescription_LegalSubdivision_Action VARCHAR(500) 'LegalSubdivision[1]/@Action'
		,LegalDescriptions_LegalDescription_LegalSubdivision_OldValue VARCHAR(500) 'LegalSubdivision[1]/@OldValue'
		);
	

--School District Data
SELECT *
INTO #FolioRecordsSchoolDistrict
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/SchoolDistrict') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'

		,SchoolDistrict_DistrictDescription VARCHAR(500) 'DistrictDescription[1]'
		,SchoolDistrict_DistrictDescription_Action VARCHAR(500) 'DistrictDescription[1]/@Action'
		,SchoolDistrict_DistrictDescription_OldValue VARCHAR(500) 'DistrictDescription[1]/@OldValue'
		);


--Sales Data
SELECT *
INTO #FolioRecordsSales
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/Sales/Sale') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'

		,Sales_Sale_ID VARCHAR(500) '@ID'
		,Sales_Sale_Action VARCHAR(500) '@Action'

		,Sales_Sale_ConveyanceDate VARCHAR(500) 'ConveyanceDate[1]'
		,Sale_ConveyanceDate_Action VARCHAR(500) 'ConveyanceDate[1]/@Action'
		,Sale_ConveyanceDate_OldValue VARCHAR(500) 'ConveyanceDate[1]/@OldValue'

		,Sales_Sale_ConveyancePrice VARCHAR(500) 'ConveyancePrice[1]'
		,Sales_Sale_ConveyancePrice_Action VARCHAR(500) 'ConveyancePrice[1]/@Action'
		,Sales_Sale_ConveyancePrice_OldValue VARCHAR(500) 'ConveyancePrice[1]/@OldValue'
		);
	
	
--Land Measurement Data
SELECT FolioRecordID
	,RollNumber
	,CASE 
		WHEN LTRIM(RTRIM(ISNULL(LandMeasurement_LandDepth, ''))) = '' THEN NULL ELSE LandMeasurement_LandDepth
	 END AS LandMeasurement_LandDepth
	,LandMeasurement_LandDepth_Action
	,LandMeasurement_LandDepth_OldValue
	,CASE 
		WHEN LTRIM(RTRIM(ISNULL(LandMeasurement_LandDimension,''))) = '' THEN NULL ELSE LandMeasurement_LandDimension
	 END AS LandMeasurement_LandDimension
	,LandMeasurement_LandDimension_Action
	,LandMeasurement_LandDimension_OldValue
	,LandMeasurement_LandDimensionTypeDescription
	,LandMeasurement_LandDimensionTypeDescription_Action
	,LandMeasurement_LandDimensionTypeDescription_OldValue
	,CASE 
		WHEN LTRIM(RTRIM(LandMeasurement_LandWidth)) = '' THEN NULL ELSE LandMeasurement_LandWidth
	 END AS LandMeasurement_LandWidth
	,LandMeasurement_LandWidth_Action
	,LandMeasurement_LandWidth_OldValue
INTO #FolioRecordsLandMesaurement
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/LandMeasurement') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'

		,LandMeasurement_LandDepth VARCHAR(500) 'LandDepth[1]'
		,LandMeasurement_LandDepth_Action VARCHAR(500) 'LandDepth[1]/@Action'
		,LandMeasurement_LandDepth_OldValue VARCHAR(500) 'LandDepth[1]/@OldValue'

		,LandMeasurement_LandDimension VARCHAR(500) 'LandDimension[1]'
		,LandMeasurement_LandDimension_Action VARCHAR(500) 'LandDimension[1]/@Action'
		,LandMeasurement_LandDimension_OldValue VARCHAR(500) 'LandDimension[1]/@OldValue'

		,LandMeasurement_LandDimensionTypeDescription VARCHAR(500) 'LandDimensionTypeDescription[1]'
		,LandMeasurement_LandDimensionTypeDescription_Action VARCHAR(500) 'LandDimensionTypeDescription[1]/@Action'
		,LandMeasurement_LandDimensionTypeDescription_OldValue VARCHAR(500) 'LandDimensionTypeDescription[1]/@OldValue'

		,LandMeasurement_LandWidth VARCHAR(500) 'LandWidth[1]'
		,LandMeasurement_LandWidth_Action VARCHAR(500) 'LandWidth[1]/@Action'
		,LandMeasurement_LandWidth_OldValue VARCHAR(500) 'LandWidth[1]/@OldValue'
		);
	
	
--Address Data
SELECT *
INTO #FolioRecordsAddresses
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAddresses/FolioAddress') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'

		,FolioAddress_ID VARCHAR(500) '@ID'
		,FolioAddress_Action VARCHAR(500) '@Action'

		,FolioAddresses_FolioAddress_City VARCHAR(500) 'City[1]'
		,FolioAddresses_FolioAddress_City_Action VARCHAR(500) 'City[1]/@Action'
		,FolioAddresses_FolioAddress_City_OldValue VARCHAR(500) 'City[1]/@OldValue'

		,FolioAddresses_FolioAddress_PostalZip VARCHAR(500) 'PostalZip[1]'
		,FolioAddresses_FolioAddress_PostalZip_Action VARCHAR(500) 'PostalZip[1]/@Action'
		,FolioAddresses_FolioAddress_PostalZip_OldValue VARCHAR(500) 'PostalZip[1]/@OldValue'

		,FolioAddresses_FolioAddress_PrimaryFlag VARCHAR(500) 'PrimaryFlag[1]'
		,FolioAddresses_FolioAddress_PrimaryFlag_Action VARCHAR(500) 'PrimaryFlag[1]/@Action'
		,FolioAddresses_FolioAddress_PrimaryFlag_OldValue VARCHAR(500) 'PrimaryFlag[1]/@OldValue'

		,FolioAddresses_FolioAddress_ProvinceState VARCHAR(500) 'ProvinceState[1]'
		,FolioAddresses_FolioAddress_ProvinceState_Action VARCHAR(500) 'ProvinceState[1]/@Action'
		,FolioAddresses_FolioAddress_ProvinceState_OldValue VARCHAR(500) 'ProvinceState[1]/@OldValue'

		,FolioAddresses_FolioAddress_StreetDirectionSuffix VARCHAR(500) 'StreetDirectionSuffix[1]'
		,FolioAddresses_FolioAddress_StreetDirectionSuffix_Action VARCHAR(500) 'StreetDirectionSuffix[1]/@Action'
		,FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue VARCHAR(500) 'StreetDirectionSuffix[1]/@OldValue'

		,FolioAddresses_FolioAddress_StreetName VARCHAR(500) 'StreetName[1]'
		,FolioAddresses_FolioAddress_StreetName_Action VARCHAR(500) 'StreetName[1]/@Action'
		,FolioAddresses_FolioAddress_StreetName_OldValue VARCHAR(500) 'StreetName[1]/@OldValue'

		,FolioAddresses_FolioAddress_StreetNumber VARCHAR(500) 'StreetNumber[1]'
		,FolioAddresses_FolioAddress_StreetNumber_Action VARCHAR(500) 'StreetNumber[1]/@Action'
		,FolioAddresses_FolioAddress_StreetNumber_OldValue VARCHAR(500) 'StreetNumber[1]/@OldValue'

		,LandMeasurement_LandWidth VARCHAR(500) 'LandWidth[1]'
		,LandMeasurement_LandWidth_Action VARCHAR(500) 'LandWidth[1]/@Action'
		,LandMeasurement_LandWidth_OldValue VARCHAR(500) 'LandWidth[1]/@OldValue'

		,FolioAddresses_FolioAddress_StreetType VARCHAR(500) 'StreetType[1]'
		,FolioAddresses_FolioAddress_StreetType_Action VARCHAR(500) 'StreetType[1]/@Action'
		,FolioAddresses_FolioAddress_StreetType_OldValue VARCHAR(500) 'StreetType[1]/@OldValue'

		,FolioAddresses_FolioAddress_UnitNumber VARCHAR(500) 'UnitNumber[1]'
		,FolioAddresses_FolioAddress_UnitNumber_Action VARCHAR(500) 'UnitNumber[1]/@Action'
		,FolioAddresses_FolioAddress_UnitNumber_OldValue VARCHAR(500) 'UnitNumber[1]/@OldValue'
		);	

--Neighbourhood Data
SELECT *
INTO #FolioRecordsNeighbourhood
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/Neighbourhood/NeighbourhoodCode') WITH (
		FolioRecordID VARCHAR(500) '(../../../@ID)'
		,RollNumber VARCHAR(500) '(../../../RollNumber[1])'

		,FolioDescription_Neighbourhood_NeighbourhoodCode VARCHAR(500) '../NeighbourhoodCode[1]'
		,FolioDescription_Neighbourhood_NeighbourhoodCode_Action VARCHAR(500) '@Action'
		,FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue VARCHAR(500) '@OldValue'

		,Neighbourhood_NeighbourhoodDescription VARCHAR(500) '../NeighbourhoodDescription[1]'
		,FolioDescription_Neighbourhood_NeighbourhoodDescription_Action VARCHAR(500) '../NeighbourhoodDescription[1]/@Action'
		,FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue VARCHAR(500) '../NeighbourhoodDescription[1]/@OldValue'
		);
	

--FolioRecords Parent Data
SELECT *
INTO #FolioRecords
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord') WITH (
		FolioRecordID VARCHAR(500) '@ID'
		,RollNumber VARCHAR(500) 'RollNumber[1]'

		,RollNumber_Action VARCHAR(500) 'RollNumber[1]/@Action'
		,RollNumber_OldValue VARCHAR(500) 'RollNumber[1]/@OldValue'

		,FolioRecords_FolioStatus VARCHAR(500) 'FolioStatus[1]'
		,FolioRecords_FolioStatusDescription VARCHAR(500) 'FolioStatusDescription[1]'

		,JurisdictionCode VARCHAR(500) '../../JurisdictionCode[1]'
		,JurisdictionDescription VARCHAR(500) '../../JurisdictionDescription[1]'

		,AssessmentAreaCode VARCHAR(500) '../../../../AssessmentAreaCode[1]'
		,AssessmentAreaDescription VARCHAR(500) '../../../../AssessmentAreaDescription[1]'
		);	


--FolioDescription Data
SELECT *
INTO #FolioDescription
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription') WITH (
		FolioRecordID VARCHAR(500) '(../@ID)'
		,RollNumber VARCHAR(500) '(../RollNumber[1])'

		,FolioDescription_Action VARCHAR(500) '@Action'

		,ActualUseDescription VARCHAR(500) 'ActualUseDescription[1]'
		,ActualUseDescription_Action VARCHAR(500) 'ActualUseDescription[1]/@Action'
		,ActualUseDescription_OldValue VARCHAR(500) 'ActualUseDescription[1]/@OldValue'

		,TenureDescription VARCHAR(500) 'TenureDescription[1]'
		,TenureDescription_Action VARCHAR(500) 'TenureDescription[1]/@Action'
		,TenureDescription_OldValue VARCHAR(500) 'TenureDescription[1]/@OldValue'

		,VacantFlag VARCHAR(500) 'VacantFlag[1]'
		,VacantFlag_Action VARCHAR(500) 'VacantFlag[1]/@Action'
		,VacantFlag_OldValue VARCHAR(500) 'VacantFlag[1]/@OldValue'
		);	


--Added Folios Data
SELECT *,'1' AS FolioAction_FolioAdd
INTO #FolioActionFolioAdd
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioAdd') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'
		);	

--Deleted Folios Data
SELECT *,'1' AS FolioAction_FolioDelete
INTO #FolioActionFolioDelete
FROM OPENXML(@docHandle, N'/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioDelete') WITH (
		FolioRecordID VARCHAR(500) '(../../@ID)'
		,RollNumber VARCHAR(500) '(../../RollNumber[1])'

		,FolioAction_FolioDelete_DeleteReasonCode VARCHAR(500) 'DeleteReasonCode[1]'
		,FolioAction_FolioDelete_DeleteReasonDescription VARCHAR(500) 'DeleteReasonDescription[1]'
		);
	
--************************************************************************************************************	

-- Insert All Records into Landing table
	INSERT INTO StageLanding.BC_ALL_Assessment_Weekly ( 
		[FolioRecord_ID]
		,[RunType]
		,[RollYear]
		,[OwnershipYear]
		,[StartDate]
		,[EndDate]
		,[RunDate]
		,[AssessmentAreaCode]
		,[AssessmentAreaDescription]
		,[JurisdictionCode]
		,[JurisdictionDescription]
		,[RollNumber]
		,[RollNumber_Action]
		,[RollNumber_OldValue]
		,[FolioAction_FolioAdd]
		,[FolioAction_FolioDelete]
		,[FolioAction_FolioDelete_DeleteReasonCode]
		,[FolioAction_FolioDelete_DeleteReasonDescription]
		,[FolioDescription_Action]
		,[ActualUseDescription]
		,[ActualUseDescription_Action]
		,[ActualUseDescription_OldValue]
		,[TenureDescription]
		,[TenureDescription_Action]
		,[TenureDescription_OldValue]
		,[VacantFlag]
		,[VacantFlag_Action]
		,[VacantFlag_OldValue]
		,[FolioAddresses_FolioAddress_Action]
		,[FolioAddresses_FolioAddress_City]
		,[FolioAddresses_FolioAddress_City_Action]
		,[FolioAddresses_FolioAddress_City_OldValue]
		,[FolioAddresses_FolioAddress_ID]
		,[FolioAddresses_FolioAddress_PostalZip]
		,[FolioAddresses_FolioAddress_PostalZip_Action]
		,[FolioAddresses_FolioAddress_PostalZip_OldValue]
		,[FolioAddresses_FolioAddress_PrimaryFlag]
		,[FolioAddresses_FolioAddress_PrimaryFlag_Action]
		,[FolioAddresses_FolioAddress_PrimaryFlag_OldValue]
		,[FolioAddresses_FolioAddress_ProvinceState]
		,[FolioAddresses_FolioAddress_ProvinceState_Action]
		,[FolioAddresses_FolioAddress_ProvinceState_OldValue]
		,[FolioAddresses_FolioAddress_StreetDirectionSuffix]
		,[FolioAddresses_FolioAddress_StreetDirectionSuffix_Action]
		,[FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue]
		,[FolioAddresses_FolioAddress_StreetName]
		,[FolioAddresses_FolioAddress_StreetName_Action]
		,[FolioAddresses_FolioAddress_StreetName_OldValue]
		,[FolioAddresses_FolioAddress_StreetNumber]
		,[FolioAddresses_FolioAddress_StreetNumber_Action]
		,[FolioAddresses_FolioAddress_StreetNumber_OldValue]
		,[FolioAddresses_FolioAddress_StreetType]
		,[FolioAddresses_FolioAddress_StreetType_Action]
		,[FolioAddresses_FolioAddress_StreetType_OldValue]
		,[FolioAddresses_FolioAddress_UnitNumber]
		,[FolioAddresses_FolioAddress_UnitNumber_Action]
		,[FolioAddresses_FolioAddress_UnitNumber_OldValue]
		,[LandMeasurement_LandDepth]
		,[LandMeasurement_LandDepth_Action]
		,[LandMeasurement_LandDepth_OldValue]
		,[LandMeasurement_LandDimension]
		,[LandMeasurement_LandDimension_Action]
		,[LandMeasurement_LandDimension_OldValue]
		,[LandMeasurement_LandDimensionTypeDescription]
		,[LandMeasurement_LandDimensionTypeDescription_Action]
		,[LandMeasurement_LandDimensionTypeDescription_OldValue]
		,[LandMeasurement_LandWidth]
		,[LandMeasurement_LandWidth_Action]
		,[LandMeasurement_LandWidth_OldValue]
		,[FolioDescription_Neighbourhood_NeighbourhoodCode]
		,[FolioDescription_Neighbourhood_NeighbourhoodCode_Action]
		,[FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue]
		,[FolioDescription_Neighbourhood_NeighbourhoodDescription]
		,[FolioDescription_Neighbourhood_NeighbourhoodDescription_Action]
		,[FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue]
		,[RegionalDistrict_DistrictDescription]
		,[RegionalDistrict_DistrictDescription_Action]
		,[RegionalDistrict_DistrictDescription_OldValue]
		,[SchoolDistrict_DistrictDescription]
		,[SchoolDistrict_DistrictDescription_Action]
		,[SchoolDistrict_DistrictDescription_OldValue]
		,[LegalDescriptions_LegalDescription_Action]
		,[LegalDescriptions_LegalDescription_Block]
		,[LegalDescriptions_LegalDescription_Block_Action]
		,[LegalDescriptions_LegalDescription_Block_OldValue]
		,[LegalDescriptions_LegalDescription_DistrictLot]
		,[LegalDescriptions_LegalDescription_DistrictLot_Action]
		,[LegalDescriptions_LegalDescription_DistrictLot_OldValue]
		,[LegalDescriptions_LegalDescription_ExceptPlan]
		,[LegalDescriptions_LegalDescription_ExceptPlan_Action]
		,[LegalDescriptions_LegalDescription_ExceptPlan_OldValue]
		,[LegalDescriptions_LegalDescription_FormattedLegalDescription]
		,[LegalDescriptions_LegalDescription_FormattedLegalDescription_Action]
		,[LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue]
		,[LegalDescriptions_LegalDescription_ID]
		,[LegalDescriptions_LegalDescription_LandDistrict]
		,[LegalDescriptions_LegalDescription_LandDistrict_Action]
		,[LegalDescriptions_LegalDescription_LandDistrict_OldValue]
		,[LegalDescriptions_LegalDescription_LandDistrictDescription]
		,[LegalDescriptions_LegalDescription_LandDistrictDescription_Action]
		,[LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue]
		,[LegalDescriptions_LegalDescription_LeaseLicenceNumber]
		,[LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action]
		,[LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue]
		,[LegalDescriptions_LegalDescription_LegalText]
		,[LegalDescriptions_LegalDescription_LegalText_Action]
		,[LegalDescriptions_LegalDescription_LegalText_OldValue]
		,[LegalDescriptions_LegalDescription_Lot]
		,[LegalDescriptions_LegalDescription_Lot_Action]
		,[LegalDescriptions_LegalDescription_Lot_OldValue]
		,[LegalDescriptions_LegalDescription_Meridian]
		,[LegalDescriptions_LegalDescription_Meridian_Action]
		,[LegalDescriptions_LegalDescription_Meridian_OldValue]
		,[LegalDescriptions_LegalDescription_MeridianShort]
		,[LegalDescriptions_LegalDescription_MeridianShort_Action]
		,[LegalDescriptions_LegalDescription_MeridianShort_OldValue]
		,[LegalDescriptions_LegalDescription_Parcel]
		,[LegalDescriptions_LegalDescription_Parcel_Action]
		,[LegalDescriptions_LegalDescription_Parcel_OldValue]
		,[LegalDescriptions_LegalDescription_Part1]
		,[LegalDescriptions_LegalDescription_Part1_Action]
		,[LegalDescriptions_LegalDescription_Part1_OldValue]
		,[LegalDescriptions_LegalDescription_Part2]
		,[LegalDescriptions_LegalDescription_Part2_Action]
		,[LegalDescriptions_LegalDescription_Part2_OldValue]
		,[LegalDescriptions_LegalDescription_Part3]
		,[LegalDescriptions_LegalDescription_Part3_Action]
		,[LegalDescriptions_LegalDescription_Part3_OldValue]
		,[LegalDescriptions_LegalDescription_Part4]
		,[LegalDescriptions_LegalDescription_Part4_Action]
		,[LegalDescriptions_LegalDescription_Part4_OldValue]
		,[LegalDescriptions_LegalDescription_PID]
		,[LegalDescriptions_LegalDescription_PID_Action]
		,[LegalDescriptions_LegalDescription_PID_OldValue]
		,[LegalDescriptions_LegalDescription_Plan]
		,[LegalDescriptions_LegalDescription_Plan_Action]
		,[LegalDescriptions_LegalDescription_Plan_OldValue]
		,[LegalDescriptions_LegalDescription_Portion]
		,[LegalDescriptions_LegalDescription_Portion_Action]
		,[LegalDescriptions_LegalDescription_Portion_OldValue]
		,[LegalDescriptions_LegalDescription_Range]
		,[LegalDescriptions_LegalDescription_Range_Action]
		,[LegalDescriptions_LegalDescription_Range_OldValue]
		,[LegalDescriptions_LegalDescription_Section]
		,[LegalDescriptions_LegalDescription_Section_Action]
		,[LegalDescriptions_LegalDescription_Section_OldValue]
		,[LegalDescriptions_LegalDescription_StrataLot]
		,[LegalDescriptions_LegalDescription_StrataLot_Action]
		,[LegalDescriptions_LegalDescription_StrataLot_OldValue]
		,[LegalDescriptions_LegalDescription_SubBlock]
		,[LegalDescriptions_LegalDescription_SubBlock_Action]
		,[LegalDescriptions_LegalDescription_SubBlock_OldValue]
		,[LegalDescriptions_LegalDescription_SubLot]
		,[LegalDescriptions_LegalDescription_SubLot_Action]
		,[LegalDescriptions_LegalDescription_SubLot_OldValue]
		,[LegalDescriptions_LegalDescription_Township]
		,[LegalDescriptions_LegalDescription_Township_Action]
		,[LegalDescriptions_LegalDescription_Township_OldValue]
		,[LegalDescriptions_LegalDescription_LegalSubdivision]
		,[LegalDescriptions_LegalDescription_LegalSubdivision_Action]
		,[LegalDescriptions_LegalDescription_LegalSubdivision_OldValue]
		,[Sales_Sale_Action]
		,[Sales_Sale_ConveyanceDate]
		,[Sales_Sale_ConveyanceDate_Action]
		,[Sales_Sale_ConveyanceDate_OldValue]
		,[Sales_Sale_ConveyancePrice]
		,[Sales_Sale_ConveyancePrice_Action]
		,[Sales_Sale_ConveyancePrice_OldValue]
		,[Sales_Sale_ID]
		)

	SELECT  distinct FR.FolioRecordID
		   , DataAdvice.RunType
		   , DataAdvice.RollYear
		   , DataAdvice.OwnershipYear
		   , DataAdvice.StartDate
		   , DataAdvice.EndDate
		   , DataAdvice.RunDate
		   , FR.AssessmentAreaCode
		   , FR.AssessmentAreaDescription
		   , FR.JurisdictionCode
		   , FR.JurisdictionDescription
		   , FR.RollNumber
		   , FR.RollNumber_Action
		   , FR.RollNumber_OldValue
		   , FAADD.FolioAction_FolioAdd 
		   , FAFD.FolioAction_FolioDelete 
		   , FAFD.FolioAction_FolioDelete_DeleteReasonCode
		   , FAFD.FolioAction_FolioDelete_DeleteReasonDescription
		   , FD.FolioDescription_Action
		   , FD.ActualUseDescription
		   , FD.ActualUseDescription_Action
		   , FD.ActualUseDescription_OldValue
		   , FD.TenureDescription
		   , FD.TenureDescription_Action
		   , FD.TenureDescription_OldValue
		   , FD.VacantFlag
		   , FD.VacantFlag_Action
		   , FD.VacantFlag_OldValue
		   , FA.FolioAddress_Action
		   , FA.FolioAddresses_FolioAddress_City
		   , FA.FolioAddresses_FolioAddress_City_Action
		   , FA.FolioAddresses_FolioAddress_City_OldValue
		   , FA.FolioAddress_ID
		   , FA.FolioAddresses_FolioAddress_PostalZip
		   , FA.FolioAddresses_FolioAddress_PostalZip_Action
		   , FA.FolioAddresses_FolioAddress_PostalZip_OldValue
		   , FA.FolioAddresses_FolioAddress_PrimaryFlag
		   , FA.FolioAddresses_FolioAddress_PrimaryFlag_Action
		   , FA.FolioAddresses_FolioAddress_PrimaryFlag_OldValue
		   , FA.FolioAddresses_FolioAddress_ProvinceState
		   , FA.FolioAddresses_FolioAddress_ProvinceState_Action
		   , FA.FolioAddresses_FolioAddress_ProvinceState_OldValue
		   , FA.FolioAddresses_FolioAddress_StreetDirectionSuffix
		   , FA.FolioAddresses_FolioAddress_StreetDirectionSuffix_Action
		   , FA.FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue
		   , FA.FolioAddresses_FolioAddress_StreetName
		   , FA.FolioAddresses_FolioAddress_StreetName_Action
		   , FA.FolioAddresses_FolioAddress_StreetName_OldValue
		   , FA.FolioAddresses_FolioAddress_StreetNumber
		   , FA.FolioAddresses_FolioAddress_StreetNumber_Action
		   , FA.FolioAddresses_FolioAddress_StreetNumber_OldValue
		   , FA.FolioAddresses_FolioAddress_StreetType
		   , FA.FolioAddresses_FolioAddress_StreetType_Action
		   , FA.FolioAddresses_FolioAddress_StreetType_OldValue
		   , FA.FolioAddresses_FolioAddress_UnitNumber
		   , FA.FolioAddresses_FolioAddress_UnitNumber_Action
		   , FA.FolioAddresses_FolioAddress_UnitNumber_OldValue
		   , FLM.LandMeasurement_LandDepth
		   , FLM.LandMeasurement_LandDepth_Action
		   , FLM.LandMeasurement_LandDepth_OldValue
		   , FLM.LandMeasurement_LandDimension
		   , FLM.LandMeasurement_LandDimension_Action
		   , FLM.LandMeasurement_LandDimension_OldValue
		   , FLM.LandMeasurement_LandDimensionTypeDescription
		   , FLM.LandMeasurement_LandDimensionTypeDescription_Action
		   , FLM.LandMeasurement_LandDimensionTypeDescription_OldValue
		   , FLM.LandMeasurement_LandWidth
		   , FLM.LandMeasurement_LandWidth_Action
		   , FLM.LandMeasurement_LandWidth_OldValue
		   , FN.FolioDescription_Neighbourhood_NeighbourhoodCode
		   , FN.FolioDescription_Neighbourhood_NeighbourhoodCode_Action
		   , FN.FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue
		   , FN.Neighbourhood_NeighbourhoodDescription
		   , FN.FolioDescription_Neighbourhood_NeighbourhoodDescription_Action
		   , FN.FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue
		   , FRD.RegionalDistrict_DistrictDescription
		   , FRD.RegionalDistrict_DistrictDescription_Action
		   , FRD.RegionalDistrict_DistrictDescription_OldValue
		   , FSD.SchoolDistrict_DistrictDescription
		   , FSD.SchoolDistrict_DistrictDescription_Action
		   , FSD.SchoolDistrict_DistrictDescription_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Action
		   , FLD.LegalDescriptions_LegalDescription_Block
		   , FLD.LegalDescriptions_LegalDescription_Block_Action
		   , FLD.LegalDescriptions_LegalDescription_Block_OldValue
		   , FLD.LegalDescriptions_LegalDescription_DistrictLot
		   , FLD.LegalDescriptions_LegalDescription_DistrictLot_Action
		   , FLD.LegalDescriptions_LegalDescription_DistrictLot_OldValue
		   , FLD.LegalDescriptions_LegalDescription_ExceptPlan
		   , FLD.LegalDescriptions_LegalDescription_ExceptPlan_Action
		   , FLD.LegalDescriptions_LegalDescription_ExceptPlan_OldValue
		   , FLD.LegalDescriptions_LegalDescription_FormattedLegalDescription
		   , FLD.LegalDescriptions_LegalDescription_FormattedLegalDescription_Action
		   , FLD.LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue
		   , FLD.LegalDescriptions_LegalDescription_ID
		   , FLD.LegalDescriptions_LegalDescription_LandDistrict
		   , FLD.LegalDescriptions_LegalDescription_LandDistrict_Action
		   , FLD.LegalDescriptions_LegalDescription_LandDistrict_OldValue
		   , FLD.LegalDescriptions_LegalDescription_LandDistrictDescription
		   , FLD.LegalDescriptions_LegalDescription_LandDistrictDescription_Action
		   , FLD.LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue
		   , FLD.LegalDescriptions_LegalDescription_LeaseLicenceNumber
		   , FLD.LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action
		   , FLD.LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue
		   , FLD.LegalDescriptions_LegalDescription_LegalText
		   , FLD.LegalDescriptions_LegalDescription_LegalText_Action
		   , FLD.LegalDescriptions_LegalDescription_LegalText_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Lot
		   , FLD.LegalDescriptions_LegalDescription_Lot_Action
		   , FLD.LegalDescriptions_LegalDescription_Lot_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Meridian
		   , FLD.LegalDescriptions_LegalDescription_Meridian_Action
		   , FLD.LegalDescriptions_LegalDescription_Meridian_OldValue
		   , FLD.LegalDescriptions_LegalDescription_MeridianShort
		   , FLD.LegalDescriptions_LegalDescription_MeridianShort_Action
		   , FLD.LegalDescriptions_LegalDescription_MeridianShort_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Parcel
		   , FLD.LegalDescriptions_LegalDescription_Parcel_Action
		   , FLD.LegalDescriptions_LegalDescription_Parcel_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Part1
		   , FLD.LegalDescriptions_LegalDescription_Part1_Action
		   , FLD.LegalDescriptions_LegalDescription_Part1_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Part2
		   , FLD.LegalDescriptions_LegalDescription_Part2_Action
		   , FLD.LegalDescriptions_LegalDescription_Part2_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Part3
		   , FLD.LegalDescriptions_LegalDescription_Part3_Action
		   , FLD.LegalDescriptions_LegalDescription_Part3_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Part4
		   , FLD.LegalDescriptions_LegalDescription_Part4_Action
		   , FLD.LegalDescriptions_LegalDescription_Part4_OldValue
		   , FLD.LegalDescriptions_LegalDescription_PID
		   , FLD.LegalDescriptions_LegalDescription_PID_Action
		   , FLD.LegalDescriptions_LegalDescription_PID_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Plan
		   , FLD.LegalDescriptions_LegalDescription_Plan_Action
		   , FLD.LegalDescriptions_LegalDescription_Plan_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Portion
		   , FLD.LegalDescriptions_LegalDescription_Portion_Action
		   , FLD.LegalDescriptions_LegalDescription_Portion_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Range
		   , FLD.LegalDescriptions_LegalDescription_Range_Action
		   , FLD.LegalDescriptions_LegalDescription_Range_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Section
		   , FLD.LegalDescriptions_LegalDescription_Section_Action
		   , FLD.LegalDescriptions_LegalDescription_Section_OldValue
		   , FLD.LegalDescriptions_LegalDescription_StrataLot
		   , FLD.LegalDescriptions_LegalDescription_StrataLot_Action
		   , FLD.LegalDescriptions_LegalDescription_StrataLot_OldValue
		   , FLD.LegalDescriptions_LegalDescription_SubBlock
		   , FLD.LegalDescriptions_LegalDescription_SubBlock_Action
		   , FLD.LegalDescriptions_LegalDescription_SubBlock_OldValue
		   , FLD.LegalDescriptions_LegalDescription_SubLot
		   , FLD.LegalDescriptions_LegalDescription_SubLot_Action
		   , FLD.LegalDescriptions_LegalDescription_SubLot_OldValue
		   , FLD.LegalDescriptions_LegalDescription_Township
		   , FLD.LegalDescriptions_LegalDescription_Township_Action
		   , FLD.LegalDescriptions_LegalDescription_Township_OldValue
		   , FLD.LegalDescriptions_LegalDescription_LegalSubdivision
		   , FLD.LegalDescriptions_LegalDescription_LegalSubdivision_Action
		   , FLD.LegalDescriptions_LegalDescription_LegalSubdivision_OldValue
		   , FRS.Sales_Sale_Action
		   , FRS.Sales_Sale_ConveyanceDate
		   , FRS.Sale_ConveyanceDate_Action
		   , FRS.Sale_ConveyanceDate_OldValue
		   , FRS.Sales_Sale_ConveyancePrice
		   , FRS.Sales_Sale_ConveyancePrice_Action
		   , FRS.Sales_Sale_ConveyancePrice_OldValue
		   , FRS.Sales_Sale_ID
		   FROM #FolioRecords FR
			LEFT JOIN #FolioRecordsAddresses FA ON FR.FolioRecordID = FA.FolioRecordID AND FR.RollNumber = FA.RollNumber
			LEFT JOIN #FolioRecordsLegalDescription FLD ON FR.FolioRecordID = FLD.FolioRecord_ID AND FR.RollNumber = FLD.RollNumber
			LEFT JOIN #RegionalDistrict FRD ON  FR.FolioRecordID = FRD.FolioRecordID AND FR.RollNumber = FRD.RollNumber
			LEFT JOIN #FolioRecordsSchoolDistrict FSD ON FR.FolioRecordID = FSD.FolioRecordID AND FR.RollNumber = FSD.RollNumber
			LEFT JOIN #FolioRecordsLandMesaurement FLM ON FR.FolioRecordID = FLM.FolioRecordID AND FR.RollNumber = FLM.RollNumber
			LEFT JOIN #FolioRecordsNeighbourhood FN ON FR.FolioRecordID = FN.FolioRecordID AND FR.RollNumber = FN.RollNumber
			LEFT JOIN #FolioDescription FD ON FR.FolioRecordID = FD.FolioRecordID AND FR.RollNumber = FD.RollNumber
			LEFT JOIN #FolioRecordsSales FRS ON FR.FolioRecordID = FRS.FolioRecordID AND FR.RollNumber = FRS.RollNumber
			LEFT JOIN  #FolioActionFolioDelete FAFD ON FR.FolioRecordID = FAFD.FolioRecordID AND FR.RollNumber = FAFD.RollNumber
			LEFT JOIN  #FolioActionFolioAdd FAADD ON FR.FolioRecordID = FAADD.FolioRecordID AND FR.RollNumber = FAADD.RollNumber
			CROSS APPLY #DataAdvice DataAdvice
			
		    
   END