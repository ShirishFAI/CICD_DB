





/****************************************************************************************      
 AUTHOR  : Srinivaas Chakravarthy T      
 DATE   : 02/18/2022      
 PURPOSE  : BCA Weekly Source File - Load to StageLanding.      
 DEPENDENCIES :       
      
 VERSION HISTORY:      
** ----------------------------------------------------------------------------------------      
** 02/18/2022 Srinivasa Chakravarthy T Original Version      
******************************************************************************************/  
CREATE PROCEDURE [ETLProcess].[CustomLoadStageLand_BC_ALL_Assessment_weekly_Test1]
	@ExternalFileName VARCHAR(500)   
AS
BEGIN
	
	SET NOCOUNT ON;
	DECLARE @Params NVARCHAR(500)='@ExternalFileName VARCHAR(500),@ExternalDataSourceName VARCHAR(100)';
	DECLARE @DynamicSQL NVARCHAR(MAX)='';                
    DECLARE @ErrorSchema varchar(300)='StageProcessErr.';        
    DECLARE @HistorySchema varchar(300)='SourceHistory.';        
    DECLARE @ExternalDataSourceName VARCHAR(100)='DTCDataSetExternal';   
	DECLARE @StageLandSchema varchar(300)='StageLanding.';                
    DECLARE @TableName VARCHAR(100)='BC_ALL_Assessment_Weekly_Test';             
    DECLARE @ProcessName VARCHAR(100) ;        
    DECLARE @CurrentStatus VARCHAR(100) ;        
    DECLARE @RunId  INT;        
    DECLARE @IsAuditEntryExists INT;        
    DECLARE @Status VARCHAR(100);        
    DECLARE @ActiveFlag BIT;        
    DECLARE @IsAuditProcessEntryExists INT;         
    DECLARE @IsError BIT=0;        
    DECLARE @ErrorProcedure VARCHAR(100);        
    DECLARE @Exception VARCHAR(500);        
    DECLARE @DynamicSQLLarge VARCHAR(8000);        
    DECLARE @ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';        
    DECLARE @IsKeyCount INT;        
    DECLARE @ProcessID INT;        
    DECLARE @DistKeyCnt INT=0;        
    DECLARE @DistRowCnt INT=0; 
	--declare @ExternalFileName VARCHAR(500) ='20220102_WEEKLY_0156.xml'
	DECLARE @Rundate varchar(30);
	DECLARE @StartDate varchar(30);
	DECLARE @EndDate varchar(30);
	DECLARE @RollYear varchar(30);
	DECLARE @OwnershipYear varchar(30);
	DECLARE @RunType varchar(30);
	DECLARE @CurrentStage varchar(50);
      
        
	 SET @ProcessName='BC_ALL_Assessment_Weekly_Test';

	 SET @TableName=@ProcessName;        
        

	  IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_NAME = N'BCAWeeklyXML_Test' and TABLE_SCHEMA=N'StageLanding')
		BEGIN
		  TRUNCATE TABLE StageLanding.BCAWeeklyXML_Test
		END

	 
		SET @DynamicSQL = @DynamicSQL+   
		'INSERT INTO StageLanding.BCAWeeklyXML_Test
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

		--create primary xml index idx_x on StageLanding.BCAWeeklyXML_Test (XMLData);
	
	
	;WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
    SELECT distinct
    bm.XMLData.value('(@RunDate)', 'varchar(500)') AS RunDate,
    bm.XMLData.value('StartDate[1]', 'varchar(500)') AS StartDate,
    bm.XMLData.value('EndDate[1]', 'varchar(500)') AS EndDate,
    bm.XMLData.value('RollYear[1]', 'varchar(500)') AS RollYear,
    bm.XMLData.value('OwnershipYear[1]', 'varchar(500)') AS OwnershipYear,
    bm.XMLData.value('RunType[1]', 'varchar(500)') AS RunType
    INTO #DataAdvice
    FROM StageLanding.BCAWeeklyXML_Test t
    CROSS APPLY
    t.XMLData.nodes('/DataAdvice')
    AS bm(XMLData);


	select @Rundate=RunDate
	,@StartDate=StartDate
	,@EndDate=EndDate
	,@RollYear=RollYear
	,@OwnershipYear=OwnershipYear
	,@RunType=RunType
	from #DataAdvice


	-- LegalDescriptions Data
	;WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	SELECT
	bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecord_ID,
	bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,

	bm.XMLData.value('@ID', 'varchar(500)') AS LegalDescriptions_LegalDescription_ID,

	bm.XMLData.value('@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Action,

	bm.XMLData.value('Block[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Block,
	bm.XMLData.value('Block[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Block_Action,
	bm.XMLData.value('Block[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Block_OldValue,

	bm.XMLData.value('DistrictLot[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_DistrictLot,
	bm.XMLData.value('DistrictLot[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_DistrictLot_Action,
	bm.XMLData.value('DistrictLot[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_DistrictLot_OldValue,

	bm.XMLData.value('ExceptPlan[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_ExceptPlan,
	bm.XMLData.value('ExceptPlan[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_ExceptPlan_Action,
	bm.XMLData.value('ExceptPlan[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_ExceptPlan_OldValue,

	bm.XMLData.value('FormattedLegalDescription[1]', 'varchar(300)') AS LegalDescriptions_LegalDescription_FormattedLegalDescription,
	bm.XMLData.value('FormattedLegalDescription[1]/@Action', 'varchar(300)') AS LegalDescriptions_LegalDescription_FormattedLegalDescription_Action,
	bm.XMLData.value('FormattedLegalDescription[1]/@OldValue', 'varchar(300)') AS LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue,

	bm.XMLData.value('LandDistrict[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_LandDistrict,
	bm.XMLData.value('LandDistrict[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_LandDistrict_Action,
	bm.XMLData.value('LandDistrict[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_LandDistrict_OldValue,
	
	bm.XMLData.value('LandDistrictDescription[1]', 'varchar(300)') AS LegalDescriptions_LegalDescription_LandDistrictDescription,
	bm.XMLData.value('LandDistrictDescription[1]/@Action', 'varchar(300)') AS LegalDescriptions_LegalDescription_LandDistrictDescription_Action,
	bm.XMLData.value('LandDistrictDescription[1]/@OldValue', 'varchar(300)') AS LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue,
	
	bm.XMLData.value('LeaseLicenceNumber[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_LeaseLicenceNumber,
	bm.XMLData.value('LeaseLicenceNumber[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action,
	bm.XMLData.value('LeaseLicenceNumber[1]/@Oldvalue', 'varchar(500)') AS LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue,

	bm.XMLData.value('LegalText[1]', 'varchar(300)') AS LegalDescriptions_LegalDescription_LegalText,
	bm.XMLData.value('LegalText[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_LegalText_Action,
	bm.XMLData.value('LegalText[1]/@OldValue', 'varchar(300)') AS LegalDescriptions_LegalDescription_LegalText_OldValue,

	bm.XMLData.value('Lot[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Lot,
	bm.XMLData.value('Lot[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Lot_Action,
	bm.XMLData.value('Lot[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Lot_OldValue,

	bm.XMLData.value('Meridian[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Meridian,
	bm.XMLData.value('Meridian[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Meridian_Action,
	bm.XMLData.value('Meridian[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Meridian_OldValue,
	
	bm.XMLData.value('MeridianShort[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_MeridianShort,
	bm.XMLData.value('MeridianShort[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_MeridianShort_Action,
	bm.XMLData.value('MeridianShort[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_MeridianShort_OldValue,

	bm.XMLData.value('Parcel[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Parcel,
	bm.XMLData.value('Parcel[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Parcel_Action,
	bm.XMLData.value('Parcel[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Parcel_OldValue,

	bm.XMLData.value('Part1[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part1,
	bm.XMLData.value('Part1[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part1_Action,
	bm.XMLData.value('Part1[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part1_OldValue,

	bm.XMLData.value('Part2[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part2,
	bm.XMLData.value('Part2[2]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part2_Action,
	bm.XMLData.value('Part2[2]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part2_OldValue,

	bm.XMLData.value('Part3[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part3,
	bm.XMLData.value('Part3[3]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part3_Action,
	bm.XMLData.value('Part3[3]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part3_OldValue,

	bm.XMLData.value('Part4[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part4,
	bm.XMLData.value('Part4[4]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part4_Action,
	bm.XMLData.value('Part4[4]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Part4_OldValue,

	bm.XMLData.value('PID[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_PID,
	bm.XMLData.value('PID[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_PID_Action,
	bm.XMLData.value('PID[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_PID_OldValue,

	bm.XMLData.value('Plan[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Plan,
	bm.XMLData.value('Plan[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Plan_Action,
	bm.XMLData.value('Plan[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Plan_OldValue,

	bm.XMLData.value('Portion[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Portion,
	bm.XMLData.value('Portion[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Portion_Action,
	bm.XMLData.value('Portion[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Portion_OldValue,
	
	bm.XMLData.value('Range[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Range,
	bm.XMLData.value('Range[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Range_Action,
	bm.XMLData.value('Range[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Range_OldValue,
	
	bm.XMLData.value('Section[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Section,
	bm.XMLData.value('Section[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Section_Action,
	bm.XMLData.value('Section[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Section_OldValue,
	
	bm.XMLData.value('StrataLot[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_StrataLot,
	bm.XMLData.value('StrataLot[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_StrataLot_Action,
	bm.XMLData.value('StrataLot[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_StrataLot_OldValue,

	bm.XMLData.value('SubBlock[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_SubBlock,
	bm.XMLData.value('SubBlock[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_SubBlock_Action,
	bm.XMLData.value('SubBlock[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_SubBlock_OldValue,

	bm.XMLData.value('SubLot[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_SubLot,
	bm.XMLData.value('SubLot[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_SubLot_Action,
	bm.XMLData.value('SubLot[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_SubLot_OldValue,

	bm.XMLData.value('Township[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_Township,
	bm.XMLData.value('Township[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_Township_Action,
	bm.XMLData.value('Township[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_Township_OldValue,
	
	bm.XMLData.value('LegalSubdivision[1]', 'varchar(500)') AS LegalDescriptions_LegalDescription_LegalSubdivision,
	bm.XMLData.value('LegalSubdivision[1]/@Action', 'varchar(500)') AS LegalDescriptions_LegalDescription_LegalSubdivision_Action,
	bm.XMLData.value('LegalSubdivision[1]/@OldValue', 'varchar(500)') AS LegalDescriptions_LegalDescription_LegalSubdivision_OldValue
	
	INTO #FolioRecordsLegalDescription
	FROM StageLanding.BCAWeeklyXML_Test t 
	CROSS APPLY 
	t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/LegalDescriptions/LegalDescription')
	AS bm(XMLData);
	select * from  #FolioRecordsLegalDescription
	

	----Regional District Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd') 
	--SELECT
	--bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,
	
	--bm.XMLData.value('DistrictDescription[1]', 'varchar(300)') AS RegionalDistrict_DistrictDescription,
	--bm.XMLData.value('DistrictDescription[1]/@Action', 'varchar(300)') AS RegionalDistrict_DistrictDescription_Action,
	--bm.XMLData.value('DistrictDescription[1]/@OldValue', 'varchar(300)') AS RegionalDistrict_DistrictDescription_OldValue
	
	--INTO #RegionalDistrict
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/RegionalDistrict')
	--AS bm(XMLData);
	

	----School District Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd') 
	--SELECT
	--bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,
	
	--bm.XMLData.value('DistrictDescription[1]', 'varchar(300)') AS SchoolDistrict_DistrictDescription,
	--bm.XMLData.value('DistrictDescription[1]/@Action', 'varchar(300)') AS SchoolDistrict_DistrictDescription_Action,
	--bm.XMLData.value('DistrictDescription[1]/@OldValue', 'varchar(300)') AS SchoolDistrict_DistrictDescription_OldValue
	
	--INTO #FolioRecordsSchoolDistrict
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/SchoolDistrict')
	--AS bm(XMLData);
	
	----Sales Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd') 
	--SELECT
	--bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,

	--bm.XMLData.value('@ID', 'varchar(500)') AS Sales_Sale_ID,

	--bm.XMLData.value('@Action', 'varchar(500)') AS Sales_Sale_Action,

	--bm.XMLData.value('ConveyanceDate[1]', 'varchar(500)') AS Sales_Sale_ConveyanceDate,
	--bm.XMLData.value('ConveyanceDate[1]/@Action', 'varchar(500)') AS Sale_ConveyanceDate_Action,
	--bm.XMLData.value('ConveyanceDate[1]/@OldValue', 'varchar(500)') AS Sale_ConveyanceDate_OldValue,

	--bm.XMLData.value('ConveyancePrice[1]', 'varchar(500)') AS Sales_Sale_ConveyancePrice,
	--bm.XMLData.value('ConveyancePrice[1]/@Action', 'varchar(500)') AS Sales_Sale_ConveyancePrice_Action,
	--bm.XMLData.value('ConveyancePrice[1]/@OldValue', 'varchar(500)') AS Sales_Sale_ConveyancePrice_OldValue
	--INTO #FolioRecordsSales
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/Sales/Sale')
	--AS bm(XMLData);
	

	----Land Measurement Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	--SELECT
	--bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,

	--bm.XMLData.value('LandDepth[1]', 'varchar(500)') AS LandMeasurement_LandDepth,
	--bm.XMLData.value('LandDepth[1]/@Action', 'varchar(500)') AS LandMeasurement_LandDepth_Action,
	--bm.XMLData.value('LandDepth[1]/@OldValue', 'varchar(500)') AS LandMeasurement_LandDepth_OldValue,

	--bm.XMLData.value('LandDimension[1]', 'varchar(500)') AS LandMeasurement_LandDimension,
	--bm.XMLData.value('LandDimension[1]/@Action', 'varchar(500)') AS LandMeasurement_LandDimension_Action,
	--bm.XMLData.value('LandDimension[1]/@OldValue', 'varchar(500)') AS LandMeasurement_LandDimension_OldValue,

	--bm.XMLData.value('LandDimensionTypeDescription[1]', 'varchar(300)') AS LandMeasurement_LandDimensionTypeDescription,
	--bm.XMLData.value('LandDimensionTypeDescription[1]/@Action', 'varchar(300)') AS LandMeasurement_LandDimensionTypeDescription_Action,
	--bm.XMLData.value('LandDimensionTypeDescription[1]/@OldValue', 'varchar(300)') AS LandMeasurement_LandDimensionTypeDescription_OldValue,

	--bm.XMLData.value('LandWidth[1]', 'varchar(500)') AS LandMeasurement_LandWidth,
	--bm.XMLData.value('LandWidth[1]/@Action', 'varchar(500)') AS LandMeasurement_LandWidth_Action,
	--bm.XMLData.value('LandWidth[1]/@OldValue', 'varchar(500)') AS LandMeasurement_LandWidth_OldValue
	--INTO #FolioRecordsLandMesaurement
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/LandMeasurement')
	--AS bm(XMLData);
	

	----Address Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	--SELECT
	--bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,
	--bm.XMLData.value('@Action', 'varchar(500)') AS FolioAddress_Action,
	--bm.XMLData.value('City[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_City,
	--bm.XMLData.value('City[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_City_Action,
	--bm.XMLData.value('City[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_City_OldValue,

	--bm.XMLData.value('@ID', 'varchar(500)') AS FolioAddress_ID,

	--bm.XMLData.value('PostalZip[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_PostalZip,
	--bm.XMLData.value('PostalZip[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_PostalZip_Action,
	--bm.XMLData.value('PostalZip[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_PostalZip_OldValue,

	--bm.XMLData.value('PrimaryFlag[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_PrimaryFlag,
	--bm.XMLData.value('PrimaryFlag[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_PrimaryFlag_Action,
	--bm.XMLData.value('PrimaryFlag[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_PrimaryFlag_OldValue,

	--bm.XMLData.value('ProvinceState[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_ProvinceState,
	--bm.XMLData.value('ProvinceState[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_ProvinceState_Action,
	--bm.XMLData.value('ProvinceState[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_ProvinceState_OldValue,

	--bm.XMLData.value('StreetDirectionSuffix[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetDirectionSuffix,
	--bm.XMLData.value('StreetDirectionSuffix[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetDirectionSuffix_Action,
	--bm.XMLData.value('StreetDirectionSuffix[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue,

	--bm.XMLData.value('StreetName[1]', 'varchar(300)') AS FolioAddresses_FolioAddress_StreetName,
	--bm.XMLData.value('StreetName[1]/@Action', 'varchar(300)') AS FolioAddresses_FolioAddress_StreetName_Action,
	--bm.XMLData.value('StreetName[1]/@OldValue', 'varchar(300)') AS FolioAddresses_FolioAddress_StreetName_OldValue,

	--bm.XMLData.value('StreetNumber[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetNumber,
	--bm.XMLData.value('StreetNumber[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetNumber_Action,
	--bm.XMLData.value('StreetNumber[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetNumber_OldValue,

	--bm.XMLData.value('StreetType[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetType,
	--bm.XMLData.value('StreetType[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetType_Action,
	--bm.XMLData.value('StreetType[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_StreetType_OldValue,

	--bm.XMLData.value('UnitNumber[1]', 'varchar(500)') AS FolioAddresses_FolioAddress_UnitNumber,
	--bm.XMLData.value('UnitNumber[1]/@Action', 'varchar(500)') AS FolioAddresses_FolioAddress_UnitNumber_Action,
	--bm.XMLData.value('UnitNumber[1]/@OldValue', 'varchar(500)') AS FolioAddresses_FolioAddress_UnitNumber_OldValue

	--INTO #FolioRecordsAddresses
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAddresses/FolioAddress')
	--AS bm(XMLData);
	


	----Neighbourhood Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd') 
	--SELECT
	--bm.XMLData.value('(../../../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../../../RollNumber[1])','varchar(500)') as RollNumber,
	
	--bm.XMLData.value('../NeighbourhoodCode[1]', 'varchar(500)') AS FolioDescription_Neighbourhood_NeighbourhoodCode,
	--bm.XMLData.value('@Action', 'varchar(500)') AS FolioDescription_Neighbourhood_NeighbourhoodCode_Action,
	--bm.XMLData.value('@OldValue', 'varchar(500)') AS FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue,
	
	--bm.XMLData.value('../NeighbourhoodDescription[1]', 'varchar(300)') AS Neighbourhood_NeighbourhoodDescription,
	--bm.XMLData.value('../NeighbourhoodDescription[1]/@Action', 'varchar(300)') AS FolioDescription_Neighbourhood_NeighbourhoodDescription_Action,
	--bm.XMLData.value('../NeighbourhoodDescription[1]/@OldValue', 'varchar(300)') AS FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue
	
	--INTO #FolioRecordsNeighbourhood
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription/Neighbourhood/NeighbourhoodCode')
	--AS bm(XMLData);
	

	------------#FolioRecords
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	--SELECT
	--f.XMLData.value('@ID', 'varchar(500)') AS FolioRecordID,
	--f.XMLData.value('RollNumber[1]','varchar(500)') as RollNumber,
	--f.XMLData.value('RollNumber[1]/@OldValue','varchar(500)') as RollNumber_OldValue,--FolioRecords_RollNumber_OldValue,
	--f.XMLData.value('FolioStatus[1]','varchar(500)') as FolioRecords_FolioStatus,
	--f.XMLData.value('FolioStatusDescription[1]','varchar(300)') as FolioRecords_FolioStatusDescription,
	--f.XMLData.value('../../JurisdictionCode[1]', 'varchar(500)') AS JurisdictionCode,--FolioRecords_JurisdictionCode,
	--f.XMLData.value('../../JurisdictionDescription[1]', 'varchar(300)') AS JurisdictionDescription,--FolioRecords_JurisdictionDescription,
	--f.XMLData.value('../../../../AssessmentAreaCode[1]', 'varchar(500)') AS AssessmentAreaCode,-- FolioRecords_AssessmentAreaCode,
	--f.XMLData.value('../../../../AssessmentAreaDescription[1]', 'varchar(300)') AS AssessmentAreaDescription,--FolioRecords_AssessmentAreaDescription,
	----bm.XMLData.value('EndDate[1]', 'varchar(500)') AS FolioRecords_EndDate,
	----bm.XMLData.value('OwnershipYear[1]', 'varchar(500)') AS FolioRecords_OwnershipYear,
	----bm.XMLData.value('RollYear[1]', 'varchar(500)') AS FolioRecords_RollYear,
	----bm.XMLData.value('RunDate[1]', 'varchar(500)') AS FolioRecords_RunDate,
	----bm.XMLData.value('RunType[1]', 'varchar(500)') AS FolioRecords_RunType,
	----bm.XMLData.value('StartDate[1]', 'varchar(500)') AS FolioRecords_StartDate,
	----------------------------------------------
	--f.XMLData.value('RollNumber[1]/@Action','varchar(500)') as RollNumber_Action --FolioRecords_RollNumber_Action
	--INTO #FolioRecords
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--OUTER APPLY 	t.XMLData.nodes('/DataAdvice')	AS bm(XMLData)
	--OUTER APPLY
	--bm.XMLData.nodes('AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord') as f(XMLData);
	

	----FolioDescription Data
	--WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	--SELECT
	--bm.XMLData.value('(../@ID)', 'varchar(500)') AS FolioRecordID,
	--bm.XMLData.value('(../RollNumber[1])','varchar(500)') as RollNumber,

	--bm.XMLData.value('@Action','varchar(300)') as FolioDescription_Action,

	--bm.XMLData.value('ActualUseDescription[1]','varchar(300)') as ActualUseDescription,
	--bm.XMLData.value('ActualUseDescription[1]/@Action','varchar(300)') as ActualUseDescription_Action,
	--bm.XMLData.value('ActualUseDescription[1]/@OldValue','varchar(300)') as ActualUseDescription_OldValue,

	--bm.XMLData.value('TenureDescription[1]','varchar(300)') as TenureDescription,
	--bm.XMLData.value('TenureDescription[1]/@Action','varchar(300)') as TenureDescription_Action,
	--bm.XMLData.value('TenureDescription[1]/@OldValue','varchar(300)') as TenureDescription_OldValue,

	--bm.XMLData.value('VacantFlag[1]','varchar(500)') as VacantFlag,
	--bm.XMLData.value('VacantFlag[1]/@Action','varchar(500)') as VacantFlag_Action,
	--bm.XMLData.value('VacantFlag[1]/@OldValue','varchar(500)') as VacantFlag_OldValue
	--INTO #FolioDescription
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--CROSS APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioDescription')
	--AS bm(XMLData);
	
	----************************************************************************************************************

	--------------#FolioRecordsFolioAdd
	----WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	----SELECT
	----bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	----bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,
	----bm.XMLData.value('AssessmentAreaCode[1]', 'varchar(500)') AS FolioAdd_AssessmentAreaCode,
	----bm.XMLData.value('AssessmentAreaDescription[1]', 'varchar(300)') AS FolioAdd_AssessmentAreaDescription,
	----bm.XMLData.value('JurisdictionCode[1]', 'varchar(500)') AS FolioAdd_JurisdictionCode,
	----bm.XMLData.value('JurisdictionDescription[1]', 'varchar(300)') AS FolioAdd_JurisdictionDescription,
	----bm.XMLData.value('RollNumber[1]', 'varchar(500)') AS FolioAction_FolioAdd -- FolioAdd_RollNumber
	----INTO #FolioRecordsFolioAdd
	----FROM StageLanding.BCAWeeklyXML t 
	----CROSS APPLY 
	----t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioActions/FolioAdd/FolioRenumber')
	----AS bm(XMLData);
	

	----WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	----SELECT
	----bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID,
	----bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber,
	----bm.XMLData.value('DeleteReasonCode[1]', 'varchar(500)') AS FolioAction_FolioDelete_DeleteReasonCode,
	----bm.XMLData.value('DeleteReasonDescription[1]', 'varchar(300)') AS FolioAction_FolioDelete_DeleteReasonDescription,
	----f.XMLData.value('AssessmentAreaCode[1]', 'varchar(500)') AS FolioDelete_AssessmentAreaCode,
	----f.XMLData.value('AssessmentAreaDescription[1]', 'varchar(300)') AS FolioDelete_AssessmentAreaDescription,
	----f.XMLData.value('JurisdictionCode[1]', 'varchar(500)') AS FolioDelete_JurisdictionCode,
	----f.XMLData.value('JurisdictionDescription[1]', 'varchar(300)') AS FolioDelete_JurisdictionDescription,
	----f.XMLData.value('RollNumber[1]', 'varchar(500)') AS FolioAction_FolioDelete --FolioDelete_RollNumber
	----INTO #FolioRecordsFolioDelete
	----FROM StageLanding.BCAWeeklyXML t 
	----OUTER APPLY 
	----t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioDelete')
	----AS bm(XMLData)
	----OUTER APPLY bm.XMLData.nodes('FolioRenumber') as f(XMLData)
	

	--;WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	--SELECt 
	--bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID
	--,bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber

	--,t.XMLData.exist(('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioDelete')) AS FolioAction_FolioDelete
	--,bm.XMLData.value('DeleteReasonCode[1]', 'varchar(500)') AS FolioAction_FolioDelete_DeleteReasonCode
	--,bm.XMLData.value('DeleteReasonDescription[1]', 'varchar(300)') AS FolioAction_FolioDelete_DeleteReasonDescription
	--INTO #FolioActionFolioDelete
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--OUTER APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioDelete')
	--AS bm(XMLData)


	--;WITH XMLNAMESPACES (DEFAULT 'http://data.bcassessment.ca/DataAdvice/Formats/DAX/DataAdvice.xsd')
	--SELECt t.XMLData.exist(('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioAdd')) AS FolioAction_FolioAdd
	--,bm.XMLData.value('(../../@ID)', 'varchar(500)') AS FolioRecordID
	--,bm.XMLData.value('(../../RollNumber[1])','varchar(500)') as RollNumber
	--INTO #FolioActionFolioAdd
	--FROM StageLanding.BCAWeeklyXML_Test t 
	--OUTER APPLY 
	--t.XMLData.nodes('/DataAdvice/AssessmentAreas/AssessmentArea/Jurisdictions/Jurisdiction/FolioRecords/FolioRecord/FolioAction/FolioAdd')
	--AS bm(XMLData)
	----OUTER APPLY bm.XMLData.nodes('FolioRenumber') as f(XMLData)

	
	--INSERT INTO StageLanding.BC_ALL_Assessment_Weekly_Test ( 
	--	[FolioRecord_ID]
	--	,[RunType]
	--	,[RollYear]
	--	,[OwnershipYear]
	--	,[StartDate]
	--	,[EndDate]
	--	,[RunDate]
	--	,[AssessmentAreaCode]
	--	,[AssessmentAreaDescription]
	--	,[JurisdictionCode]
	--	,[JurisdictionDescription]
	--	,[RollNumber]
	--	,[RollNumber_Action]
	--	,[RollNumber_OldValue]
	--	,[FolioAction_FolioAdd]
	--	,[FolioAction_FolioDelete]
	--	,[FolioAction_FolioDelete_DeleteReasonCode]
	--	,[FolioAction_FolioDelete_DeleteReasonDescription]
	--	,[FolioDescription_Action]
	--	,[ActualUseDescription]
	--	,[ActualUseDescription_Action]
	--	,[ActualUseDescription_OldValue]
	--	,[TenureDescription]
	--	,[TenureDescription_Action]
	--	,[TenureDescription_OldValue]
	--	,[VacantFlag]
	--	,[VacantFlag_Action]
	--	,[VacantFlag_OldValue]
	--	,[FolioAddresses_FolioAddress_Action]
	--	,[FolioAddresses_FolioAddress_City]
	--	,[FolioAddresses_FolioAddress_City_Action]
	--	,[FolioAddresses_FolioAddress_City_OldValue]
	--	,[FolioAddresses_FolioAddress_ID]
	--	,[FolioAddresses_FolioAddress_PostalZip]
	--	,[FolioAddresses_FolioAddress_PostalZip_Action]
	--	,[FolioAddresses_FolioAddress_PostalZip_OldValue]
	--	,[FolioAddresses_FolioAddress_PrimaryFlag]
	--	,[FolioAddresses_FolioAddress_PrimaryFlag_Action]
	--	,[FolioAddresses_FolioAddress_PrimaryFlag_OldValue]
	--	,[FolioAddresses_FolioAddress_ProvinceState]
	--	,[FolioAddresses_FolioAddress_ProvinceState_Action]
	--	,[FolioAddresses_FolioAddress_ProvinceState_OldValue]
	--	,[FolioAddresses_FolioAddress_StreetDirectionSuffix]
	--	,[FolioAddresses_FolioAddress_StreetDirectionSuffix_Action]
	--	,[FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue]
	--	,[FolioAddresses_FolioAddress_StreetName]
	--	,[FolioAddresses_FolioAddress_StreetName_Action]
	--	,[FolioAddresses_FolioAddress_StreetName_OldValue]
	--	,[FolioAddresses_FolioAddress_StreetNumber]
	--	,[FolioAddresses_FolioAddress_StreetNumber_Action]
	--	,[FolioAddresses_FolioAddress_StreetNumber_OldValue]
	--	,[FolioAddresses_FolioAddress_StreetType]
	--	,[FolioAddresses_FolioAddress_StreetType_Action]
	--	,[FolioAddresses_FolioAddress_StreetType_OldValue]
	--	,[FolioAddresses_FolioAddress_UnitNumber]
	--	,[FolioAddresses_FolioAddress_UnitNumber_Action]
	--	,[FolioAddresses_FolioAddress_UnitNumber_OldValue]
	--	,[LandMeasurement_LandDepth]
	--	,[LandMeasurement_LandDepth_Action]
	--	,[LandMeasurement_LandDepth_OldValue]
	--	,[LandMeasurement_LandDimension]
	--	,[LandMeasurement_LandDimension_Action]
	--	,[LandMeasurement_LandDimension_OldValue]
	--	,[LandMeasurement_LandDimensionTypeDescription]
	--	,[LandMeasurement_LandDimensionTypeDescription_Action]
	--	,[LandMeasurement_LandDimensionTypeDescription_OldValue]
	--	,[LandMeasurement_LandWidth]
	--	,[LandMeasurement_LandWidth_Action]
	--	,[LandMeasurement_LandWidth_OldValue]
	--	,[FolioDescription_Neighbourhood_NeighbourhoodCode]
	--	,[FolioDescription_Neighbourhood_NeighbourhoodCode_Action]
	--	,[FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue]
	--	,[FolioDescription_Neighbourhood_NeighbourhoodDescription]
	--	,[FolioDescription_Neighbourhood_NeighbourhoodDescription_Action]
	--	,[FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue]
	--	,[RegionalDistrict_DistrictDescription]
	--	,[RegionalDistrict_DistrictDescription_Action]
	--	,[RegionalDistrict_DistrictDescription_OldValue]
	--	,[SchoolDistrict_DistrictDescription]
	--	,[SchoolDistrict_DistrictDescription_Action]
	--	,[SchoolDistrict_DistrictDescription_OldValue]
	--	,[LegalDescriptions_LegalDescription_Action]
	--	,[LegalDescriptions_LegalDescription_Block]
	--	,[LegalDescriptions_LegalDescription_Block_Action]
	--	,[LegalDescriptions_LegalDescription_Block_OldValue]
	--	,[LegalDescriptions_LegalDescription_DistrictLot]
	--	,[LegalDescriptions_LegalDescription_DistrictLot_Action]
	--	,[LegalDescriptions_LegalDescription_DistrictLot_OldValue]
	--	,[LegalDescriptions_LegalDescription_ExceptPlan]
	--	,[LegalDescriptions_LegalDescription_ExceptPlan_Action]
	--	,[LegalDescriptions_LegalDescription_ExceptPlan_OldValue]
	--	,[LegalDescriptions_LegalDescription_FormattedLegalDescription]
	--	,[LegalDescriptions_LegalDescription_FormattedLegalDescription_Action]
	--	,[LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue]
	--	,[LegalDescriptions_LegalDescription_ID]
	--	,[LegalDescriptions_LegalDescription_LandDistrict]
	--	,[LegalDescriptions_LegalDescription_LandDistrict_Action]
	--	,[LegalDescriptions_LegalDescription_LandDistrict_OldValue]
	--	,[LegalDescriptions_LegalDescription_LandDistrictDescription]
	--	,[LegalDescriptions_LegalDescription_LandDistrictDescription_Action]
	--	,[LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue]
	--	,[LegalDescriptions_LegalDescription_LeaseLicenceNumber]
	--	,[LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action]
	--	,[LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue]
	--	,[LegalDescriptions_LegalDescription_LegalText]
	--	,[LegalDescriptions_LegalDescription_LegalText_Action]
	--	,[LegalDescriptions_LegalDescription_LegalText_OldValue]
	--	,[LegalDescriptions_LegalDescription_Lot]
	--	,[LegalDescriptions_LegalDescription_Lot_Action]
	--	,[LegalDescriptions_LegalDescription_Lot_OldValue]
	--	,[LegalDescriptions_LegalDescription_Meridian]
	--	,[LegalDescriptions_LegalDescription_Meridian_Action]
	--	,[LegalDescriptions_LegalDescription_Meridian_OldValue]
	--	,[LegalDescriptions_LegalDescription_MeridianShort]
	--	,[LegalDescriptions_LegalDescription_MeridianShort_Action]
	--	,[LegalDescriptions_LegalDescription_MeridianShort_OldValue]
	--	,[LegalDescriptions_LegalDescription_Parcel]
	--	,[LegalDescriptions_LegalDescription_Parcel_Action]
	--	,[LegalDescriptions_LegalDescription_Parcel_OldValue]
	--	,[LegalDescriptions_LegalDescription_Part1]
	--	,[LegalDescriptions_LegalDescription_Part1_Action]
	--	,[LegalDescriptions_LegalDescription_Part1_OldValue]
	--	,[LegalDescriptions_LegalDescription_Part2]
	--	,[LegalDescriptions_LegalDescription_Part2_Action]
	--	,[LegalDescriptions_LegalDescription_Part2_OldValue]
	--	,[LegalDescriptions_LegalDescription_Part3]
	--	,[LegalDescriptions_LegalDescription_Part3_Action]
	--	,[LegalDescriptions_LegalDescription_Part3_OldValue]
	--	,[LegalDescriptions_LegalDescription_Part4]
	--	,[LegalDescriptions_LegalDescription_Part4_Action]
	--	,[LegalDescriptions_LegalDescription_Part4_OldValue]
	--	,[LegalDescriptions_LegalDescription_PID]
	--	,[LegalDescriptions_LegalDescription_PID_Action]
	--	,[LegalDescriptions_LegalDescription_PID_OldValue]
	--	,[LegalDescriptions_LegalDescription_Plan]
	--	,[LegalDescriptions_LegalDescription_Plan_Action]
	--	,[LegalDescriptions_LegalDescription_Plan_OldValue]
	--	,[LegalDescriptions_LegalDescription_Portion]
	--	,[LegalDescriptions_LegalDescription_Portion_Action]
	--	,[LegalDescriptions_LegalDescription_Portion_OldValue]
	--	,[LegalDescriptions_LegalDescription_Range]
	--	,[LegalDescriptions_LegalDescription_Range_Action]
	--	,[LegalDescriptions_LegalDescription_Range_OldValue]
	--	,[LegalDescriptions_LegalDescription_Section]
	--	,[LegalDescriptions_LegalDescription_Section_Action]
	--	,[LegalDescriptions_LegalDescription_Section_OldValue]
	--	,[LegalDescriptions_LegalDescription_StrataLot]
	--	,[LegalDescriptions_LegalDescription_StrataLot_Action]
	--	,[LegalDescriptions_LegalDescription_StrataLot_OldValue]
	--	,[LegalDescriptions_LegalDescription_SubBlock]
	--	,[LegalDescriptions_LegalDescription_SubBlock_Action]
	--	,[LegalDescriptions_LegalDescription_SubBlock_OldValue]
	--	,[LegalDescriptions_LegalDescription_SubLot]
	--	,[LegalDescriptions_LegalDescription_SubLot_Action]
	--	,[LegalDescriptions_LegalDescription_SubLot_OldValue]
	--	,[LegalDescriptions_LegalDescription_Township]
	--	,[LegalDescriptions_LegalDescription_Township_Action]
	--	,[LegalDescriptions_LegalDescription_Township_OldValue]
	--	,[LegalDescriptions_LegalDescription_LegalSubdivision]
	--	,[LegalDescriptions_LegalDescription_LegalSubdivision_Action]
	--	,[LegalDescriptions_LegalDescription_LegalSubdivision_OldValue]
	--	,[Sales_Sale_Action]
	--	,[Sales_Sale_ConveyanceDate]
	--	,[Sales_Sale_ConveyanceDate_Action]
	--	,[Sales_Sale_ConveyanceDate_OldValue]
	--	,[Sales_Sale_ConveyancePrice]
	--	,[Sales_Sale_ConveyancePrice_Action]
	--	,[Sales_Sale_ConveyancePrice_OldValue]
	--	,[Sales_Sale_ID]
	--	)

	--SELECT  distinct FR.FolioRecordID
	--	   , @RunType
	--	   , @RollYear
	--	   , @OwnershipYear
	--	   , @StartDate
	--	   , @EndDate
	--	   , @RunDate
	--	   , FR.AssessmentAreaCode
	--	   , FR.AssessmentAreaDescription
	--	   , FR.JurisdictionCode
	--	   , FR.JurisdictionDescription
	--	   , FR.RollNumber
	--	   , FR.RollNumber_Action
	--	   , FR.RollNumber_OldValue
	--	   , FAADD.FolioAction_FolioAdd 
	--	   , FAFD.FolioAction_FolioDelete 
	--	   , FAFD.FolioAction_FolioDelete_DeleteReasonCode
	--	   , FAFD.FolioAction_FolioDelete_DeleteReasonDescription
	--	   , FD.FolioDescription_Action
	--	   , FD.ActualUseDescription
	--	   , FD.ActualUseDescription_Action
	--	   , FD.ActualUseDescription_OldValue
	--	   , FD.TenureDescription
	--	   , FD.TenureDescription_Action
	--	   , FD.TenureDescription_OldValue
	--	   , FD.VacantFlag
	--	   , FD.VacantFlag_Action
	--	   , FD.VacantFlag_OldValue
	--	   , FA.FolioAddress_Action
	--	   , FA.FolioAddresses_FolioAddress_City
	--	   , FA.FolioAddresses_FolioAddress_City_Action
	--	   , FA.FolioAddresses_FolioAddress_City_OldValue
	--	   , FA.FolioAddress_ID
	--	   , FA.FolioAddresses_FolioAddress_PostalZip
	--	   , FA.FolioAddresses_FolioAddress_PostalZip_Action
	--	   , FA.FolioAddresses_FolioAddress_PostalZip_OldValue
	--	   , FA.FolioAddresses_FolioAddress_PrimaryFlag
	--	   , FA.FolioAddresses_FolioAddress_PrimaryFlag_Action
	--	   , FA.FolioAddresses_FolioAddress_PrimaryFlag_OldValue
	--	   , FA.FolioAddresses_FolioAddress_ProvinceState
	--	   , FA.FolioAddresses_FolioAddress_ProvinceState_Action
	--	   , FA.FolioAddresses_FolioAddress_ProvinceState_OldValue
	--	   , FA.FolioAddresses_FolioAddress_StreetDirectionSuffix
	--	   , FA.FolioAddresses_FolioAddress_StreetDirectionSuffix_Action
	--	   , FA.FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue
	--	   , FA.FolioAddresses_FolioAddress_StreetName
	--	   , FA.FolioAddresses_FolioAddress_StreetName_Action
	--	   , FA.FolioAddresses_FolioAddress_StreetName_OldValue
	--	   , FA.FolioAddresses_FolioAddress_StreetNumber
	--	   , FA.FolioAddresses_FolioAddress_StreetNumber_Action
	--	   , FA.FolioAddresses_FolioAddress_StreetNumber_OldValue
	--	   , FA.FolioAddresses_FolioAddress_StreetType
	--	   , FA.FolioAddresses_FolioAddress_StreetType_Action
	--	   , FA.FolioAddresses_FolioAddress_StreetType_OldValue
	--	   , FA.FolioAddresses_FolioAddress_UnitNumber
	--	   , FA.FolioAddresses_FolioAddress_UnitNumber_Action
	--	   , FA.FolioAddresses_FolioAddress_UnitNumber_OldValue
	--	   , FLM.LandMeasurement_LandDepth
	--	   , FLM.LandMeasurement_LandDepth_Action
	--	   , FLM.LandMeasurement_LandDepth_OldValue
	--	   , FLM.LandMeasurement_LandDimension
	--	   , FLM.LandMeasurement_LandDimension_Action
	--	   , FLM.LandMeasurement_LandDimension_OldValue
	--	   , FLM.LandMeasurement_LandDimensionTypeDescription
	--	   , FLM.LandMeasurement_LandDimensionTypeDescription_Action
	--	   , FLM.LandMeasurement_LandDimensionTypeDescription_OldValue
	--	   , FLM.LandMeasurement_LandWidth
	--	   , FLM.LandMeasurement_LandWidth_Action
	--	   , FLM.LandMeasurement_LandWidth_OldValue
	--	   , FN.FolioDescription_Neighbourhood_NeighbourhoodCode
	--	   , FN.FolioDescription_Neighbourhood_NeighbourhoodCode_Action
	--	   , FN.FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue
	--	   , FN.Neighbourhood_NeighbourhoodDescription
	--	   , FN.FolioDescription_Neighbourhood_NeighbourhoodDescription_Action
	--	   , FN.FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue
	--	   , FRD.RegionalDistrict_DistrictDescription
	--	   , FRD.RegionalDistrict_DistrictDescription_Action
	--	   , FRD.RegionalDistrict_DistrictDescription_OldValue
	--	   , FSD.SchoolDistrict_DistrictDescription
	--	   , FSD.SchoolDistrict_DistrictDescription_Action
	--	   , FSD.SchoolDistrict_DistrictDescription_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Block
	--	   , FLD.LegalDescriptions_LegalDescription_Block_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Block_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_DistrictLot
	--	   , FLD.LegalDescriptions_LegalDescription_DistrictLot_Action
	--	   , FLD.LegalDescriptions_LegalDescription_DistrictLot_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_ExceptPlan
	--	   , FLD.LegalDescriptions_LegalDescription_ExceptPlan_Action
	--	   , FLD.LegalDescriptions_LegalDescription_ExceptPlan_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_FormattedLegalDescription
	--	   , FLD.LegalDescriptions_LegalDescription_FormattedLegalDescription_Action
	--	   , FLD.LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_ID
	--	   , FLD.LegalDescriptions_LegalDescription_LandDistrict
	--	   , FLD.LegalDescriptions_LegalDescription_LandDistrict_Action
	--	   , FLD.LegalDescriptions_LegalDescription_LandDistrict_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_LandDistrictDescription
	--	   , FLD.LegalDescriptions_LegalDescription_LandDistrictDescription_Action
	--	   , FLD.LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_LeaseLicenceNumber
	--	   , FLD.LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action
	--	   , FLD.LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_LegalText
	--	   , FLD.LegalDescriptions_LegalDescription_LegalText_Action
	--	   , FLD.LegalDescriptions_LegalDescription_LegalText_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Lot
	--	   , FLD.LegalDescriptions_LegalDescription_Lot_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Lot_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Meridian
	--	   , FLD.LegalDescriptions_LegalDescription_Meridian_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Meridian_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_MeridianShort
	--	   , FLD.LegalDescriptions_LegalDescription_MeridianShort_Action
	--	   , FLD.LegalDescriptions_LegalDescription_MeridianShort_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Parcel
	--	   , FLD.LegalDescriptions_LegalDescription_Parcel_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Parcel_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Part1
	--	   , FLD.LegalDescriptions_LegalDescription_Part1_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Part1_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Part2
	--	   , FLD.LegalDescriptions_LegalDescription_Part2_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Part2_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Part3
	--	   , FLD.LegalDescriptions_LegalDescription_Part3_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Part3_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Part4
	--	   , FLD.LegalDescriptions_LegalDescription_Part4_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Part4_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_PID
	--	   , FLD.LegalDescriptions_LegalDescription_PID_Action
	--	   , FLD.LegalDescriptions_LegalDescription_PID_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Plan
	--	   , FLD.LegalDescriptions_LegalDescription_Plan_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Plan_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Portion
	--	   , FLD.LegalDescriptions_LegalDescription_Portion_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Portion_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Range
	--	   , FLD.LegalDescriptions_LegalDescription_Range_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Range_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Section
	--	   , FLD.LegalDescriptions_LegalDescription_Section_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Section_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_StrataLot
	--	   , FLD.LegalDescriptions_LegalDescription_StrataLot_Action
	--	   , FLD.LegalDescriptions_LegalDescription_StrataLot_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_SubBlock
	--	   , FLD.LegalDescriptions_LegalDescription_SubBlock_Action
	--	   , FLD.LegalDescriptions_LegalDescription_SubBlock_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_SubLot
	--	   , FLD.LegalDescriptions_LegalDescription_SubLot_Action
	--	   , FLD.LegalDescriptions_LegalDescription_SubLot_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_Township
	--	   , FLD.LegalDescriptions_LegalDescription_Township_Action
	--	   , FLD.LegalDescriptions_LegalDescription_Township_OldValue
	--	   , FLD.LegalDescriptions_LegalDescription_LegalSubdivision
	--	   , FLD.LegalDescriptions_LegalDescription_LegalSubdivision_Action
	--	   , FLD.LegalDescriptions_LegalDescription_LegalSubdivision_OldValue
	--	   , FRS.Sales_Sale_Action
	--	   , FRS.Sales_Sale_ConveyanceDate
	--	   , FRS.Sale_ConveyanceDate_Action
	--	   , FRS.Sale_ConveyanceDate_OldValue
	--	   , FRS.Sales_Sale_ConveyancePrice
	--	   , FRS.Sales_Sale_ConveyancePrice_Action
	--	   , FRS.Sales_Sale_ConveyancePrice_OldValue
	--	   , FRS.Sales_Sale_ID
	--	   FROM #FolioRecords FR
	--		LEFT JOIN #FolioRecordsAddresses FA ON FR.FolioRecordID = FA.FolioRecordID AND FR.RollNumber = FA.RollNumber
	--		LEFT JOIN #FolioRecordsLegalDescription FLD ON FR.FolioRecordID = FLD.FolioRecord_ID AND FR.RollNumber = FLD.RollNumber
	--		LEFT JOIN #RegionalDistrict FRD ON  FR.FolioRecordID = FRD.FolioRecordID AND FR.RollNumber = FRD.RollNumber
	--		--LEFT JOIN #RegionalHospitalDistrict FHD ON FR.FolioRecordID = FHD.FolioRecordID AND FR.RollNumber = FHD.RollNumber
	--		LEFT JOIN #FolioRecordsSchoolDistrict FSD ON FR.FolioRecordID = FSD.FolioRecordID AND FR.RollNumber = FSD.RollNumber
	--		LEFT JOIN #FolioRecordsLandMesaurement FLM ON FR.FolioRecordID = FLM.FolioRecordID AND FR.RollNumber = FLM.RollNumber
	--		LEFT JOIN #FolioRecordsNeighbourhood FN ON FR.FolioRecordID = FN.FolioRecordID AND FR.RollNumber = FN.RollNumber
	--		LEFT JOIN #FolioDescription FD ON FR.FolioRecordID = FD.FolioRecordID AND FR.RollNumber = FD.RollNumber
	--		LEFT JOIN #FolioRecordsSales FRS ON FR.FolioRecordID = FRS.FolioRecordID AND FR.RollNumber = FRS.RollNumber
	--		--LEFT JOIN #FolioRecordsPredominantManualClass FPC ON FR.FolioRecordID = FPC.FolioRecordID AND FR.RollNumber = FPC.RollNumber
	--		--LEFT JOIN #FolioRecordsManagedForest FMC ON FR.FolioRecordID = FMC.FolioRecordID AND FR.RollNumber = FMC.RollNumber
	--		--LEFT JOIN #FolioRecordsFolioAdd FAA ON FR.FolioRecordID = FAA.FolioRecordID AND FR.RollNumber = FAA.RollNumber
	--		--LEFT JOIN #FolioRecordsFolioDelete FAD ON FR.FolioRecordID = FAD.FolioRecordID AND FR.RollNumber = FAD.RollNumber
	--		LEFT JOIN  #FolioActionFolioDelete FAFD ON FR.FolioRecordID = FAFD.FolioRecordID AND FR.RollNumber = FAFD.RollNumber
	--		LEFT JOIN  #FolioActionFolioAdd FAADD ON FR.FolioRecordID = FAADD.FolioRecordID AND FR.RollNumber = FAADD.RollNumber
			
		    
   END