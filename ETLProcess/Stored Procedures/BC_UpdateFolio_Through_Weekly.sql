



CREATE PROCEDURE [ETLProcess].[BC_UpdateFolio_Through_Weekly]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Raghavendra
-- DATE			: 11/25/2022
-- PURPOSE		: Update BCA Weekly files to dbo.BC_UPTO_DATE
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 11/25/2022	Raghavendra	Original Version
******************************************************************************************/

	DECLARE @ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';
	DECLARE @ProcessName VARCHAR(100) ; 
	DECLARE @TableName VARCHAR(100)='BC_ALL_Assessment_Weekly'; 
	DECLARE @ErrorSchema varchar(300)='StageProcessErr.';        
    DECLARE @HistorySchema varchar(300)='SourceHistory.'; 
	DECLARE @ErrorProcedure VARCHAR(100); 
	DECLARE @IsError BIT=0;     

	SELECT @ErrorProcedure= s.name+'.'+o.name         
	FROM         
	SYS.OBJECTS O            
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;        
        
	 SET @ProcessName='BC_ALL_Assessment_Weekly';
	 SET @TableName=@ProcessName;

	BEGIN TRY 
		--BEGIN TRAN 

			SET NOCOUNT ON;

			DECLARE @ModifiedDate datetime;
			SELECT  @ModifiedDate=GETUTCDATE(); --'2022-09-08 15:53:47.400'

			DROP TABLE IF EXISTS #ID_MaxDate;

			SELECT FolioRecord_ID,Sub_Id,StartDate AS Max_StartDate, Action_Field INTO #ID_MaxDate FROM 
			(
				--Lega1 Description
				select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID AS Sub_Id ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Block_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Block_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_DistrictLot_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_DistrictLot_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_ExceptPlan_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_ExceptPlan_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_FormattedLegalDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_FormattedLegalDescription_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LandDistrict_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_LandDistrict_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LandDistrictDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_LandDistrictDescription_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LegalText_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_LegalText_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Lot_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Lot_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Meridian_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Meridian_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_MeridianShort_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_MeridianShort_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Parcel_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Parcel_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part1_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Part1_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part2_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Part2_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part3_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Part3_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part4_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Part4_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_PID_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_PID_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Plan_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Plan_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Portion_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Portion_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Range_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Range_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Section_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Section_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_StrataLot_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_StrataLot_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_SubBlock_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_SubBlock_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_SubLot_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_SubLot_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Township_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_Township_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LegalSubdivision_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LegalDescriptions_LegalDescription_LegalSubdivision_Action ='Change'  group by FolioRecord_ID, LegalDescriptions_LegalDescription_ID

				--Sales
				union all select  FolioRecord_ID, Sales_Sale_ID ,Max(StartDate) as StartDate,'Sales_Sale_ConveyanceDate_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE Sales_Sale_ConveyanceDate_Action ='Change'  group by FolioRecord_ID, Sales_Sale_ID
				union all select  FolioRecord_ID, Sales_Sale_ID ,Max(StartDate) as StartDate,'Sales_Sale_ConveyancePrice_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE Sales_Sale_ConveyancePrice_Action ='Change'  group by FolioRecord_ID, Sales_Sale_ID

				--Folio Address
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_City_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_City_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_PostalZip_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_PostalZip_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_PrimaryFlag_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_PrimaryFlag_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_ProvinceState_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_ProvinceState_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetDirectionSuffix_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_StreetDirectionSuffix_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetName_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_StreetName_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetNumber_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_StreetNumber_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetType_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_StreetType_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID
				union all select  FolioRecord_ID, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_UnitNumber_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioAddresses_FolioAddress_UnitNumber_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID

				-- Base fields
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'RollNumber_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE RollNumber_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'FolioDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioDescription_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'ActualUseDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE ActualUseDescription_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'TenureDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE TenureDescription_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'VacantFlag_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE VacantFlag_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandDepth_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LandMeasurement_LandDepth_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandDimension_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LandMeasurement_LandDimension_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandDimensionTypeDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LandMeasurement_LandDimensionTypeDescription_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandWidth_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE LandMeasurement_LandWidth_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'FolioDescription_Neighbourhood_NeighbourhoodCode_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioDescription_Neighbourhood_NeighbourhoodCode_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'FolioDescription_Neighbourhood_NeighbourhoodDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE FolioDescription_Neighbourhood_NeighbourhoodDescription_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'RegionalDistrict_DistrictDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE RegionalDistrict_DistrictDescription_Action='Change'  group by FolioRecord_ID
				union all select  FolioRecord_ID,'' SubId ,Max(StartDate) as StartDate,'SchoolDistrict_DistrictDescription_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly WHERE SchoolDistrict_DistrictDescription_Action='Change'  group by FolioRecord_ID
			)A

			--SELECT * FROM #ID_MaxDate

			--RollNumber_Action
			UPDATE dbo.BC_UPTO_DATE
			SET ROLLNUMBER=WEEKLY.ROLLNUMBER , LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='RollNumber_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.RollNumber_Action='Change' 
				--and isnull(UPTODATE.RollNumber,'') = isnull(WEEKLY.RollNumber_OldValue,'');

			--ActualUseDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET TenureDescription=WEEKLY.TenureDescription , LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='TenureDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.TenureDescription_Action='Change' 
				--and isnull(UPTODATE.TenureDescription,'') = isnull(WEEKLY.TenureDescription_OldValue,'');

			--TenureDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET ActualUseDescription=WEEKLY.ActualUseDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='ActualUseDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.ActualUseDescription_Action='Change' 
				--and isnull(UPTODATE.ActualUseDescription,'') = isnull(WEEKLY.ActualUseDescription_OldValue,'');


			--VacantFlag_Action
			UPDATE dbo.BC_UPTO_DATE
			SET VacantFlag=WEEKLY.VacantFlag, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='VacantFlag_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.VacantFlag_Action='Change' 
				--and isnull(UPTODATE.VacantFlag,'') = isnull(WEEKLY.VacantFlag_OldValue,'');

			--FolioAddresses_FolioAddress_City_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_City=WEEKLY.FolioAddresses_FolioAddress_City, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_City_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_City_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_City,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_City_OldValue,'');

			--FolioAddresses_FolioAddress_PostalZip_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_PostalZip=WEEKLY.FolioAddresses_FolioAddress_PostalZip, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_PostalZip_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_PostalZip_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_PostalZip,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_PostalZip_OldValue,'');

			--FolioAddresses_FolioAddress_ProvinceState_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_ProvinceState=WEEKLY.FolioAddresses_FolioAddress_ProvinceState, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_ProvinceState_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_ProvinceState_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_ProvinceState,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_ProvinceState_OldValue,'');

			--FolioAddresses_FolioAddress_StreetDirectionSuffix_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_StreetDirectionSuffix=WEEKLY.FolioAddresses_FolioAddress_StreetDirectionSuffix, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_StreetDirectionSuffix_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_StreetDirectionSuffix_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_StreetDirectionSuffix,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue,'');

			--FolioAddresses_FolioAddress_StreetName_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_StreetName=WEEKLY.FolioAddresses_FolioAddress_StreetName, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *,UPTODATE.FolioAddresses_FolioAddress_StreetName,WEEKLY.FolioAddresses_FolioAddress_StreetName
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_StreetName_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_StreetName_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_StreetName,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_StreetName_OldValue,'');

			--FolioAddresses_FolioAddress_StreetNumber_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_StreetNumber=WEEKLY.FolioAddresses_FolioAddress_StreetNumber, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *,UPTODATE.FolioAddresses_FolioAddress_StreetNumber,WEEKLY.FolioAddresses_FolioAddress_StreetNumber
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_StreetNumber_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_StreetNumber_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_StreetNumber,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_StreetNumber_OldValue,'');

			--FolioAddresses_FolioAddress_StreetType_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_StreetType=WEEKLY.FolioAddresses_FolioAddress_StreetType, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.FolioAddresses_FolioAddress_StreetType,WEEKLY.FolioAddresses_FolioAddress_StreetType,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_StreetType_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_StreetType_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_StreetType,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_StreetType_OldValue,'');


			--FolioAddresses_FolioAddress_UnitNumber_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_UnitNumber=WEEKLY.FolioAddresses_FolioAddress_UnitNumber, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.FolioAddresses_FolioAddress_UnitNumber,WEEKLY.FolioAddresses_FolioAddress_UnitNumber,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_UnitNumber_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_UnitNumber_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_UnitNumber,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_UnitNumber_OldValue,'');


			--LandMeasurement_LandDepth_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LandMeasurement_LandDepth=WEEKLY.LandMeasurement_LandDepth, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='LandMeasurement_LandDepth_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LandMeasurement_LandDepth_Action='Change' 
				--and isnull(UPTODATE.LandMeasurement_LandDepth,'') = isnull(WEEKLY.LandMeasurement_LandDepth_OldValue,'');


			--LandMeasurement_LandDimension_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LandMeasurement_LandDimension=WEEKLY.LandMeasurement_LandDimension, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='LandMeasurement_LandDimension_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LandMeasurement_LandDimension_Action='Change' 
				--and isnull(UPTODATE.LandMeasurement_LandDimension,'') = isnull(WEEKLY.LandMeasurement_LandDimension_OldValue,'');

			--LandMeasurement_LandDimensionTypeDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LandMeasurement_LandDimensionTypeDescription=WEEKLY.LandMeasurement_LandDimensionTypeDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='LandMeasurement_LandDimensionTypeDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LandMeasurement_LandDimensionTypeDescription_Action='Change' 
				--and isnull(UPTODATE.LandMeasurement_LandDimensionTypeDescription,'') = isnull(WEEKLY.LandMeasurement_LandDimensionTypeDescription_OldValue,'');


			--LandMeasurement_LandWidth_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LandMeasurement_LandWidth=WEEKLY.LandMeasurement_LandWidth, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='LandMeasurement_LandWidth_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LandMeasurement_LandWidth_Action='Change' 
				--and isnull(UPTODATE.LandMeasurement_LandWidth,'') = isnull(WEEKLY.LandMeasurement_LandWidth_OldValue,'');

			--FolioDescription_Neighbourhood_NeighbourhoodCode_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioDescription_Neighbourhood_NeighbourhoodCode=WEEKLY.FolioDescription_Neighbourhood_NeighbourhoodCode, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='FolioDescription_Neighbourhood_NeighbourhoodCode_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioDescription_Neighbourhood_NeighbourhoodCode_Action='Change' 
				--and isnull(UPTODATE.FolioDescription_Neighbourhood_NeighbourhoodCode,'') = isnull(WEEKLY.LandMeasurement_LandWidth_OldValue,'');

			--FolioDescription_Neighbourhood_NeighbourhoodDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioDescription_Neighbourhood_NeighbourhoodDescription=WEEKLY.FolioDescription_Neighbourhood_NeighbourhoodDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='FolioDescription_Neighbourhood_NeighbourhoodDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioDescription_Neighbourhood_NeighbourhoodDescription_Action='Change' 
				--and isnull(UPTODATE.FolioDescription_Neighbourhood_NeighbourhoodDescription,'') = isnull(WEEKLY.FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue,'');


			--RegionalDistrict_DistrictDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET RegionalDistrict_DistrictDescription=WEEKLY.RegionalDistrict_DistrictDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='RegionalDistrict_DistrictDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.RegionalDistrict_DistrictDescription_Action='Change' 
				--and isnull(UPTODATE.RegionalDistrict_DistrictDescription,'') = isnull(WEEKLY.RegionalDistrict_DistrictDescription_OldValue,'');


			--SchoolDistrict_DistrictDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET SchoolDistrict_DistrictDescription=WEEKLY.SchoolDistrict_DistrictDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  *
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
			JOIN #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID 
				and maxdate.Action_Field='SchoolDistrict_DistrictDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.SchoolDistrict_DistrictDescription_Action='Change' 
			--and isnull(UPTODATE.SchoolDistrict_DistrictDescription,'')= isnull(WEEKLY.SchoolDistrict_DistrictDescription_OldValue,'');


			--LegalDescriptions_LegalDescription_Block_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Block=WEEKLY.LegalDescriptions_LegalDescription_Block, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_Block,WEEKLY.LegalDescriptions_LegalDescription_Block,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Block_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Block_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Block,'') = isnull(WEEKLY.LegalDescriptions_LegalDescription_Block_OldValue,'');

			--LegalDescriptions_LegalDescription_DistrictLot_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_DistrictLot=WEEKLY.LegalDescriptions_LegalDescription_DistrictLot, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_DistrictLot,WEEKLY.LegalDescriptions_LegalDescription_DistrictLot,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_DistrictLot_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_DistrictLot_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_DistrictLot,'') = isnull(WEEKLY.LegalDescriptions_LegalDescription_DistrictLot_OldValue,'');

			--LegalDescriptions_LegalDescription_ExceptPlan_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_ExceptPlan=WEEKLY.LegalDescriptions_LegalDescription_ExceptPlan, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_ExceptPlan,WEEKLY.LegalDescriptions_LegalDescription_ExceptPlan,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_ExceptPlan_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_ExceptPlan_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_ExceptPlan,'') = isnull(WEEKLY.LegalDescriptions_LegalDescription_ExceptPlan_OldValue,'');


			--LegalDescriptions_LegalDescription_FormattedLegalDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_FormattedLegalDescription=WEEKLY.LegalDescriptions_LegalDescription_FormattedLegalDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_FormattedLegalDescription,WEEKLY.LegalDescriptions_LegalDescription_FormattedLegalDescription,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_FormattedLegalDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_FormattedLegalDescription_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_FormattedLegalDescription,'') = isnull(WEEKLY.LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue,'');

			--LegalDescriptions_LegalDescription_LandDistrict_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_LandDistrict=WEEKLY.LegalDescriptions_LegalDescription_LandDistrict, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_LandDistrict,WEEKLY.LegalDescriptions_LegalDescription_LandDistrict,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_LandDistrict_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_LandDistrict_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_LandDistrict,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_LandDistrict_OldValue,'');

			--LegalDescriptions_LegalDescription_LandDistrictDescription_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_LandDistrictDescription=WEEKLY.LegalDescriptions_LegalDescription_LandDistrictDescription, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_LandDistrictDescription,WEEKLY.LegalDescriptions_LegalDescription_LandDistrictDescription,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_LandDistrictDescription_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_LandDistrictDescription_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_LandDistrictDescription,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue,'');

			--LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_LeaseLicenceNumber=WEEKLY.LegalDescriptions_LegalDescription_LeaseLicenceNumber, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_LeaseLicenceNumber,WEEKLY.LegalDescriptions_LegalDescription_LeaseLicenceNumber,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_LeaseLicenceNumber,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue,'');

			--LegalDescriptions_LegalDescription_LegalText_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_LegalText=WEEKLY.LegalDescriptions_LegalDescription_LegalText, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.LegalDescriptions_LegalDescription_LegalText,WEEKLY.LegalDescriptions_LegalDescription_LegalText,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_LegalText_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_LegalText_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_LegalText,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_LegalText_OldValue,'');

				-------------------------------------------------------------------------------------------------------------------------
			--LegalDescriptions_LegalDescription_Lot_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Lot=WEEKLY.LegalDescriptions_LegalDescription_Lot, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Lot_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Lot,WEEKLY.LegalDescriptions_LegalDescription_Lot,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Lot_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Lot_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Lot,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Lot_OldValue,'');

			--LegalDescriptions_LegalDescription_Meridian_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Meridian=WEEKLY.LegalDescriptions_LegalDescription_Meridian, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Meridian_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Meridian,WEEKLY.LegalDescriptions_LegalDescription_Meridian,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Meridian_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Meridian_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Meridian,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Meridian_OldValue,'');


			--LegalDescriptions_LegalDescription_MeridianShort
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_MeridianShort=WEEKLY.LegalDescriptions_LegalDescription_MeridianShort, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Lot_OldValue,UPTODATE.LegalDescriptions_LegalDescription_MeridianShort,WEEKLY.LegalDescriptions_LegalDescription_MeridianShort,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_MeridianShort_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_MeridianShort_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_MeridianShort,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_MeridianShort_OldValue,'');

			--LegalDescriptions_LegalDescription_Parcel_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Parcel=WEEKLY.LegalDescriptions_LegalDescription_Parcel, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Parcel_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Parcel,WEEKLY.LegalDescriptions_LegalDescription_Parcel,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Parcel_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Parcel_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Parcel,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Parcel_OldValue,'');

			--LegalDescriptions_LegalDescription_Part1_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Part1=WEEKLY.LegalDescriptions_LegalDescription_Part1, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Part1_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Part1,WEEKLY.LegalDescriptions_LegalDescription_Part1,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Part1_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Part1_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Part1,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Part1_OldValue,'');

			--LegalDescriptions_LegalDescription_Part2_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Part2=WEEKLY.LegalDescriptions_LegalDescription_Part2, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Part2_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Part2,WEEKLY.LegalDescriptions_LegalDescription_Part2,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Part2_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Part2_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Part2,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Part2_OldValue,'');

			--LegalDescriptions_LegalDescription_Part3_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Part3=WEEKLY.LegalDescriptions_LegalDescription_Part3, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Part3_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Part3,WEEKLY.LegalDescriptions_LegalDescription_Part3,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Part3_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Part3_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Part3,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Part3_OldValue,'');

			--LegalDescriptions_LegalDescription_Part4_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Part4=WEEKLY.LegalDescriptions_LegalDescription_Part4, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Part4_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Part4,WEEKLY.LegalDescriptions_LegalDescription_Part4,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Part4_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Part4_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Part4,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Part4_OldValue,'');


			--LegalDescriptions_LegalDescription_PID_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_PID=WEEKLY.LegalDescriptions_LegalDescription_PID, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_PID_OldValue,UPTODATE.LegalDescriptions_LegalDescription_PID,WEEKLY.LegalDescriptions_LegalDescription_PID,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_PID_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_PID_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_PID,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_PID_OldValue,'');

			--LegalDescriptions_LegalDescription_Plan_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Plan=WEEKLY.LegalDescriptions_LegalDescription_Plan, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Plan_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Plan,WEEKLY.LegalDescriptions_LegalDescription_Plan,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Plan_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Plan_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Plan,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Plan_OldValue,'');

			--LegalDescriptions_LegalDescription_Portion_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Portion=WEEKLY.LegalDescriptions_LegalDescription_Portion, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Portion_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Portion,WEEKLY.LegalDescriptions_LegalDescription_Portion,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Portion_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Portion_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Portion,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Portion_OldValue,'');

			--LegalDescriptions_LegalDescription_Range_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Range=WEEKLY.LegalDescriptions_LegalDescription_Range, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Range_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Range,WEEKLY.LegalDescriptions_LegalDescription_Range,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Range_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Range_Action='Change'
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Range,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Range_OldValue,'');

			--LegalDescriptions_LegalDescription_Range_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Section=WEEKLY.LegalDescriptions_LegalDescription_Section, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Section_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Section,WEEKLY.LegalDescriptions_LegalDescription_Section,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Section_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Section_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Section,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Section_OldValue,'');

			--LegalDescriptions_LegalDescription_StrataLot_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_StrataLot=WEEKLY.LegalDescriptions_LegalDescription_StrataLot, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Section_OldValue,UPTODATE.LegalDescriptions_LegalDescription_StrataLot,WEEKLY.LegalDescriptions_LegalDescription_StrataLot,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_StrataLot_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_StrataLot_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_StrataLot,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_StrataLot_OldValue,'');

			--LegalDescriptions_LegalDescription_SubBlock_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_SubBlock=WEEKLY.LegalDescriptions_LegalDescription_SubBlock, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_SubBlock_OldValue,UPTODATE.LegalDescriptions_LegalDescription_SubBlock,WEEKLY.LegalDescriptions_LegalDescription_SubBlock,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_SubBlock_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_SubBlock_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_SubBlock,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_SubBlock_OldValue,'');

			--LegalDescriptions_LegalDescription_SubLot_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_SubLot=WEEKLY.LegalDescriptions_LegalDescription_SubLot, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_SubLot_OldValue,UPTODATE.LegalDescriptions_LegalDescription_SubLot,WEEKLY.LegalDescriptions_LegalDescription_SubLot,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_SubLot' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_SubLot_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_SubLot,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_SubLot_OldValue,'');

			--LegalDescriptions_LegalDescription_Township_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_Township=WEEKLY.LegalDescriptions_LegalDescription_Township, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_Township_OldValue,UPTODATE.LegalDescriptions_LegalDescription_Township,WEEKLY.LegalDescriptions_LegalDescription_Township,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_Township_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_Township_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_Township,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_Township_OldValue,'');

			--LegalDescriptions_LegalDescription_LegalSubdivision_Action
			UPDATE dbo.BC_UPTO_DATE
			SET LegalDescriptions_LegalDescription_LegalSubdivision=WEEKLY.LegalDescriptions_LegalDescription_LegalSubdivision, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.LegalDescriptions_LegalDescription_LegalSubdivision_OldValue,UPTODATE.LegalDescriptions_LegalDescription_LegalSubdivision,WEEKLY.LegalDescriptions_LegalDescription_LegalSubdivision,WEEKLY.LegalDescriptions_LegalDescription_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.LegalDescriptions_LegalDescription_ID=WEEKLY.LegalDescriptions_LegalDescription_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.LegalDescriptions_LegalDescription_ID=maxdate.Sub_Id
				and maxdate.Action_Field='LegalDescriptions_LegalDescription_LegalSubdivision_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.LegalDescriptions_LegalDescription_LegalSubdivision_Action='Change' 
				--and isnull(UPTODATE.LegalDescriptions_LegalDescription_LegalSubdivision,'')=isnull(WEEKLY.LegalDescriptions_LegalDescription_LegalSubdivision_OldValue,'');

			--Sales_Sale_ConveyanceDate_Action
			UPDATE dbo.BC_UPTO_DATE
			SET Sales_Sale_ConveyanceDate=WEEKLY.Sales_Sale_ConveyanceDate, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.Sales_Sale_ConveyanceDate_OldValue,UPTODATE.Sales_Sale_ConveyanceDate,WEEKLY.Sales_Sale_ConveyanceDate,WEEKLY.Sales_Sale_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.Sales_Sale_ID=WEEKLY.Sales_Sale_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.Sales_Sale_ID=maxdate.Sub_Id
				and maxdate.Action_Field='Sales_Sale_ConveyanceDate_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.Sales_Sale_ConveyanceDate_Action='Change' 
				--and isnull(UPTODATE.Sales_Sale_ConveyanceDate,'')=isnull(WEEKLY.Sales_Sale_ConveyanceDate_OldValue,'');

			--Sales_Sale_ConveyancePrice_Action
			UPDATE dbo.BC_UPTO_DATE
			SET Sales_Sale_ConveyancePrice=WEEKLY.Sales_Sale_ConveyancePrice, LastModifiedDateUTC=@ModifiedDate
			--SELECT  WEEKLY.Sales_Sale_ConveyancePrice_OldValue,UPTODATE.Sales_Sale_ConveyancePrice,WEEKLY.Sales_Sale_ConveyancePrice,WEEKLY.Sales_Sale_ID ,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.Sales_Sale_ID=WEEKLY.Sales_Sale_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.Sales_Sale_ID=maxdate.Sub_Id
				and maxdate.Action_Field='Sales_Sale_ConveyancePrice_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.Sales_Sale_ConveyancePrice_Action='Change' 
				--and isnull(UPTODATE.Sales_Sale_ConveyancePrice,'')=isnull(WEEKLY.Sales_Sale_ConveyancePrice_OldValue,'');

			--FolioAddresses_FolioAddress_PrimaryFlag_Action
			UPDATE dbo.BC_UPTO_DATE
			SET FolioAddresses_FolioAddress_PrimaryFlag=WEEKLY.FolioAddresses_FolioAddress_PrimaryFlag, LastModifiedDateUTC=@ModifiedDate
			--SELECT  UPTODATE.FolioAddresses_FolioAddress_UnitNumber,WEEKLY.FolioAddresses_FolioAddress_UnitNumber,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY 
				ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID 
				AND UPTODATE.FolioAddresses_FolioAddress_ID=WEEKLY.FolioAddresses_FolioAddress_ID 
			join #ID_MaxDate maxdate on WEEKLY.FolioRecord_ID=maxdate.FolioRecord_ID and WEEKLY.FolioAddresses_FolioAddress_ID=maxdate.Sub_Id
				and maxdate.Action_Field='FolioAddresses_FolioAddress_PrimaryFlag_Action' 
				and weekly.StartDate=maxdate.Max_StartDate
			where WEEKLY.FolioAddresses_FolioAddress_PrimaryFlag_Action='Change' 
				--and isnull(UPTODATE.FolioAddresses_FolioAddress_PrimaryFlag,'') = isnull(WEEKLY.FolioAddresses_FolioAddress_PrimaryFlag_OldValue,'');

			/*------------------------------------------------------------------------------------------------------------------------------------
			Update Sales
			--------------------------------------------------------------------------------------------------------------------------------------*/
			
			--VacantFlag_Action
			UPDATE UPTODATE
			SET Sales_Sale_ConveyanceDate=WEEKLY.Sales_Sale_ConveyanceDate
			, LastModifiedDateUTC=@ModifiedDate
			--SELECT  Sales_Sale_ConveyanceDate=WEEKLY.Sales_Sale_ConveyanceDate,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
				AND UPTODATE.Sales_Sale_ID=WEEKLY.Sales_Sale_ID
			where WEEKLY.Sales_Sale_ConveyanceDate_Action='Change' 
			--AND WEEKLY.FolioRecord_ID='D0001JJZX6' AND WEEKLY.Sales_Sale_ID='D00010QEBW'


			UPDATE UPTODATE
			SET Sales_Sale_ConveyancePrice=WEEKLY.Sales_Sale_ConveyancePrice
			, LastModifiedDateUTC=@ModifiedDate
			-- SELECT  weekly.Sales_Sale_ConveyancePrice,WEEKLY.Sales_Sale_ConveyancePrice,*
			FROM DBO.BC_UPTO_DATE  UPTODATE
			JOIN StageLanding.BC_ALL_Assessment_Weekly WEEKLY ON UPTODATE.FolioRecord_ID=WEEKLY.FolioRecord_ID
				AND UPTODATE.Sales_Sale_ID=WEEKLY.Sales_Sale_ID
			where WEEKLY.Sales_Sale_ConveyancePrice_Action='Change' 
			--AND WEEKLY.FolioRecord_ID='D0001JJZX6' AND WEEKLY.Sales_Sale_ID='D00010QEBW'
				--and isnull(UPTODATE.VacantFlag,'') = isnull(WEEKLY.VacantFlag_OldValue,'');

			

			/*------------------------------------------------------------------------------------------------------------------------------------
			Update HashBytes
			--------------------------------------------------------------------------------------------------------------------------------------*/

			--Update Hashbytes
			UPDATE dbo.BC_UPTO_DATE
			SET [HashBytes]=HASHBYTES('SHA2_512', CONCAT_WS('|', FolioRecord_ID, RollYear, AssessmentAreaDescription, JurisdictionCode, JurisdictionDescription
						, RollNumber, ActualUseDescription, VacantFlag, TenureDescription, FolioAddresses_FolioAddress_City, FolioAddresses_FolioAddress_ID
						, FolioAddresses_FolioAddress_PostalZip, FolioAddresses_FolioAddress_PrimaryFlag, FolioAddresses_FolioAddress_ProvinceState
						, FolioAddresses_FolioAddress_StreetDirectionSuffix, FolioAddresses_FolioAddress_StreetName, FolioAddresses_FolioAddress_StreetNumber
						, FolioAddresses_FolioAddress_StreetType, FolioAddresses_FolioAddress_UnitNumber, LandMeasurement_LandDepth, LandMeasurement_LandDimension
						, LandMeasurement_LandDimensionTypeDescription, LandMeasurement_LandWidth, FolioDescription_Neighbourhood_NeighbourhoodCode
						, FolioDescription_Neighbourhood_NeighbourhoodDescription, RegionalDistrict_DistrictDescription, SchoolDistrict_DistrictDescription
						, LegalDescriptions_LegalDescription_Block, LegalDescriptions_LegalDescription_DistrictLot, LegalDescriptions_LegalDescription_ExceptPlan
						, LegalDescriptions_LegalDescription_FormattedLegalDescription, LegalDescriptions_LegalDescription_ID, LegalDescriptions_LegalDescription_LandDistrict
						, LegalDescriptions_LegalDescription_LandDistrictDescription, LegalDescriptions_LegalDescription_LeaseLicenceNumber, LegalDescriptions_LegalDescription_LegalText
						, LegalDescriptions_LegalDescription_Lot, LegalDescriptions_LegalDescription_Meridian, LegalDescriptions_LegalDescription_MeridianShort
						, LegalDescriptions_LegalDescription_Parcel, LegalDescriptions_LegalDescription_Part1, LegalDescriptions_LegalDescription_Part2
						, LegalDescriptions_LegalDescription_Part3, LegalDescriptions_LegalDescription_Part4, LegalDescriptions_LegalDescription_PID, LegalDescriptions_LegalDescription_Plan
						, LegalDescriptions_LegalDescription_Portion, LegalDescriptions_LegalDescription_Range, LegalDescriptions_LegalDescription_Section
						, LegalDescriptions_LegalDescription_StrataLot, LegalDescriptions_LegalDescription_SubBlock, LegalDescriptions_LegalDescription_SubLot
						, LegalDescriptions_LegalDescription_LegalSubdivision, LegalDescriptions_LegalDescription_Township, Sales_Sale_ConveyanceDate, Sales_Sale_ConveyancePrice
						, Sales_Sale_ID))
			WHERE LastModifiedDateUTC=@ModifiedDate ;--'2022-09-08 15:53:47.400' --
		--COMMIT TRAN;
	END TRY

	BEGIN CATCH        
      --UPDATE Stage.ExternalFileslist SET IsError=1 WHERE FileName=@ExternalFileName;        
        --ROLLBACK TRAN;

       SET @IsError=1        
        
       EXEC ETLProcess.AuditLog        
        @ProcessCategory = @ProcessCategory        
       , @Phase = 'ProcessHistory'        
       , @ProcessName = @ProcessName        
       , @Stage ='Error BCA_Weekly_Update'        
       , @Status = 'Error'        
       , @CurrentStatus = 'Error'        
       , @Inserts = 0;        
        
       EXEC ETLProcess.AuditLog        
        @ProcessCategory = @ProcessCategory        
       , @Phase = 'Process'        
       , @ProcessName = @ProcessName        
       , @Status = 'Error'        
       , @CurrentStatus = 'Error'        
       , @Stage = 'Landing';         
        
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
              
               
       EXEC ETLProcess.EmailNotification        
        @ProcessCategory=@ProcessCategory        
       , @ProcessName= @ProcessName        
       , @ProcessStage='Landing'        
       , @ErrorMessage='Failed to Load BCA Weekly Update'        
       , @IsError='Yes';        
      END CATCH 

	IF @IsError=1        
	  THROW 50005, N'An error occurred while updating BCA Weekly Update', 1;        
 
END