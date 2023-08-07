


CREATE PROCEDURE [ETLProcess].[BC_UpdateFolio_Through_Weekly_DTC_Entity]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Raghavendra
-- DATE			: 11/25/2022
-- PURPOSE		: Update BCA Weekly files to Entity tables
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
			SET NOCOUNT ON;
			declare @modifiedDate datetime; set @modifiedDate=GETUTCDATE();

			/*--------------------------------------------------------------------------------------------------------------------------------------------
			Update entity tables for Legal Description
			--------------------------------------------------------------------------------------------------------------------------------------------*/
					
			DROP TABLE IF EXISTS #ID_MaxDate_Legal;

			SELECT FolioRecord_ID,RollNumber, Sub_Id,StartDate AS Max_StartDate, Action_Field INTO #ID_MaxDate_Legal FROM 
			(
				--Lega1 Description
				select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID AS Sub_Id ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Block' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Block_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_DistrictLot' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_DistrictLot_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_ExceptPlan' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_ExceptPlan_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_FormattedLegalDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_FormattedLegalDescription_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LandDistrict' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_LandDistrict_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LandDistrictDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_LandDistrictDescription_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LeaseLicenceNumber' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LegalText' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_LegalText_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Lot' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Lot_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Meridian' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Meridian_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_MeridianShort' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_MeridianShort_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Parcel' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Parcel_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part1' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Part1_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part2' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Part2_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part3' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Part3_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Part4' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Part4_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_PID' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_PID_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Plan' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Plan_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Portion' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Portion_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Range' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Range_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Section' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Section_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_StrataLot' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_StrataLot_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_SubBlock' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_SubBlock_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_SubLot' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_SubLot_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_Township' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_Township_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
				union all select  FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID ,Max(StartDate) as StartDate,'LegalDescriptions_LegalDescription_LegalSubdivision' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LegalDescriptions_LegalDescription_LegalSubdivision_Action ='Change'  group by FolioRecord_ID, RollNumber,  LegalDescriptions_LegalDescription_ID
			)A;
			-- select * from #ID_MaxDate_Legal --3151

			

			DECLARE @Cnt_ID_MaxDate_Legal int=0;
			SELECT @Cnt_ID_MaxDate_Legal=COUNT(*) FROM #ID_MaxDate_Legal;

			IF(@Cnt_ID_MaxDate_Legal>0)
			BEGIN

				Print 'Updating Legal Description fields'
				DROP TABLE IF EXISTS #OldValueNewValueLegalDescription1;

				create table #OldValueNewValueLegalDescription1
				(FolioRecord_id nvarchar(500)
				,LegalDescriptions_LegalDescription_ID nvarchar(500)
				,Action_Field nvarchar(500)
				,RollNumber nvarchar(500)
				,New_Value nvarchar(max)
				,Code nvarchar(500)
				,Old_Value nvarchar(max)
				);

				--drop table if exists #StartDate_rank;
				drop table if exists #ID_MaxDate_Legal_RNK;

				SELECT mx.*, bc.Code,ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS StartDate_rnk INTO #ID_MaxDate_Legal_RNK
				FROM #ID_MaxDate_Legal mx
				JOIN  dbo.BC_UPTO_DATE bc on mx.FolioRecord_ID=bc.FolioRecord_ID and bc.LegalDescriptions_LegalDescription_ID=mx.Sub_Id and bc.code is not null
				-- select count(*) from #ID_MaxDate_Legal_RNK; select count(*) from #ID_MaxDate_Legal_RNK1;
				-- select  Sub_Id from #ID_MaxDate_Legal_RNK where Sub_Id not in ( select Sub_Id from #ID_MaxDate_Legal_RNK1);

				 

				declare @LoopCnt_Legal int=1;
				declare @Max_LoopCnt_Legal int=0;
				SELECT @Max_LoopCnt_Legal=isnull(max(StartDate_rnk),0) from #ID_MaxDate_Legal_RNK
				 select @LoopCnt_Legal, @Max_LoopCnt_Legal, getdate()

				WHILE(@LoopCnt_Legal <=@Max_LoopCnt_Legal)
				BEGIN
					--SELECT @LoopCnt_Legal
					declare @OldValueNewValueLegalDescription as nvarchar(max) = N'';

							select @OldValueNewValueLegalDescription = @OldValueNewValueLegalDescription + 'select FolioRecord_ID, LegalDescriptions_LegalDescription_ID,'''
							+ md.Action_Field +''' as action_Field, '''
							+ md.RollNumber +''' as RollNumber, '
							+ md.action_field + ' as New_Value, '''
							+ md.Code + ''' as code,'
							+ md.action_field + '_OldValue as Old_Value '
							+ 'from StageLanding.BC_ALL_Assessment_Weekly_Current WHERE '
							+ ' FolioRecord_ID = '''+ md.FolioRecord_ID +''' and '
							+ ' LegalDescriptions_LegalDescription_ID = '''+ Sub_Id +''' and '
							+ ' RollNumber = '''+ md.RollNumber +''' union all '
							from #ID_MaxDate_Legal_RNK md 
							where md.action_field not in ('FolioAddresses_FolioAddress') and md.action_field like '%LegalDescription%'
								and md.StartDate_rnk=@LoopCnt_Legal
							--and FolioRecord_ID='A0000KSQTG' AND sub_Id='A000012H5U' and RollNumber='19350000';

							-- remove last N' UNION ALL '
							if len(@OldValueNewValueLegalDescription) > 11
								set @OldValueNewValueLegalDescription = left(@OldValueNewValueLegalDescription, len(@OldValueNewValueLegalDescription) - 10)

								--select @OldValueNewValueLegalDescription;

							SET @OldValueNewValueLegalDescription ='INSERT INTO #OldValueNewValueLegalDescription1 '+ @OldValueNewValueLegalDescription
								--SELECT @OldValueNewValueLegalDescription;
	
							EXECUTE sp_executeSql @OldValueNewValueLegalDescription

							--SELECT * FROM #OldValueNewValueLegalDescription1
							SET @LoopCnt_Legal=@LoopCnt_Legal+1
				END

				--select * from #PossibleLegalRecords_Address;


				DROP TABLE IF EXISTS #KeyValuesLegalDescription;

				-- GET MasterAddressID and KeyValues
				SELECT *,ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS RNK
				into #KeyValuesLegalDescription
				FROM (
					Select distinct OldValueNewValueLegalDescription.*
					,ETLSourceMapping.SourceColumnName ,ETLSourceMapping.DestinationColumnName
					,columns.TABLE_NAME
					from  #OldValueNewValueLegalDescription1 OldValueNewValueLegalDescription
					INNER JOIN DBO.Property Pr ON OldValueNewValueLegalDescription.Code=pr.Code
					INNER JOIN ETLProcess.ETLSourceMapping ETLSourceMapping on OldValueNewValueLegalDescription.Action_Field=ETLSourceMapping.SourceColumnName 
						AND Pr.ProvinceCode='BC'
					JOIN INFORMATION_SCHEMA.COLUMNS columns on columns.COLUMN_NAME=ETLSourceMapping.DestinationColumnName
					WHERE ETLSourceMapping.ProcessId=8 --AND TABLE_NAME='Property'
						AND columns.TABLE_SCHEMA='dbo' and TABLE_NAME not like '%[_]%' and TABLE_NAME not in ('MADAddress')
				)A;

				
				--SELECT  * FROM #KeyValuesLegalDescription where table_name='Parcel' and code='8_18_3703241';
				
				
				-- Final entity update scripts
				--declare @modifiedDate datetime; set @modifiedDate=GETUTCDATE();
				declare @LoopCnt_Legal_final int=1;
				declare @Max_LoopCnt_Legal_final int=0;
				SELECT @Max_LoopCnt_Legal_final=isnull(max(rnk),0) from #KeyValuesLegalDescription
				 select @LoopCnt_Legal_final, @Max_LoopCnt_Legal_final, getdate()

				WHILE(@LoopCnt_Legal_final <=@Max_LoopCnt_Legal_final)
				BEGIN
					declare @UpdateEntityScriptsLegalDescription as nvarchar(max) = N'';

					select @UpdateEntityScriptsLegalDescription = @UpdateEntityScriptsLegalDescription +'UPDATE DBO.'
					+ TABLE_NAME + ' SET '
					+ DestinationColumnName 
					--+ ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
					+ case when New_Value=''  then ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
						when New_Value is null then ' = NULLIF('''','''') '
						else ' = ''' + replace(New_Value,'''','''''') + '''' end
					+ ',LastModifiedDateUTC = cast('''+cast(@ModifiedDate as varchar)+''' as datetime) WHERE CODE = '''+ CODE+ ''' ;'
					FROM #KeyValuesLegalDescription 
					WHERE RNK=@LoopCnt_Legal_final;

					--select @LoopCnt_Legal_final,@UpdateEntityScriptsLegalDescription
					EXECUTE sp_executeSql @UpdateEntityScriptsLegalDescription;
					SET @LoopCnt_Legal_final=@LoopCnt_Legal_final+1
				END

			END

			/*--------------------------------------------------------------------------------------------------------------------------------------------
			Update entity tables for Sales
			--------------------------------------------------------------------------------------------------------------------------------------------*/
			DROP TABLE IF EXISTS #ID_MaxDate_Sales;

			SELECT FolioRecord_ID,RollNumber, Sub_Id,StartDate AS Max_StartDate, Action_Field INTO #ID_MaxDate_Sales FROM 
			(
				----Sales
				select  FolioRecord_ID, RollNumber, Sales_Sale_ID as Sub_Id ,Max(StartDate) as StartDate,'Sales_Sale_ConveyanceDate' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE Sales_Sale_ConveyanceDate_Action ='Change'  group by FolioRecord_ID, Sales_Sale_ID,RollNumber
				union all select  FolioRecord_ID,RollNumber, Sales_Sale_ID ,Max(StartDate) as StartDate,'Sales_Sale_ConveyancePrice' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE Sales_Sale_ConveyancePrice_Action ='Change'  group by FolioRecord_ID, Sales_Sale_ID,RollNumber

				)A;
			--SELECT * FROM #ID_MaxDate_Sales


			DECLARE @ID_MaxDate_Sales_Count int=0
			select @ID_MaxDate_Sales_Count=count(*) from #ID_MaxDate_Sales
			-- SELECT @ID_MaxDate_Sales_Count

			IF @ID_MaxDate_Sales_Count>0
			BEGIN

				DROP TABLE IF EXISTS #OldValueNewValueSales;

				create table #OldValueNewValueSales
				(FolioRecord_id nvarchar(500)
				,Sales_Sale_ID nvarchar(500)
				,Action_Field nvarchar(500)
				,RollNumber nvarchar(500)
				,New_Value nvarchar(max)
				,code nvarchar(500)
				,Old_Value nvarchar(max)
				,StartDate nvarchar(max)
				);

				--drop table if exists #StartDate_rank;
				drop table if exists #ID_MaxDate_Sales_RNK;

				SELECT mx.*, bc.Code,ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS StartDate_rnk INTO #ID_MaxDate_Sales_RNK
				FROM #ID_MaxDate_Sales mx
				JOIN  dbo.BC_UPTO_DATE bc on mx.FolioRecord_ID=bc.FolioRecord_ID and bc.Sales_Sale_ID=mx.Sub_Id and bc.code is not null
				--select * from #ID_MaxDate_Sales_RNK

				declare @LoopCnt_Sales int=1;
				declare @Max_LoopCnt_Sales int=0;
				SELECT @Max_LoopCnt_Sales=isnull(max(StartDate_rnk),0) from #ID_MaxDate_Sales_RNK
				 select @LoopCnt_Sales, @Max_LoopCnt_Sales, getdate()

				WHILE(@LoopCnt_Sales <=@Max_LoopCnt_Sales)
				BEGIN

					declare @OldValueNewValueSales as nvarchar(max) = N'';  --03:29

					select @OldValueNewValueSales = @OldValueNewValueSales + 'select FolioRecord_ID, Sales_Sale_ID,'''
					+ action_field +''' as action_Field, '''
					+ RollNumber +''' as RollNumber, '
					+ action_field + ' as New_Value, '''
					+ md.Code + ''' as code,'
					+ action_field + '_OldValue as Old_Value, '
					+ 'StartDate '
					+ 'from StageLanding.BC_ALL_Assessment_Weekly_Current WHERE '
					+ ' FolioRecord_ID = '''+ FolioRecord_ID +''' and '
					+ ' Sales_Sale_ID = '''+ Sub_Id +''' and '
					+ ' RollNumber = '''+ RollNumber +''' union all '
					from #ID_MaxDate_Sales_RNK md
					where action_field not in ('FolioAddresses_FolioAddress') and action_field like '%sales%'
					and md.StartDate_rnk=@LoopCnt_Sales
					--select  @OldValueNewValueSales

					-- remove last N' UNION ALL '
					if len(@OldValueNewValueSales) > 11
						set @OldValueNewValueSales = left(@OldValueNewValueSales, len(@OldValueNewValueSales) - 10);

						--select @OldValueNewValueLegalDescription;

					SET @OldValueNewValueSales ='INSERT INTO #OldValueNewValueSales '+ @OldValueNewValueSales;
						--SELECT @OldValueNewValueSales;
	
					EXECUTE sp_executeSql @OldValueNewValueSales;

					SET @LoopCnt_Sales=@LoopCnt_Sales+1

				END
				-- SELECT * FROM #OldValueNewValueSales

				drop table if exists #TableColumnMapping_Sales

				SELECT * 
				into #TableColumnMapping_Sales
				FROM ETLProcess.ETLSourceMapping ETLSourceMapping with(nolock)-- on OldValueNewValueLegalDescription.Action_Field=ETLSourceMapping.SourceColumnName 
					--AND Pr.ProvinceCode='BC'
				JOIN INFORMATION_SCHEMA.COLUMNS columns with(nolock) on columns.COLUMN_NAME=ETLSourceMapping.DestinationColumnName
				WHERE ETLSourceMapping.ProcessId=8 
					AND columns.TABLE_SCHEMA='dbo' and TABLE_NAME not like '%[_]%' and TABLE_NAME not in ('MADAddress');


				DROP TABLE IF EXISTS #KeyValuesSalesDescription;

				-- GET MasterAddressID and KeyValues
				Select distinct OldValueNewValueLegalDescription.*
				,TableColumnMapping.SourceColumnName ,TableColumnMapping.DestinationColumnName
				,TableColumnMapping.TABLE_NAME
				into #KeyValuesSalesDescription --select count(*) 
				from  #OldValueNewValueSales OldValueNewValueLegalDescription
				INNER JOIN DBO.Property Pr with(nolock) ON OldValueNewValueLegalDescription.Code=pr.Code
				INNER JOIN #TableColumnMapping_Sales TableColumnMapping on OldValueNewValueLegalDescription.Action_Field=TableColumnMapping.SourceColumnName 
				

				--select * from #KeyValuesSalesDescription

				--DECLARE @ModifiedDate datetime;
				--SELECT  @ModifiedDate=GETUTCDATE(); --'2022-09-08 15:53:47.400'

				-- Final entity update scripts

				declare @UpdateEntityScriptsSales as nvarchar(max) = N'';

				select @UpdateEntityScriptsSales = @UpdateEntityScriptsSales +'UPDATE DBO.'
				+ TABLE_NAME + ' SET '
				+ DestinationColumnName 
				+ ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
				+ ',LastModifiedDateUTC = cast('''+cast(@ModifiedDate as varchar)+''' as datetime) WHERE CODE = '''+ CODE+ ''' ;'			
				FROM #KeyValuesSalesDescription;

				--select @UpdateEntityScriptsSales
				EXECUTE sp_executeSql @UpdateEntityScriptsSales;

			END

			/*--------------------------------------------------------------------------------------------------------------------------------------------
			Update entity tables for Folio Address
			--------------------------------------------------------------------------------------------------------------------------------------------*/

			DROP TABLE IF EXISTS #ID_MaxDate_Address;

			SELECT FolioRecord_ID,RollNumber, Sub_Id,StartDate AS Max_StartDate, Action_Field INTO #ID_MaxDate_Address FROM 
			(	--Folio Address
				--union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_Action' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID as Sub_Id ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_City' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_City_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_PostalZip' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_PostalZip_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_PrimaryFlag' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_PrimaryFlag_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_ProvinceState' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_ProvinceState_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetDirectionSuffix' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_StreetDirectionSuffix_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetName' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_StreetName_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetNumber' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_StreetNumber_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_StreetType' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_StreetType_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, FolioAddresses_FolioAddress_ID ,Max(StartDate) as StartDate,'FolioAddresses_FolioAddress_UnitNumber' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioAddresses_FolioAddress_UnitNumber_Action ='Change'  group by FolioRecord_ID, FolioAddresses_FolioAddress_ID, RollNumber
			)A;

			--select * from #ID_MaxDate_Address
			DECLARE @Cnt_ID_MaxDate_Address int=0;
			SELECT @Cnt_ID_MaxDate_Address=COUNT(*) FROM #ID_MaxDate_Address;

			IF(@Cnt_ID_MaxDate_Address>0)
			BEGIN
				Print 'Updating Folio Address fields'

				DROP TABLE IF EXISTS #OldValueNewValueAddress;

				create table #OldValueNewValueAddress
				(FolioRecord_id nvarchar(500)
				,FolioAddresses_FolioAddress_ID nvarchar(500)
				,Action_Field nvarchar(500)
				,RollNumber nvarchar(500)
				,New_Value nvarchar(max)
				,Code nvarchar(500)
				,Old_Value nvarchar(max)
				);

				--drop table if exists #StartDate_rank_Address;
				drop table if exists #ID_MaxDate_Address_RNK;

				SELECT ad.*, bc.Code ,ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS StartDate_rnk INTO #ID_MaxDate_Address_RNK 
				FROM #ID_MaxDate_Address ad
				JOIN  dbo.BC_UPTO_DATE bc on ad.FolioRecord_ID=bc.FolioRecord_ID and bc.FolioAddresses_FolioAddress_ID=ad.Sub_Id and bc.code is not null

				--select * from #ID_MaxDate_Address_RNK

				declare @LoopCnt_Address int=1;
				declare @Max_LoopCnt_Address int=0;
				SELECT @Max_LoopCnt_Address=isnull(max(StartDate_rnk),0) from #ID_MaxDate_Address_RNK
				 select @LoopCnt_Address, @Max_LoopCnt_Address, getdate()

				 WHILE(@LoopCnt_Address <=@Max_LoopCnt_Address)
					BEGIN
						--SELECT @LoopCnt_Legal
						declare @OldValueNewValueAddress as nvarchar(max) = N'';

								select @OldValueNewValueAddress = @OldValueNewValueAddress + 'select FolioRecord_ID, FolioAddresses_FolioAddress_ID,'''
								+ action_field +''' as action_Field, '''
								+ RollNumber +''' as RollNumber, '
								+ action_field + ' as New_Value, '''
								+ md.Code + ''' as code,'
								+ action_field + '_OldValue as Old_Value '
								+ 'from StageLanding.BC_ALL_Assessment_Weekly_Current WHERE '
								+ ' FolioRecord_ID = '''+ FolioRecord_ID +''' and '
								+ ' FolioAddresses_FolioAddress_ID = '''+ Sub_Id +''' and '
								+ ' RollNumber = '''+ RollNumber +''' union all '
								from #ID_MaxDate_Address_RNK md
								where action_field not in ('FolioAddresses_FolioAddress') and action_field like '%FolioAddress%'
									and md.StartDate_rnk=@LoopCnt_Address
								--and FolioRecord_ID='A0000KSQTG' AND sub_Id='A000012H5U' and RollNumber='19350000';

								-- remove last N' UNION ALL '
								if len(@OldValueNewValueAddress) > 11
									set @OldValueNewValueAddress = left(@OldValueNewValueAddress, len(@OldValueNewValueAddress) - 10)

									--select @OldValueNewValueLegalDescription;

								SET @OldValueNewValueAddress ='INSERT INTO #OldValueNewValueAddress '+ @OldValueNewValueAddress;
								--SELECT @OldValueNewValueAddress;
	
								EXECUTE sp_executeSql @OldValueNewValueAddress

								--SELECT * FROM ##OldValueNewValueLegalDescription1
								SET @LoopCnt_Address=@LoopCnt_Address+1
					END

				--select * from #OldValueNewValueAddress

				DROP TABLE IF EXISTS  #KeyValuesAddress

				-- GET MasterAddressID and KeyValues
				SELECT *, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS RNK
				into #KeyValuesAddress
				FROM (
				Select distinct OldValueNewValueAddress.*
				,ETLSourceMapping.SourceColumnName ,ETLSourceMapping.DestinationColumnName
				,columns.TABLE_NAME
				from  #OldValueNewValueAddress OldValueNewValueAddress
				INNER JOIN ETLProcess.ETLSourceMapping ETLSourceMapping on OldValueNewValueAddress.Action_Field=ETLSourceMapping.SourceColumnName  
				JOIN INFORMATION_SCHEMA.COLUMNS columns on columns.COLUMN_NAME=ETLSourceMapping.DestinationColumnName
				WHERE ETLSourceMapping.ProcessId=8 
					AND columns.TABLE_SCHEMA='dbo' and TABLE_NAME not like '%[_]%' and TABLE_NAME not in ('MADAddress')
				)A;

				--select * from #KeyValuesAddress

				--DECLARE @ModifiedDate datetime;	SELECT  @ModifiedDate=GETUTCDATE();

				-- Final entity update scripts
				--declare @modifiedDate datetime; set @modifiedDate=GETUTCDATE();
				declare @LoopCnt_Address_final int=1;
				declare @Max_LoopCnt_Address_final int=0;
				SELECT @Max_LoopCnt_Address_final=isnull(max(rnk),0) from #KeyValuesAddress
				 select @LoopCnt_Address_final, @Max_LoopCnt_Address_final, getdate()

				WHILE(@LoopCnt_Address_final <=@Max_LoopCnt_Address_final)
				BEGIN
					declare @UpdateEntityScriptsAddress as nvarchar(max) = N'';

					select @UpdateEntityScriptsAddress = @UpdateEntityScriptsAddress +'UPDATE DBO.'
					+ TABLE_NAME + ' SET '
					--+ DestinationColumnName + ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
					+ DestinationColumnName 
					+ case when New_Value=''  then ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
						when New_Value is null then ' = NULLIF('''','''') '
						else ' = ''' + replace(New_Value,'''','''''') + '''' end
					+ ',LastModifiedDateUTC = cast('''+cast(@ModifiedDate as varchar)+''' as datetime) WHERE CODE = '''+ CODE+ ''' ;'
					FROM #KeyValuesAddress WHERE RNK=@LoopCnt_Address_final;

					EXECUTE sp_executeSql @UpdateEntityScriptsAddress;

					SET @LoopCnt_Address_final=@LoopCnt_Address_final+1;

				END
				--select @UpdateEntityScriptsAddress

				---Update masterAddressId to Null
				UPDATE address_code
				SET MasterAddressID =NULL
				,IsMADSent =NULL ,IsMADReceived=NULL
				, MADSentDateUTC = NULL , MADReceivedDateUTC=NULL
				,isDuplicate=0
				,LastModifiedDateUTC=@ModifiedDate
				from dbo.Address address_code 
				join #KeyValuesAddress keycode on address_code.Code=keycode.code

				----Update masterAddressId to Null
				UPDATE address_code
				SET MasterAddressID =NULL
				,IsMADSent =NULL ,IsMADReceived=NULL
				, MADSentDateUTC = NULL , MADReceivedDateUTC=NULL
				,IsDuplicate=0
				,LastModifiedDateUTC=@ModifiedDate
				from dbo.Address address_code 
				join #KeyValuesAddress keycode on address_code.MasterAddressID=keycode.code

				---Update masterAddressId to Null
				UPDATE Property_code
				SET MasterAddressID =NULL
				,isDuplicate=0
				,LastModifiedDateUTC=@ModifiedDate
				from dbo.Property Property_code 
				join #KeyValuesAddress keycode on Property_code.Code=keycode.code;

			END

			/*--------------------------------------------------------------------------------------------------------------------------------------------
			Update entity tables for Folio Base details
			--------------------------------------------------------------------------------------------------------------------------------------------*/

			DROP TABLE IF EXISTS #ID_MaxDate_base;

			SELECT FolioRecord_ID,RollNumber, Sub_Id,StartDate AS Max_StartDate, Action_Field INTO #ID_MaxDate_base FROM 
			(
				-- Base fields
				select  FolioRecord_ID, RollNumber, '' Sub_Id ,Max(StartDate) as StartDate,'RollNumber' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE RollNumber_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'FolioDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioDescription_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'ActualUseDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE ActualUseDescription_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'TenureDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE TenureDescription_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'VacantFlag' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE VacantFlag_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandDepth' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LandMeasurement_LandDepth_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandDimension' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LandMeasurement_LandDimension_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandDimensionTypeDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LandMeasurement_LandDimensionTypeDescription_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'LandMeasurement_LandWidth' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE LandMeasurement_LandWidth_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'FolioDescription_Neighbourhood_NeighbourhoodCode' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioDescription_Neighbourhood_NeighbourhoodCode_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'FolioDescription_Neighbourhood_NeighbourhoodDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE FolioDescription_Neighbourhood_NeighbourhoodDescription_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'RegionalDistrict_DistrictDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE RegionalDistrict_DistrictDescription_Action='Change'  group by FolioRecord_ID, RollNumber
				union all select  FolioRecord_ID, RollNumber, '' SubId ,Max(StartDate) as StartDate,'SchoolDistrict_DistrictDescription' Action_Field FROM StageLanding.BC_ALL_Assessment_Weekly_Current WHERE SchoolDistrict_DistrictDescription_Action='Change'  group by FolioRecord_ID, RollNumber
			)A;

			--select * from #ID_MaxDate_base
			DECLARE @Cnt_ID_MaxDate_Base int=0;
			SELECT @Cnt_ID_MaxDate_Base=COUNT(*) FROM #ID_MaxDate_base;

			
			IF(@Cnt_ID_MaxDate_Base>0)
			BEGIN

				DROP TABLE IF EXISTS #OldValueNewValueBase

				create table #OldValueNewValueBase
				(FolioRecord_id nvarchar(500)
				--,FolioAddresses_FolioAddress_ID nvarchar(500)
				,Action_Field nvarchar(500)
				,RollNumber nvarchar(500)
				,New_Value nvarchar(max)
				,Code nvarchar(500)
				,Old_Value nvarchar(max)
				)

				drop table if exists #ID_MaxDate_Base_RNK;

				SELECT base.*, bc.code, ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS StartDate_rnk INTO #ID_MaxDate_Base_RNK 
				FROM #ID_MaxDate_base base
				JOIN dbo.BC_UPTO_DATE bc ON bc.FolioRecord_ID=base.FolioRecord_ID and bc.code is not null
				where action_field <> 'FolioDescription' and bc.Code is not null

				--select * from #ID_MaxDate_Base_RNK order by StartDate_rnk

				declare @LoopCnt_Base int=1;
				declare @Max_LoopCnt_Base int;
				SELECT @Max_LoopCnt_Base=isnull(max(StartDate_rnk),0) from #ID_MaxDate_Base_RNK
				select @LoopCnt_Base, @Max_LoopCnt_Base

				WHILE(@LoopCnt_Base <=@Max_LoopCnt_Base)
				BEGIN
					declare @OldValueNewValueBase as nvarchar(max) = N''

					select @OldValueNewValueBase = @OldValueNewValueBase + 'select FolioRecord_ID, '''
					+ action_field +''' as action_Field, '''
					+ RollNumber +''' as RollNumber, '
					+ action_field + ' as New_Value, '''
					+ md.Code + ''' as code,'
					+ action_field + '_OldValue as Old_Value '
					+ 'from StageLanding.BC_ALL_Assessment_Weekly_Current WHERE '
					+ ' FolioRecord_ID = '''+ FolioRecord_ID +''' and '
					--+ ' FolioAddresses_FolioAddress_ID = '''+ Sub_Id +''' and '
					+ ' RollNumber = '''+ RollNumber +''' union all '
					from #ID_MaxDate_Base_RNK md
					where action_field not like '%FolioAddress%' and action_field not like '%LegalDesc%' and action_field <> 'FolioDescription'
					and md.StartDate_rnk=@LoopCnt_Base
					--and FolioRecord_ID='A0000KSDHE' AND sub_Id='A000010XYL' and RollNumber='04897280'


					-- remove last N' UNION ALL '
					if len(@OldValueNewValueBase) > 11
						set @OldValueNewValueBase = left(@OldValueNewValueBase, len(@OldValueNewValueBase) - 10)

						--select @OldValueNewValueBase

					SET @OldValueNewValueBase ='INSERT INTO #OldValueNewValueBase '+ @OldValueNewValueBase
						--SELECT @OldValueNewValueBase

					EXECUTE sp_executeSql @OldValueNewValueBase;

					SET @LoopCnt_Base=@LoopCnt_Base+1
				END

				--select * from #OldValueNewValueBase

				DROP TABLE IF EXISTS  #KeyValuesBase;

				-- GET MasterAddressID and KeyValues
				Select distinct OldValueNewValueBase.*
				,ETLSourceMapping.SourceColumnName ,ETLSourceMapping.DestinationColumnName
				,columns.TABLE_NAME
				into #KeyValuesBase
				from  #OldValueNewValueBase OldValueNewValueBase
				INNER JOIN ETLProcess.ETLSourceMapping ETLSourceMapping on OldValueNewValueBase.Action_Field=ETLSourceMapping.SourceColumnName 
				JOIN INFORMATION_SCHEMA.COLUMNS columns on columns.COLUMN_NAME=ETLSourceMapping.DestinationColumnName
				WHERE ETLSourceMapping.ProcessId=8 
					AND columns.TABLE_SCHEMA='dbo' and TABLE_NAME not like '%[_]%' and TABLE_NAME not in ('MADAddress');

				--select * from #KeyValuesBase

				--DECLARE @ModifiedDate datetime;	SELECT  @ModifiedDate=GETUTCDATE();

				-- Final entity update scripts

				declare @UpdateEntityScriptsBase as nvarchar(max) = N'';

				select @UpdateEntityScriptsBase = @UpdateEntityScriptsBase +'UPDATE DBO.'
				+ TABLE_NAME + ' SET '
				+ DestinationColumnName 
				--+ ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
				+ case when New_Value=''  then ' = NULLIF('''+ ISNULL(replace(New_Value,'''',''''''),'')+''','''') '
					when New_Value is null then ' = NULLIF('''','''') '
					else ' = ''' + replace(New_Value,'''','''''') + '''' end
				+ ',LastModifiedDateUTC = cast('''+cast(@ModifiedDate as varchar)+''' as datetime) WHERE CODE = '''+ CODE+ ''' ;'
				FROM #KeyValuesBase;

				--select * from #KeyValues where FolioRecord_ID='D0001JM0HV'
				--SELECT @UpdateEntityScriptsBase

				EXECUTE sp_executeSql @UpdateEntityScriptsBase

			END

			/*--------------------------------------------------------------------------------------------------------------------------------------------
			Update IsDuplicate to 0
			--------------------------------------------------------------------------------------------------------------------------------------------*/
			UPDATE [dbo].[Building] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Business] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Listing] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Parcel] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Permit] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[PIN] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Property] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Sales] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Taxation] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;
			UPDATE [dbo].[Valuation] SET IsDuplicate=0 where LastModifiedDateUTC=@ModifiedDate and IsDuplicate=1;

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
		   , @ErrorMessage='Failed to Load BCA Weekly Update DTC, Entity'        
		   , @IsError='Yes';        
      END CATCH 

		IF @IsError=1        
	  THROW 50005, N'An error occurred while updating BCA Weekly Update DTC, Entity', 1;        
 
END