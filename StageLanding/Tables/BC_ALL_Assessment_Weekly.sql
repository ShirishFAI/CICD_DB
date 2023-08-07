﻿CREATE TABLE [StageLanding].[BC_ALL_Assessment_Weekly] (
    [FolioRecord_ID]                                                        NVARCHAR (MAX) NULL,
    [RunType]                                                               NVARCHAR (MAX) NULL,
    [RollYear]                                                              NVARCHAR (MAX) NULL,
    [OwnershipYear]                                                         NVARCHAR (MAX) NULL,
    [StartDate]                                                             NVARCHAR (MAX) NULL,
    [EndDate]                                                               NVARCHAR (MAX) NULL,
    [RunDate]                                                               NVARCHAR (MAX) NULL,
    [AssessmentAreaCode]                                                    NVARCHAR (MAX) NULL,
    [AssessmentAreaDescription]                                             NVARCHAR (MAX) NULL,
    [JurisdictionCode]                                                      NVARCHAR (MAX) NULL,
    [JurisdictionDescription]                                               NVARCHAR (MAX) NULL,
    [RollNumber]                                                            NVARCHAR (MAX) NULL,
    [RollNumber_Action]                                                     NVARCHAR (MAX) NULL,
    [RollNumber_OldValue]                                                   NVARCHAR (MAX) NULL,
    [FolioAction_FolioAdd]                                                  NVARCHAR (MAX) NULL,
    [FolioAction_FolioDelete]                                               NVARCHAR (MAX) NULL,
    [FolioAction_FolioDelete_DeleteReasonCode]                              NVARCHAR (MAX) NULL,
    [FolioAction_FolioDelete_DeleteReasonDescription]                       NVARCHAR (MAX) NULL,
    [FolioDescription_Action]                                               NVARCHAR (MAX) NULL,
    [ActualUseDescription]                                                  NVARCHAR (MAX) NULL,
    [ActualUseDescription_Action]                                           NVARCHAR (MAX) NULL,
    [ActualUseDescription_OldValue]                                         NVARCHAR (MAX) NULL,
    [TenureDescription]                                                     NVARCHAR (MAX) NULL,
    [TenureDescription_Action]                                              NVARCHAR (MAX) NULL,
    [TenureDescription_OldValue]                                            NVARCHAR (MAX) NULL,
    [VacantFlag]                                                            NVARCHAR (MAX) NULL,
    [VacantFlag_Action]                                                     NVARCHAR (MAX) NULL,
    [VacantFlag_OldValue]                                                   NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_Action]                                    NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_City]                                      NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_City_Action]                               NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_City_OldValue]                             NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_ID]                                        NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PostalZip]                                 NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PostalZip_Action]                          NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PostalZip_OldValue]                        NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PrimaryFlag]                               NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PrimaryFlag_Action]                        NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PrimaryFlag_OldValue]                      NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_ProvinceState]                             NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_ProvinceState_Action]                      NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_ProvinceState_OldValue]                    NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetDirectionSuffix]                     NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetDirectionSuffix_Action]              NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetDirectionSuffix_OldValue]            NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetName]                                NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetName_Action]                         NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetName_OldValue]                       NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetNumber]                              NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetNumber_Action]                       NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetNumber_OldValue]                     NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetType]                                NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetType_Action]                         NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetType_OldValue]                       NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_UnitNumber]                                NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_UnitNumber_Action]                         NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_UnitNumber_OldValue]                       NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDepth]                                             NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDepth_Action]                                      NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDepth_OldValue]                                    NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimension]                                         NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimension_Action]                                  NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimension_OldValue]                                NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimensionTypeDescription]                          NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimensionTypeDescription_Action]                   NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimensionTypeDescription_OldValue]                 NVARCHAR (MAX) NULL,
    [LandMeasurement_LandWidth]                                             NVARCHAR (MAX) NULL,
    [LandMeasurement_LandWidth_Action]                                      NVARCHAR (MAX) NULL,
    [LandMeasurement_LandWidth_OldValue]                                    NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodCode]                      NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodCode_Action]               NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodCode_OldValue]             NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodDescription]               NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodDescription_Action]        NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodDescription_OldValue]      NVARCHAR (MAX) NULL,
    [RegionalDistrict_DistrictDescription]                                  NVARCHAR (MAX) NULL,
    [RegionalDistrict_DistrictDescription_Action]                           NVARCHAR (MAX) NULL,
    [RegionalDistrict_DistrictDescription_OldValue]                         NVARCHAR (MAX) NULL,
    [SchoolDistrict_DistrictDescription]                                    NVARCHAR (MAX) NULL,
    [SchoolDistrict_DistrictDescription_Action]                             NVARCHAR (MAX) NULL,
    [SchoolDistrict_DistrictDescription_OldValue]                           NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Action]                             NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Block]                              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Block_Action]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Block_OldValue]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_DistrictLot]                        NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_DistrictLot_Action]                 NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_DistrictLot_OldValue]               NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_ExceptPlan]                         NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_ExceptPlan_Action]                  NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_ExceptPlan_OldValue]                NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_FormattedLegalDescription]          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_FormattedLegalDescription_Action]   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_FormattedLegalDescription_OldValue] NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_ID]                                 NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrict]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrict_Action]                NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrict_OldValue]              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrictDescription]            NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrictDescription_Action]     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrictDescription_OldValue]   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LeaseLicenceNumber]                 NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LeaseLicenceNumber_Action]          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LeaseLicenceNumber_OldValue]        NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalText]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalText_Action]                   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalText_OldValue]                 NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Lot]                                NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Lot_Action]                         NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Lot_OldValue]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Meridian]                           NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Meridian_Action]                    NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Meridian_OldValue]                  NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_MeridianShort]                      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_MeridianShort_Action]               NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_MeridianShort_OldValue]             NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Parcel]                             NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Parcel_Action]                      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Parcel_OldValue]                    NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part1]                              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part1_Action]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part1_OldValue]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part2]                              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part2_Action]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part2_OldValue]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part3]                              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part3_Action]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part3_OldValue]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part4]                              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part4_Action]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part4_OldValue]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_PID]                                NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_PID_Action]                         NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_PID_OldValue]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Plan]                               NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Plan_Action]                        NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Plan_OldValue]                      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Portion]                            NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Portion_Action]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Portion_OldValue]                   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Range]                              NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Range_Action]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Range_OldValue]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Section]                            NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Section_Action]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Section_OldValue]                   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_StrataLot]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_StrataLot_Action]                   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_StrataLot_OldValue]                 NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubBlock]                           NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubBlock_Action]                    NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubBlock_OldValue]                  NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubLot]                             NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubLot_Action]                      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubLot_OldValue]                    NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Township]                           NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Township_Action]                    NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Township_OldValue]                  NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalSubdivision]                   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalSubdivision_Action]            NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalSubdivision_OldValue]          NVARCHAR (MAX) NULL,
    [Sales_Sale_Action]                                                     NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyanceDate]                                             NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyanceDate_Action]                                      NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyanceDate_OldValue]                                    NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyancePrice]                                            NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyancePrice_Action]                                     NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyancePrice_OldValue]                                   NVARCHAR (MAX) NULL,
    [Sales_Sale_ID]                                                         NVARCHAR (MAX) NULL
);

