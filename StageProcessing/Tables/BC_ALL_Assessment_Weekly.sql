﻿CREATE TABLE [StageProcessing].[BC_ALL_Assessment_Weekly] (
    [SourceID]                                     INT             NULL,
    [Code]                                         VARCHAR (200)   NULL,
    [FolioRecords_AssessmentAreaDescription]       VARCHAR (100)   NULL,
    [FolioAddress_City]                            VARCHAR (200)   NULL,
    [FolioAddress_ProvinceState]                   VARCHAR (50)    NULL,
    [FolioAddress_StreetName]                      VARCHAR (200)   NULL,
    [FolioAddress_StreetNumber]                    VARCHAR (100)   NULL,
    [FolioAddress_StreetType]                      VARCHAR (200)   NULL,
    [FolioAddress_UnitNumber]                      VARCHAR (100)   NULL,
    [FolioAddress_PostalZip]                       VARCHAR (50)    NULL,
    [FolioAddress_StreetDirectionSuffix]           VARCHAR (20)    NULL,
    [FolioDescription_ActualUseDescription]        VARCHAR (100)   NULL,
    [LandMeasurement_LandDepth]                    VARCHAR (50)    NULL,
    [LandMeasurement_LandDimension]                DECIMAL (17, 2) NULL,
    [LandMeasurement_LandDimensionTypeDescription] VARCHAR (50)    NULL,
    [LandMeasurement_LandWidth]                    VARCHAR (50)    NULL,
    [Neighbourhood_NeighbourhoodCode]              VARCHAR (100)   NULL,
    [Neighbourhood_NeighbourhoodDescription]       VARCHAR (200)   NULL,
    [RegionalDistrict_DistrictDescription]         VARCHAR (50)    NULL,
    [RegionalHospitalDistrict_DistrictDescription] VARCHAR (100)   NULL,
    [SchoolDistrict_DistrictDescription]           VARCHAR (50)    NULL,
    [FolioDescription_TenureDescription]           VARCHAR (100)   NULL,
    [FolioDescription_VacantFlag]                  VARCHAR (5)     NULL,
    [LegalDescription_Block]                       VARCHAR (4000)  NULL,
    [LegalDescription_DistrictLot]                 VARCHAR (4000)  NULL,
    [LegalDescription_FormattedLegalDescription]   VARCHAR (4000)  NULL,
    [LegalDescription_LandDistrict]                VARCHAR (10)    NULL,
    [LegalDescription_LandDistrictDescription]     VARCHAR (100)   NULL,
    [LegalDescription_LegalText]                   VARCHAR (4000)  NULL,
    [LegalDescription_Lot]                         VARCHAR (4000)  NULL,
    [LegalDescription_PID]                         VARCHAR (50)    NULL,
    [LegalDescription_Plan]                        VARCHAR (100)   NULL,
    [LegalDescription_Range]                       VARCHAR (10)    NULL,
    [LegalDescription_Section]                     VARCHAR (4000)  NULL,
    [LegalDescription_Township]                    VARCHAR (10)    NULL,
    [LegalDescription_ExceptPlan]                  VARCHAR (4000)  NULL,
    [LegalDescription_LegalSubdivision]            VARCHAR (4000)  NULL,
    [LegalDescription_Parcel]                      VARCHAR (4000)  NULL,
    [LegalDescription_Part1]                       VARCHAR (4000)  NULL,
    [LegalDescription_Part2]                       VARCHAR (4000)  NULL,
    [LegalDescription_Part3]                       VARCHAR (4000)  NULL,
    [LegalDescription_Part4]                       VARCHAR (4000)  NULL,
    [LegalDescription_Portion]                     VARCHAR (4000)  NULL,
    [LegalDescription_StrataLot]                   VARCHAR (4000)  NULL,
    [LegalDescription_SubBlock]                    VARCHAR (4000)  NULL,
    [LegalDescription_SubLot]                      VARCHAR (4000)  NULL,
    [FolioRecords_RollNumber]                      VARCHAR (200)   NULL,
    [Sale_ConveyanceDate]                          DATE            NULL,
    [Sale_ConveyancePrice]                         DECIMAL (17, 2) NULL,
    [FolioRecords_JurisdictionCode]                VARCHAR (10)    NULL,
    [FolioRecords_JurisdictionDescription]         VARCHAR (100)   NULL,
    [FolioRecords_RollYear]                        INT             NULL,
    [HashBytes]                                    BINARY (64)     NULL,
    [ActionType]                                   CHAR (1)        NULL,
    [IsDuplicate]                                  BIT             NULL
);

