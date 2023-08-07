CREATE TABLE [SourceHistory].[BC_ALL_Assessment_Weekly] (
    [Code]                                         VARCHAR (200)  NULL,
    [FolioRecords_AssessmentAreaDescription]       NVARCHAR (MAX) NULL,
    [FolioAddress_City]                            NVARCHAR (MAX) NULL,
    [FolioAddress_ProvinceState]                   NVARCHAR (MAX) NULL,
    [FolioAddress_StreetName]                      NVARCHAR (MAX) NULL,
    [FolioAddress_StreetNumber]                    NVARCHAR (MAX) NULL,
    [FolioAddress_StreetType]                      NVARCHAR (MAX) NULL,
    [FolioAddress_UnitNumber]                      NVARCHAR (MAX) NULL,
    [FolioAddress_PostalZip]                       NVARCHAR (MAX) NULL,
    [FolioAddress_StreetDirectionSuffix]           NVARCHAR (MAX) NULL,
    [FolioDescription_ActualUseDescription]        NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDepth]                    NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimension]                NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimensionTypeDescription] NVARCHAR (MAX) NULL,
    [LandMeasurement_LandWidth]                    NVARCHAR (MAX) NULL,
    [Neighbourhood_NeighbourhoodCode]              NVARCHAR (MAX) NULL,
    [Neighbourhood_NeighbourhoodDescription]       NVARCHAR (MAX) NULL,
    [RegionalDistrict_DistrictDescription]         NVARCHAR (MAX) NULL,
    [RegionalHospitalDistrict_DistrictDescription] NVARCHAR (MAX) NULL,
    [SchoolDistrict_DistrictDescription]           NVARCHAR (MAX) NULL,
    [FolioDescription_TenureDescription]           NVARCHAR (MAX) NULL,
    [FolioDescription_VacantFlag]                  NVARCHAR (MAX) NULL,
    [LegalDescription_Block]                       NVARCHAR (MAX) NULL,
    [LegalDescription_DistrictLot]                 NVARCHAR (MAX) NULL,
    [LegalDescription_FormattedLegalDescription]   NVARCHAR (MAX) NULL,
    [LegalDescription_LandDistrict]                NVARCHAR (MAX) NULL,
    [LegalDescription_LandDistrictDescription]     NVARCHAR (MAX) NULL,
    [LegalDescription_LegalText]                   NVARCHAR (MAX) NULL,
    [LegalDescription_Lot]                         NVARCHAR (MAX) NULL,
    [LegalDescription_PID]                         NVARCHAR (MAX) NULL,
    [LegalDescription_Plan]                        NVARCHAR (MAX) NULL,
    [LegalDescription_Range]                       NVARCHAR (MAX) NULL,
    [LegalDescription_Section]                     NVARCHAR (MAX) NULL,
    [LegalDescription_Township]                    NVARCHAR (MAX) NULL,
    [LegalDescription_ExceptPlan]                  NVARCHAR (MAX) NULL,
    [LegalDescription_LegalSubdivision]            NVARCHAR (MAX) NULL,
    [LegalDescription_Parcel]                      NVARCHAR (MAX) NULL,
    [LegalDescription_Part1]                       NVARCHAR (MAX) NULL,
    [LegalDescription_Part2]                       NVARCHAR (MAX) NULL,
    [LegalDescription_Part3]                       NVARCHAR (MAX) NULL,
    [LegalDescription_Part4]                       NVARCHAR (MAX) NULL,
    [LegalDescription_Portion]                     NVARCHAR (MAX) NULL,
    [LegalDescription_StrataLot]                   NVARCHAR (MAX) NULL,
    [LegalDescription_SubBlock]                    NVARCHAR (MAX) NULL,
    [LegalDescription_SubLot]                      NVARCHAR (MAX) NULL,
    [FolioRecords_RollNumber]                      NVARCHAR (MAX) NULL,
    [Sale_ConveyanceDate]                          NVARCHAR (MAX) NULL,
    [Sale_ConveyancePrice]                         NVARCHAR (MAX) NULL,
    [FolioRecords_JurisdictionCode]                NVARCHAR (MAX) NULL,
    [FolioRecords_JurisdictionDescription]         NVARCHAR (MAX) NULL,
    [FolioRecords_RollYear]                        NVARCHAR (MAX) NULL,
    [HashBytes]                                    BINARY (64)    NULL,
    [HistEndDate]                                  DATETIME       NULL,
    [IsDuplicate]                                  BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_ALL_Assessment_Weekly_Code]
    ON [SourceHistory].[BC_ALL_Assessment_Weekly]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_ALL_Assessment_Weekly_HistEndDate]
    ON [SourceHistory].[BC_ALL_Assessment_Weekly]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

