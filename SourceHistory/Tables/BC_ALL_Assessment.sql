CREATE TABLE [SourceHistory].[BC_ALL_Assessment] (
    [Code]                                                              VARCHAR (200)  NULL,
    [RollNumber]                                                        NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_UnitNumber]                            NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetNumber]                          NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetName]                            NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetType]                            NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_City]                                  NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_ProvinceState]                         NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_PostalZip]                             NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_FormattedLegalDescription]      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_PID]                            NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Plan]                           NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrict]                   NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LandDistrictDescription]        NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodCode]                  NVARCHAR (MAX) NULL,
    [FolioDescription_Neighbourhood_NeighbourhoodDescription]           NVARCHAR (MAX) NULL,
    [ActualUseDescription]                                              NVARCHAR (MAX) NULL,
    [VacantFlag]                                                        NVARCHAR (MAX) NULL,
    [TenureDescription]                                                 NVARCHAR (MAX) NULL,
    [SchoolDistrict_DistrictDescription]                                NVARCHAR (MAX) NULL,
    [RegionalDistrict_DistrictDescription]                              NVARCHAR (MAX) NULL,
    [Values_GeneralValues_PropertyClassValues_PropertyClassDescription] NVARCHAR (MAX) NULL,
    [Valuation_ValuesByETC_LandValue]                                   NVARCHAR (MAX) NULL,
    [Valuation_ValuesByETC_ImprovementValue]                            NVARCHAR (MAX) NULL,
    [AssessmentAreaDescription]                                         NVARCHAR (MAX) NULL,
    [JurisdictionCode]                                                  NVARCHAR (MAX) NULL,
    [JurisdictionDescription]                                           NVARCHAR (MAX) NULL,
    [RollYear]                                                          NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyanceDate]                                         NVARCHAR (MAX) NULL,
    [Sales_Sale_ConveyancePrice]                                        NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimensionTypeDescription]                      NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDimension]                                     NVARCHAR (MAX) NULL,
    [LandMeasurement_LandWidth]                                         NVARCHAR (MAX) NULL,
    [LandMeasurement_LandDepth]                                         NVARCHAR (MAX) NULL,
    [FolioAddresses_FolioAddress_StreetDirectionSuffix]                 NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Range]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Township]                       NVARCHAR (MAX) NULL,
    [HashBytes]                                                         BINARY (64)    NULL,
    [HistEndDate]                                                       DATETIME       NULL,
    [IsDuplicate]                                                       BIT            NULL,
    [LegalDescriptions_LegalDescription_Block]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_DistrictLot]                    NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_ExceptPlan]                     NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalSubdivision]               NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_LegalText]                      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Lot]                            NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Parcel]                         NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part1]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part2]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part3]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Part4]                          NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Portion]                        NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_Section]                        NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_StrataLot]                      NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubBlock]                       NVARCHAR (MAX) NULL,
    [LegalDescriptions_LegalDescription_SubLot]                         NVARCHAR (MAX) NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_ALL_Assessment_Code]
    ON [SourceHistory].[BC_ALL_Assessment]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_ALL_Assessment_HistEndDate]
    ON [SourceHistory].[BC_ALL_Assessment]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

