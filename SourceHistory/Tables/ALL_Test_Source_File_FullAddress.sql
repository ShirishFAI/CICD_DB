CREATE TABLE [SourceHistory].[ALL_Test_Source_File_FullAddress] (
    [Code]                      VARCHAR (200)  NULL,
    [City]                      NVARCHAR (510) NULL,
    [PostalCode]                NVARCHAR (510) NULL,
    [ProvinceCode]              NVARCHAR (510) NULL,
    [FSA]                       NVARCHAR (510) NULL,
    [District]                  NVARCHAR (510) NULL,
    [JurCode]                   NVARCHAR (510) NULL,
    [Country]                   NVARCHAR (510) NULL,
    [FullAddress]               NVARCHAR (510) NULL,
    [Latitude]                  NVARCHAR (510) NULL,
    [Longitude]                 NVARCHAR (510) NULL,
    [LatitudeLongitude]         NVARCHAR (510) NULL,
    [Neighbourhood]             NVARCHAR (510) NULL,
    [NeighbourhoodDescription]  NVARCHAR (510) NULL,
    [Municipality]              NVARCHAR (510) NULL,
    [Region]                    NVARCHAR (510) NULL,
    [Township]                  NVARCHAR (510) NULL,
    [Range]                     NVARCHAR (510) NULL,
    [LandDistrict]              NVARCHAR (510) NULL,
    [LandDistrictName]          NVARCHAR (510) NULL,
    [AreaDescription]           NVARCHAR (510) NULL,
    [JurDescription]            NVARCHAR (510) NULL,
    [SchoolDistrictDescription] NVARCHAR (510) NULL,
    [CrossStreet]               NVARCHAR (510) NULL,
    [Community]                 NVARCHAR (510) NULL,
    [IsMunicipalAddress]        NVARCHAR (510) NULL,
    [PIN]                       NVARCHAR (510) NULL,
    [IsCondo]                   NVARCHAR (510) NULL,
    [ListStatus]                NVARCHAR (510) NULL,
    [EstateTypeCode]            NVARCHAR (510) NULL,
    [GVSEligible]               NVARCHAR (510) NULL,
    [IsEnergy]                  NVARCHAR (510) NULL,
    [IsMobileHome]              NVARCHAR (510) NULL,
    [IsNativeLand]              NVARCHAR (510) NULL,
    [IsNewHome]                 NVARCHAR (510) NULL,
    [IsPartLot]                 NVARCHAR (510) NULL,
    [IsRenovatedLotNum]         NVARCHAR (510) NULL,
    [IsVacantLand]              NVARCHAR (510) NULL,
    [MetesAndBounds]            NVARCHAR (510) NULL,
    [NewHomeEasement]           NVARCHAR (510) NULL,
    [OccupancyTypeCode]         NVARCHAR (510) NULL,
    [PrimaryProperty]           NVARCHAR (510) NULL,
    [PropertyTypeCode]          NVARCHAR (510) NULL,
    [Company]                   NVARCHAR (510) NULL,
    [HashBytes]                 BINARY (64)    NULL,
    [HistEndDate]               DATETIME       NULL,
    [IsDuplicate]               BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ALL_Test_Source_File_FullAddress_Code]
    ON [SourceHistory].[ALL_Test_Source_File_FullAddress]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ALL_Test_Source_File_FullAddress_HistEndDate]
    ON [SourceHistory].[ALL_Test_Source_File_FullAddress]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

