CREATE TABLE [SourceHistory].[ALL_TestAddressFields] (
    [Code]                      VARCHAR (200)  NULL,
    [UnitNumber]                NVARCHAR (510) NULL,
    [StreetNumber]              NVARCHAR (510) NULL,
    [StreetName]                NVARCHAR (510) NULL,
    [StreetType]                NVARCHAR (510) NULL,
    [StreetDirection]           NVARCHAR (510) NULL,
    [City]                      NVARCHAR (510) NULL,
    [ProvinceCode]              NVARCHAR (510) NULL,
    [FSA]                       NVARCHAR (510) NULL,
    [District]                  NVARCHAR (510) NULL,
    [JurCode]                   NVARCHAR (510) NULL,
    [Country]                   NVARCHAR (510) NULL,
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
    [PostalCode]                NVARCHAR (510) NULL,
    [CompleteDate]              NVARCHAR (510) NULL,
    [POSDateSales]              NVARCHAR (510) NULL,
    [ClosingDate]               NVARCHAR (510) NULL,
    [HashBytes]                 BINARY (64)    NULL,
    [HistEndDate]               DATETIME       NULL,
    [IsDuplicate]               BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ALL_TestAddressFields_Code]
    ON [SourceHistory].[ALL_TestAddressFields]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ALL_TestAddressFields_HistEndDate]
    ON [SourceHistory].[ALL_TestAddressFields]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

