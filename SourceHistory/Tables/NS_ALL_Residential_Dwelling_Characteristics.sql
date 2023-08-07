CREATE TABLE [SourceHistory].[NS_ALL_Residential_Dwelling_Characteristics] (
    [Code]                      VARCHAR (200)  NULL,
    [Municipal Unit]            NVARCHAR (MAX) NULL,
    [Assessment Account Number] NVARCHAR (MAX) NULL,
    [Civic Number]              NVARCHAR (MAX) NULL,
    [Civic Additional]          NVARCHAR (MAX) NULL,
    [Civic Direction]           NVARCHAR (MAX) NULL,
    [Civic Street Name]         NVARCHAR (MAX) NULL,
    [Civic Street Suffix]       NVARCHAR (MAX) NULL,
    [Civic City Name]           NVARCHAR (MAX) NULL,
    [Living Units]              NVARCHAR (MAX) NULL,
    [Year Built]                NVARCHAR (MAX) NULL,
    [Square Foot Living Area]   NVARCHAR (MAX) NULL,
    [Style]                     NVARCHAR (MAX) NULL,
    [Bedrooms]                  NVARCHAR (MAX) NULL,
    [Bathrooms]                 NVARCHAR (MAX) NULL,
    [Finished Basement]         NVARCHAR (MAX) NULL,
    [Garage]                    NVARCHAR (MAX) NULL,
    [Y Map Coordinate]          NVARCHAR (MAX) NULL,
    [X Map Coordinate]          NVARCHAR (MAX) NULL,
    [Map Coordinates]           NVARCHAR (MAX) NULL,
    [HistEndDate]               DATETIME       NULL,
    [IsDuplicate]               BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_NS_ALL_Residential_Dwelling_Characteristics_Code]
    ON [SourceHistory].[NS_ALL_Residential_Dwelling_Characteristics]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_NS_ALL_Residential_Dwelling_Characteristics_HistEndDate]
    ON [SourceHistory].[NS_ALL_Residential_Dwelling_Characteristics]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

