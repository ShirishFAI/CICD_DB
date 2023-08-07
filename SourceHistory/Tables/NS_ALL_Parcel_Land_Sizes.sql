CREATE TABLE [SourceHistory].[NS_ALL_Parcel_Land_Sizes] (
    [Code]                      VARCHAR (200)  NULL,
    [Municipal Unit]            NVARCHAR (MAX) NULL,
    [Assessment Account Number] NVARCHAR (MAX) NULL,
    [Civic Number]              NVARCHAR (MAX) NULL,
    [Civic Additional]          NVARCHAR (MAX) NULL,
    [Civic Direction]           NVARCHAR (MAX) NULL,
    [Civic Street Name]         NVARCHAR (MAX) NULL,
    [Civic Street Suffix]       NVARCHAR (MAX) NULL,
    [Civic City Name]           NVARCHAR (MAX) NULL,
    [Acreage]                   NVARCHAR (MAX) NULL,
    [Square Feet]               NVARCHAR (MAX) NULL,
    [Y Map Coordinate]          NVARCHAR (MAX) NULL,
    [X Map Coordinate]          NVARCHAR (MAX) NULL,
    [Map Coordinates]           NVARCHAR (MAX) NULL,
    [HistEndDate]               DATETIME       NULL,
    [IsDuplicate]               BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_NS_ALL_Parcel_Land_Sizes_Code]
    ON [SourceHistory].[NS_ALL_Parcel_Land_Sizes]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_NS_ALL_Parcel_Land_Sizes_HistEndDate]
    ON [SourceHistory].[NS_ALL_Parcel_Land_Sizes]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

