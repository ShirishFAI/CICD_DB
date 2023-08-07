CREATE TABLE [SourceHistory].[ALL_Test_Source_LATLONG] (
    [Code]         VARCHAR (200)  NULL,
    [StreetNumber] NVARCHAR (MAX) NULL,
    [StreetName]   NVARCHAR (MAX) NULL,
    [StreetType]   NVARCHAR (MAX) NULL,
    [PostalCode]   NVARCHAR (MAX) NULL,
    [City]         NVARCHAR (MAX) NULL,
    [ProvinceCode] NVARCHAR (MAX) NULL,
    [HashBytes]    BINARY (64)    NULL,
    [HistEndDate]  DATETIME       NULL,
    [IsDuplicate]  BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ALL_Test_Source_LATLONG_Code]
    ON [SourceHistory].[ALL_Test_Source_LATLONG]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ALL_Test_Source_LATLONG_HistEndDate]
    ON [SourceHistory].[ALL_Test_Source_LATLONG]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

