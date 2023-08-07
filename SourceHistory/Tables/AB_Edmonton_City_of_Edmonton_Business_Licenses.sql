CREATE TABLE [SourceHistory].[AB_Edmonton_City_of_Edmonton_Business_Licenses] (
    [Code]             VARCHAR (200)  NULL,
    [Address]          NVARCHAR (MAX) NULL,
    [Licence Number]   NVARCHAR (MAX) NULL,
    [Neighbourhood ID] NVARCHAR (MAX) NULL,
    [Neighbourhood]    NVARCHAR (MAX) NULL,
    [Latitude]         NVARCHAR (MAX) NULL,
    [Longitude]        NVARCHAR (MAX) NULL,
    [Location]         NVARCHAR (MAX) NULL,
    [HistEndDate]      DATETIME       NULL,
    [IsDuplicate]      BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Edmonton_City_of_Edmonton_Business_Licenses_Code]
    ON [SourceHistory].[AB_Edmonton_City_of_Edmonton_Business_Licenses]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Edmonton_City_of_Edmonton_Business_Licenses_HistEndDate]
    ON [SourceHistory].[AB_Edmonton_City_of_Edmonton_Business_Licenses]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

