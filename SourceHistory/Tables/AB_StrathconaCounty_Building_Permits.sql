CREATE TABLE [SourceHistory].[AB_StrathconaCounty_Building_Permits] (
    [Code]              VARCHAR (200)  NULL,
    [PermitNum]         NVARCHAR (MAX) NULL,
    [OriginalAddress1]  NVARCHAR (MAX) NULL,
    [PermitClassMapped] NVARCHAR (MAX) NULL,
    [Zoning]            NVARCHAR (MAX) NULL,
    [TotalSqFt]         NVARCHAR (MAX) NULL,
    [City]              NVARCHAR (MAX) NULL,
    [Province]          NVARCHAR (MAX) NULL,
    [Jurisdiction]      NVARCHAR (MAX) NULL,
    [PIN]               NVARCHAR (MAX) NULL,
    [Latitude]          NVARCHAR (MAX) NULL,
    [Longitude]         NVARCHAR (MAX) NULL,
    [Location]          NVARCHAR (MAX) NULL,
    [Plan]              NVARCHAR (MAX) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_StrathconaCounty_Building_Permits_Code]
    ON [SourceHistory].[AB_StrathconaCounty_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_StrathconaCounty_Building_Permits_HistEndDate]
    ON [SourceHistory].[AB_StrathconaCounty_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

