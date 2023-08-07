CREATE TABLE [SourceHistory].[AB_Calgary_Building_Permits] (
    [Code]              VARCHAR (200)  NULL,
    [PermitNum]         NVARCHAR (MAX) NULL,
    [AppliedDate]       NVARCHAR (MAX) NULL,
    [PermitClass]       NVARCHAR (MAX) NULL,
    [PermitClassGroup]  NVARCHAR (MAX) NULL,
    [PermitClassMapped] NVARCHAR (MAX) NULL,
    [HousingUnits]      NVARCHAR (MAX) NULL,
    [TotalSqFt]         NVARCHAR (MAX) NULL,
    [OriginalAddress]   NVARCHAR (MAX) NULL,
    [Latitude]          NVARCHAR (MAX) NULL,
    [Longitude]         NVARCHAR (MAX) NULL,
    [Location]          NVARCHAR (MAX) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Calgary_Building_Permits_Code]
    ON [SourceHistory].[AB_Calgary_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Calgary_Building_Permits_HistEndDate]
    ON [SourceHistory].[AB_Calgary_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

