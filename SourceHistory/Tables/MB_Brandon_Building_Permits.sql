CREATE TABLE [SourceHistory].[MB_Brandon_Building_Permits] (
    [Code]          VARCHAR (200)  NULL,
    [Permit Number] NVARCHAR (MAX) NULL,
    [Permit Type]   NVARCHAR (MAX) NULL,
    [Address]       NVARCHAR (MAX) NULL,
    [Roll Number]   NVARCHAR (MAX) NULL,
    [HistEndDate]   DATETIME       NULL,
    [IsDuplicate]   BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_MB_Brandon_Building_Permits_Code]
    ON [SourceHistory].[MB_Brandon_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_MB_Brandon_Building_Permits_HistEndDate]
    ON [SourceHistory].[MB_Brandon_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

