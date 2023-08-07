CREATE TABLE [SourceHistory].[BC_Saanich_Parcels] (
    [Code]        VARCHAR (200)  NULL,
    [JURISDICT]   NVARCHAR (MAX) NULL,
    [FOLIO]       NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [STRUNIT]     NVARCHAR (MAX) NULL,
    [STRNUMBER]   NVARCHAR (MAX) NULL,
    [STRNAME]     NVARCHAR (MAX) NULL,
    [LEGPLAN]     NVARCHAR (MAX) NULL,
    [AREASQM]     NVARCHAR (MAX) NULL,
    [AREAHECT]    NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Saanich_Parcels_Code]
    ON [SourceHistory].[BC_Saanich_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Saanich_Parcels_HistEndDate]
    ON [SourceHistory].[BC_Saanich_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

