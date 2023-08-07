CREATE TABLE [SourceHistory].[BC_Victoria_Parcels] (
    [Code]        VARCHAR (200)  NULL,
    [FOLIO]       NVARCHAR (MAX) NULL,
    [HOUSE]       NVARCHAR (MAX) NULL,
    [LEGAL_TYPE]  NVARCHAR (MAX) NULL,
    [ParcelArea]  NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [STREET]      NVARCHAR (MAX) NULL,
    [UNIT]        NVARCHAR (MAX) NULL,
    [ActualUse]   NVARCHAR (MAX) NULL,
    [TAXYEAR]     NVARCHAR (MAX) NULL,
    [TaxLevy]     NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Victoria_Parcels_Code]
    ON [SourceHistory].[BC_Victoria_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Victoria_Parcels_HistEndDate]
    ON [SourceHistory].[BC_Victoria_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

