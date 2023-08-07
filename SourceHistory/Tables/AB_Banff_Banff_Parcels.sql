CREATE TABLE [SourceHistory].[AB_Banff_Banff_Parcels] (
    [Code]        VARCHAR (200)  NULL,
    [ROLL]        NVARCHAR (MAX) NULL,
    [GIS_ID]      NVARCHAR (MAX) NULL,
    [Full_Addre]  NVARCHAR (MAX) NULL,
    [Acres]       NVARCHAR (MAX) NULL,
    [SqFt]        NVARCHAR (MAX) NULL,
    [Shape_area]  NVARCHAR (MAX) NULL,
    [Shape_len]   NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Banff_Banff_Parcels_Code]
    ON [SourceHistory].[AB_Banff_Banff_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Banff_Banff_Parcels_HistEndDate]
    ON [SourceHistory].[AB_Banff_Banff_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

