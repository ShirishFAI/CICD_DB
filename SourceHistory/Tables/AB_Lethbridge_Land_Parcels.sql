CREATE TABLE [SourceHistory].[AB_Lethbridge_Land_Parcels] (
    [Code]          VARCHAR (200)  NULL,
    [Neighbourhood] NVARCHAR (MAX) NULL,
    [Shape_Length]  NVARCHAR (MAX) NULL,
    [Shape_Area]    NVARCHAR (MAX) NULL,
    [IsResidential] NVARCHAR (MAX) NULL,
    [AreaSpec]      NVARCHAR (MAX) NULL,
    [ZoneDesc]      NVARCHAR (MAX) NULL,
    [ParcelID]      NVARCHAR (MAX) NULL,
    [Roll]          NVARCHAR (MAX) NULL,
    [PlanNumber]    NVARCHAR (MAX) NULL,
    [Lot]           NVARCHAR (MAX) NULL,
    [Address]       NVARCHAR (MAX) NULL,
    [Zone]          NVARCHAR (MAX) NULL,
    [HistEndDate]   DATETIME       NULL,
    [IsDuplicate]   BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Lethbridge_Land_Parcels_Code]
    ON [SourceHistory].[AB_Lethbridge_Land_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Lethbridge_Land_Parcels_HistEndDate]
    ON [SourceHistory].[AB_Lethbridge_Land_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

