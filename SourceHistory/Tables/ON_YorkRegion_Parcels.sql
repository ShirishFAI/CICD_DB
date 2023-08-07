CREATE TABLE [SourceHistory].[ON_YorkRegion_Parcels] (
    [Code]          VARCHAR (200)  NULL,
    [PAR_GIS_ID]    NVARCHAR (MAX) NULL,
    [LOCATION]      NVARCHAR (MAX) NULL,
    [MUNNAME]       NVARCHAR (MAX) NULL,
    [PLANNUM]       NVARCHAR (MAX) NULL,
    [Shape__Area]   NVARCHAR (MAX) NULL,
    [Shape__Length] NVARCHAR (MAX) NULL,
    [HistEndDate]   DATETIME       NULL,
    [IsDuplicate]   BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ON_YorkRegion_Parcels_Code]
    ON [SourceHistory].[ON_YorkRegion_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ON_YorkRegion_Parcels_HistEndDate]
    ON [SourceHistory].[ON_YorkRegion_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

