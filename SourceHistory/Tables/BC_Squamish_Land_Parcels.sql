CREATE TABLE [SourceHistory].[BC_Squamish_Land_Parcels] (
    [Code]          VARCHAR (200)  NULL,
    [OBJECTID]      NVARCHAR (MAX) NULL,
    [GIS_ID]        NVARCHAR (MAX) NULL,
    [ROLL]          NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [CIVIC_ADDRESS] NVARCHAR (MAX) NULL,
    [LTSA_PLAN]     NVARCHAR (MAX) NULL,
    [ZONE_CODE]     NVARCHAR (MAX) NULL,
    [ZONE_DESC]     NVARCHAR (MAX) NULL,
    [AREA_HA]       NVARCHAR (MAX) NULL,
    [AREA_FT]       NVARCHAR (MAX) NULL,
    [HistEndDate]   DATETIME       NULL,
    [IsDuplicate]   BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Squamish_Land_Parcels_Code]
    ON [SourceHistory].[BC_Squamish_Land_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Squamish_Land_Parcels_HistEndDate]
    ON [SourceHistory].[BC_Squamish_Land_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

