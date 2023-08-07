CREATE TABLE [StageProcessingErr].[BC_Squamish_Land_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [OBJECTID]      NVARCHAR (MAX) NULL,
    [GIS_ID]        NVARCHAR (MAX) NULL,
    [ROLL]          NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [CIVIC_ADDRESS] NVARCHAR (MAX) NULL,
    [LTSA_PLAN]     NVARCHAR (MAX) NULL,
    [ZONE_CODE]     NVARCHAR (MAX) NULL,
    [ZONE_DESC]     NVARCHAR (MAX) NULL,
    [AREA_HA]       NVARCHAR (MAX) NULL,
    [AREA_FT]       NVARCHAR (MAX) NULL
);

