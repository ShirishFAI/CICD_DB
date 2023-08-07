CREATE TABLE [StageProcessing].[BC_Squamish_Land_Parcels] (
    [SourceID]      INT             NULL,
    [Code]          VARCHAR (200)   NULL,
    [OBJECTID]      NVARCHAR (510)  NULL,
    [GIS_ID]        NVARCHAR (510)  NULL,
    [ROLL]          VARCHAR (200)   NULL,
    [PID]           VARCHAR (50)    NULL,
    [CIVIC_ADDRESS] VARCHAR (500)   NULL,
    [LTSA_PLAN]     VARCHAR (100)   NULL,
    [ZONE_CODE]     VARCHAR (400)   NULL,
    [ZONE_DESC]     VARCHAR (400)   NULL,
    [AREA_HA]       DECIMAL (17, 2) NULL,
    [AREA_FT]       DECIMAL (17, 2) NULL,
    [ActionType]    CHAR (1)        NULL,
    [IsDuplicate]   BIT             NULL
);

