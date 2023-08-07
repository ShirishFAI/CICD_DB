CREATE TABLE [StageProcessing].[BC_Nanaimo_PARCELS] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [TYPE]        VARCHAR (200)   NULL,
    [HOUSENUMBE]  VARCHAR (100)   NULL,
    [STREETNAME]  VARCHAR (200)   NULL,
    [POSTALCODE]  VARCHAR (50)    NULL,
    [AREA]        DECIMAL (17, 2) NULL,
    [PLAN]        VARCHAR (100)   NULL,
    [PID]         VARCHAR (50)    NULL,
    [GID]         NVARCHAR (510)  NULL,
    [ZONING1]     VARCHAR (400)   NULL,
    [ZONE1_DESC]  VARCHAR (400)   NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

