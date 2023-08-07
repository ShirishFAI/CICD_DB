CREATE TABLE [StageProcessingErr].[BC_Nanaimo_PARCELS] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [TYPE]          NVARCHAR (MAX) NULL,
    [HOUSENUMBE]    NVARCHAR (MAX) NULL,
    [STREETNAME]    NVARCHAR (MAX) NULL,
    [POSTALCODE]    NVARCHAR (MAX) NULL,
    [AREA]          NVARCHAR (MAX) NULL,
    [PLAN]          NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [GID]           NVARCHAR (MAX) NULL,
    [ZONING1]       NVARCHAR (MAX) NULL,
    [ZONE1_DESC]    NVARCHAR (MAX) NULL
);

