CREATE TABLE [StageProcessingErr].[BC_NewWestminster_PARCELS_w_ATTRIBUTES] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [NwID]          NVARCHAR (MAX) NULL,
    [ZONING]        NVARCHAR (MAX) NULL,
    [ZONECAT]       NVARCHAR (MAX) NULL,
    [LANDUSE]       NVARCHAR (MAX) NULL,
    [FRONTAGE]      NVARCHAR (MAX) NULL,
    [AVGDEPTH]      NVARCHAR (MAX) NULL,
    [SITEAREA]      NVARCHAR (MAX) NULL,
    [FULLADDR1]     NVARCHAR (MAX) NULL
);

