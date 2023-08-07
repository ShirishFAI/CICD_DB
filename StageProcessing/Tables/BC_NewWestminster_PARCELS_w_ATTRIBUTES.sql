CREATE TABLE [StageProcessing].[BC_NewWestminster_PARCELS_w_ATTRIBUTES] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [NwID]        NVARCHAR (510)  NULL,
    [ZONING]      VARCHAR (400)   NULL,
    [ZONECAT]     VARCHAR (400)   NULL,
    [LANDUSE]     VARCHAR (255)   NULL,
    [FRONTAGE]    VARCHAR (50)    NULL,
    [AVGDEPTH]    VARCHAR (50)    NULL,
    [SITEAREA]    DECIMAL (17, 2) NULL,
    [FULLADDR1]   VARCHAR (500)   NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

