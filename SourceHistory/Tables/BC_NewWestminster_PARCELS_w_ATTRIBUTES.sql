CREATE TABLE [SourceHistory].[BC_NewWestminster_PARCELS_w_ATTRIBUTES] (
    [Code]        VARCHAR (200)  NULL,
    [NwID]        NVARCHAR (MAX) NULL,
    [ZONING]      NVARCHAR (MAX) NULL,
    [ZONECAT]     NVARCHAR (MAX) NULL,
    [LANDUSE]     NVARCHAR (MAX) NULL,
    [FRONTAGE]    NVARCHAR (MAX) NULL,
    [AVGDEPTH]    NVARCHAR (MAX) NULL,
    [SITEAREA]    NVARCHAR (MAX) NULL,
    [FULLADDR1]   NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_NewWestminster_PARCELS_w_ATTRIBUTES_Code]
    ON [SourceHistory].[BC_NewWestminster_PARCELS_w_ATTRIBUTES]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_NewWestminster_PARCELS_w_ATTRIBUTES_HistEndDate]
    ON [SourceHistory].[BC_NewWestminster_PARCELS_w_ATTRIBUTES]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

