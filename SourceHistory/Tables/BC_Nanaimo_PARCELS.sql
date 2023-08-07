CREATE TABLE [SourceHistory].[BC_Nanaimo_PARCELS] (
    [Code]        VARCHAR (200)  NULL,
    [TYPE]        NVARCHAR (MAX) NULL,
    [HOUSENUMBE]  NVARCHAR (MAX) NULL,
    [STREETNAME]  NVARCHAR (MAX) NULL,
    [POSTALCODE]  NVARCHAR (MAX) NULL,
    [AREA]        NVARCHAR (MAX) NULL,
    [PLAN]        NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [GID]         NVARCHAR (MAX) NULL,
    [ZONING1]     NVARCHAR (MAX) NULL,
    [ZONE1_DESC]  NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Nanaimo_PARCELS_Code]
    ON [SourceHistory].[BC_Nanaimo_PARCELS]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Nanaimo_PARCELS_HistEndDate]
    ON [SourceHistory].[BC_Nanaimo_PARCELS]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

