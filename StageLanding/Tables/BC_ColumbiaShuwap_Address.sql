CREATE TABLE [StageLanding].[BC_ColumbiaShuwap_Address] (
    [IDHOUSE]    NVARCHAR (MAX) NULL,
    [IDPARCEL]   NVARCHAR (MAX) NULL,
    [ROLL]       NVARCHAR (MAX) NULL,
    [HOUSENUM]   NVARCHAR (MAX) NULL,
    [APT_BAY]    NVARCHAR (MAX) NULL,
    [STREET]     NVARCHAR (MAX) NULL,
    [HOUSETYPE]  NVARCHAR (MAX) NULL,
    [ELECT]      NVARCHAR (MAX) NULL,
    [NewHouseDa] NVARCHAR (MAX) NULL,
    [SourceID]   INT            IDENTITY (1, 1) NOT NULL,
    [Code]       VARCHAR (200)  NULL
);

