CREATE TABLE [SourceHistory].[BC_ColumbiaShuwap_Address] (
    [Code]        VARCHAR (200)  NULL,
    [IDHOUSE]     NVARCHAR (MAX) NULL,
    [IDPARCEL]    NVARCHAR (MAX) NULL,
    [ROLL]        NVARCHAR (MAX) NULL,
    [HOUSENUM]    NVARCHAR (MAX) NULL,
    [APT_BAY]     NVARCHAR (MAX) NULL,
    [STREET]      NVARCHAR (MAX) NULL,
    [HOUSETYPE]   NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_ColumbiaShuwap_Address_Code]
    ON [SourceHistory].[BC_ColumbiaShuwap_Address]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_ColumbiaShuwap_Address_HistEndDate]
    ON [SourceHistory].[BC_ColumbiaShuwap_Address]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

