CREATE TABLE [SourceHistory].[MB_Brandon_Assessment_File] (
    [Code]           VARCHAR (200)  NULL,
    [ROLL_NUMBER]    NVARCHAR (MAX) NULL,
    [STREET_ADDRESS] NVARCHAR (MAX) NULL,
    [GROSS_TAX]      NVARCHAR (MAX) NULL,
    [NET_TAX]        NVARCHAR (MAX) NULL,
    [TAX_YEAR]       NVARCHAR (MAX) NULL,
    [HistEndDate]    DATETIME       NULL,
    [IsDuplicate]    BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_MB_Brandon_Assessment_File_Code]
    ON [SourceHistory].[MB_Brandon_Assessment_File]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_MB_Brandon_Assessment_File_HistEndDate]
    ON [SourceHistory].[MB_Brandon_Assessment_File]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

