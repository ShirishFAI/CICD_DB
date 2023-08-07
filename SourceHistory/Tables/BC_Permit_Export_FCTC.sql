CREATE TABLE [SourceHistory].[BC_Permit_Export_FCTC] (
    [Code]           VARCHAR (200)  NULL,
    [Jurisdiction]   NVARCHAR (MAX) NULL,
    [RollNumber]     NVARCHAR (MAX) NULL,
    [ServiceDate]    NVARCHAR (MAX) NULL,
    [PermitNumber]   NVARCHAR (MAX) NULL,
    [DemolitionFlag] NVARCHAR (MAX) NULL,
    [HashBytes]      BINARY (64)    NULL,
    [HistEndDate]    DATETIME       NULL,
    [IsDuplicate]    BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Permit_Export_FCTC_Code]
    ON [SourceHistory].[BC_Permit_Export_FCTC]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Permit_Export_FCTC_HistEndDate]
    ON [SourceHistory].[BC_Permit_Export_FCTC]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

