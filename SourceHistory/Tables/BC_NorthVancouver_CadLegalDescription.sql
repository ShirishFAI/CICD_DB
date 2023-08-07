CREATE TABLE [SourceHistory].[BC_NorthVancouver_CadLegalDescription] (
    [Code]             VARCHAR (200)  NULL,
    [propertynumber]   NVARCHAR (MAX) NULL,
    [pid]              NVARCHAR (MAX) NULL,
    [folio]            NVARCHAR (MAX) NULL,
    [plannumber]       NVARCHAR (MAX) NULL,
    [legaldescription] NVARCHAR (MAX) NULL,
    [legaltype]        NVARCHAR (MAX) NULL,
    [HistEndDate]      DATETIME       NULL,
    [IsDuplicate]      BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_NorthVancouver_CadLegalDescription_Code]
    ON [SourceHistory].[BC_NorthVancouver_CadLegalDescription]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_NorthVancouver_CadLegalDescription_HistEndDate]
    ON [SourceHistory].[BC_NorthVancouver_CadLegalDescription]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

