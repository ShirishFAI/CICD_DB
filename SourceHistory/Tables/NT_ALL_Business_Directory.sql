CREATE TABLE [SourceHistory].[NT_ALL_Business_Directory] (
    [Code]         VARCHAR (200)  NULL,
    [BUSINESSNAME] NVARCHAR (MAX) NULL,
    [BUSINESSTYPE] NVARCHAR (MAX) NULL,
    [ADDRESS1]     NVARCHAR (MAX) NULL,
    [CITY]         NVARCHAR (MAX) NULL,
    [PROVINCE]     NVARCHAR (MAX) NULL,
    [POSTALCODE]   NVARCHAR (MAX) NULL,
    [HistEndDate]  DATETIME       NULL,
    [IsDuplicate]  BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_NT_ALL_Business_Directory_Code]
    ON [SourceHistory].[NT_ALL_Business_Directory]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_NT_ALL_Business_Directory_HistEndDate]
    ON [SourceHistory].[NT_ALL_Business_Directory]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

