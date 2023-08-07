CREATE TABLE [SourceHistory].[AB_Banff_BusinessLicense] (
    [Code]              VARCHAR (200)  NULL,
    [BUSINESS_NAME]     NVARCHAR (MAX) NULL,
    [LICENSE_NUMBER]    NVARCHAR (MAX) NULL,
    [UNIT]              NVARCHAR (MAX) NULL,
    [STREET_NUMBER]     NVARCHAR (MAX) NULL,
    [STREET_NAME]       NVARCHAR (MAX) NULL,
    [PROPOSED_BUSINESS] NVARCHAR (MAX) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Banff_BusinessLicense_Code]
    ON [SourceHistory].[AB_Banff_BusinessLicense]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Banff_BusinessLicense_HistEndDate]
    ON [SourceHistory].[AB_Banff_BusinessLicense]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

