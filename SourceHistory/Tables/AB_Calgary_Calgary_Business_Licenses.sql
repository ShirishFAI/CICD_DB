CREATE TABLE [SourceHistory].[AB_Calgary_Calgary_Business_Licenses] (
    [Code]         VARCHAR (200)  NULL,
    [TRADENAME]    NVARCHAR (MAX) NULL,
    [ADDRESS]      NVARCHAR (MAX) NULL,
    [LICENCETYPES] NVARCHAR (MAX) NULL,
    [longitude]    NVARCHAR (MAX) NULL,
    [latitude]     NVARCHAR (MAX) NULL,
    [location]     NVARCHAR (MAX) NULL,
    [HistEndDate]  DATETIME       NULL,
    [IsDuplicate]  BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Calgary_Calgary_Business_Licenses_Code]
    ON [SourceHistory].[AB_Calgary_Calgary_Business_Licenses]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Calgary_Calgary_Business_Licenses_HistEndDate]
    ON [SourceHistory].[AB_Calgary_Calgary_Business_Licenses]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

