CREATE TABLE [SourceHistory].[AB_Banff_DevelopmentPermits] (
    [Code]             VARCHAR (200)  NULL,
    [Roll_No]          NVARCHAR (MAX) NULL,
    [Application_No]   NVARCHAR (MAX) NULL,
    [Date_Of_Decision] NVARCHAR (MAX) NULL,
    [Plan]             NVARCHAR (MAX) NULL,
    [Unit_No]          NVARCHAR (MAX) NULL,
    [Street_No]        NVARCHAR (MAX) NULL,
    [Street_Name]      NVARCHAR (MAX) NULL,
    [HistEndDate]      DATETIME       NULL,
    [IsDuplicate]      BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Banff_DevelopmentPermits_Code]
    ON [SourceHistory].[AB_Banff_DevelopmentPermits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Banff_DevelopmentPermits_HistEndDate]
    ON [SourceHistory].[AB_Banff_DevelopmentPermits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

