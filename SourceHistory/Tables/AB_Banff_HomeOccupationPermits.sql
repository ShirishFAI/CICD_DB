CREATE TABLE [SourceHistory].[AB_Banff_HomeOccupationPermits] (
    [Code]           VARCHAR (200)  NULL,
    [Roll_No]        NVARCHAR (MAX) NULL,
    [Application_No] NVARCHAR (MAX) NULL,
    [Date_Received]  NVARCHAR (MAX) NULL,
    [Plan]           NVARCHAR (MAX) NULL,
    [Unit_No]        NVARCHAR (MAX) NULL,
    [Street_No]      NVARCHAR (MAX) NULL,
    [Street_Name]    NVARCHAR (MAX) NULL,
    [HistEndDate]    DATETIME       NULL,
    [IsDuplicate]    BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Banff_HomeOccupationPermits_Code]
    ON [SourceHistory].[AB_Banff_HomeOccupationPermits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Banff_HomeOccupationPermits_HistEndDate]
    ON [SourceHistory].[AB_Banff_HomeOccupationPermits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

