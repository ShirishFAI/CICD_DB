CREATE TABLE [SourceHistory].[AB_Banff_BuildingPermits] (
    [Code]                    VARCHAR (200)  NULL,
    [Roll_No]                 NVARCHAR (MAX) NULL,
    [Building_Permit_No]      NVARCHAR (MAX) NULL,
    [Date_Received]           NVARCHAR (MAX) NULL,
    [Plan]                    NVARCHAR (MAX) NULL,
    [Unit_No]                 NVARCHAR (MAX) NULL,
    [Street_No]               NVARCHAR (MAX) NULL,
    [Street_Name]             NVARCHAR (MAX) NULL,
    [Description]             NVARCHAR (MAX) NULL,
    [No_Of_New_Dweling_Units] NVARCHAR (MAX) NULL,
    [HistEndDate]             DATETIME       NULL,
    [IsDuplicate]             BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Banff_BuildingPermits_Code]
    ON [SourceHistory].[AB_Banff_BuildingPermits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Banff_BuildingPermits_HistEndDate]
    ON [SourceHistory].[AB_Banff_BuildingPermits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

