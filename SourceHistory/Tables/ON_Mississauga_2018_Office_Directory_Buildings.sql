CREATE TABLE [SourceHistory].[ON_Mississauga_2018_Office_Directory_Buildings] (
    [Code]        VARCHAR (200)  NULL,
    [BLDG_ID]     NVARCHAR (MAX) NULL,
    [Address]     NVARCHAR (MAX) NULL,
    [OFF_GFA_m2]  NVARCHAR (MAX) NULL,
    [OFF_GFAft2]  NVARCHAR (MAX) NULL,
    [Storeys]     NVARCHAR (MAX) NULL,
    [Year_Built]  NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ON_Mississauga_2018_Office_Directory_Buildings_Code]
    ON [SourceHistory].[ON_Mississauga_2018_Office_Directory_Buildings]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ON_Mississauga_2018_Office_Directory_Buildings_HistEndDate]
    ON [SourceHistory].[ON_Mississauga_2018_Office_Directory_Buildings]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

