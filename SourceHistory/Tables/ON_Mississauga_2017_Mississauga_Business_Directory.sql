CREATE TABLE [SourceHistory].[ON_Mississauga_2017_Mississauga_Business_Directory] (
    [Code]        VARCHAR (200)  NULL,
    [BID]         NVARCHAR (MAX) NULL,
    [Name]        NVARCHAR (MAX) NULL,
    [StreetNo]    NVARCHAR (MAX) NULL,
    [StreetName]  NVARCHAR (MAX) NULL,
    [UnitNo]      NVARCHAR (MAX) NULL,
    [PostalCode]  NVARCHAR (MAX) NULL,
    [NAICSCode]   NVARCHAR (MAX) NULL,
    [NAICSTitle]  NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ON_Mississauga_2017_Mississauga_Business_Directory_Code]
    ON [SourceHistory].[ON_Mississauga_2017_Mississauga_Business_Directory]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ON_Mississauga_2017_Mississauga_Business_Directory_HistEndDate]
    ON [SourceHistory].[ON_Mississauga_2017_Mississauga_Business_Directory]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

