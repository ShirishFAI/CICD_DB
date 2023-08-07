CREATE TABLE [SourceHistory].[ON_Brampton_Building_Permits] (
    [Code]         VARCHAR (200)  NULL,
    [X]            NVARCHAR (MAX) NULL,
    [Y]            NVARCHAR (MAX) NULL,
    [ADDRESS]      NVARCHAR (MAX) NULL,
    [FOLDERRSN]    NVARCHAR (MAX) NULL,
    [PERMITNUMBER] NVARCHAR (MAX) NULL,
    [SUBDESC]      NVARCHAR (MAX) NULL,
    [WORKDESC]     NVARCHAR (MAX) NULL,
    [GFA]          NVARCHAR (MAX) NULL,
    [BEDROOMS]     NVARCHAR (MAX) NULL,
    [STOREYS]      NVARCHAR (MAX) NULL,
    [HistEndDate]  DATETIME       NULL,
    [IsDuplicate]  BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ON_Brampton_Building_Permits_Code]
    ON [SourceHistory].[ON_Brampton_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ON_Brampton_Building_Permits_HistEndDate]
    ON [SourceHistory].[ON_Brampton_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

