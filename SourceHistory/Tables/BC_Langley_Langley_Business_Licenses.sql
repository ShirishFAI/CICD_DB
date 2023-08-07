CREATE TABLE [SourceHistory].[BC_Langley_Langley_Business_Licenses] (
    [Code]               VARCHAR (200)  NULL,
    [X]                  NVARCHAR (MAX) NULL,
    [Y]                  NVARCHAR (MAX) NULL,
    [PropertyNumber]     NVARCHAR (MAX) NULL,
    [Civic_Unit]         NVARCHAR (MAX) NULL,
    [Civic_House]        NVARCHAR (MAX) NULL,
    [Civic_Street]       NVARCHAR (MAX) NULL,
    [CommunityName]      NVARCHAR (MAX) NULL,
    [Category]           NVARCHAR (MAX) NULL,
    [NAICS_Primary]      NVARCHAR (MAX) NULL,
    [NAICS_Primary_Desc] NVARCHAR (MAX) NULL,
    [HistEndDate]        DATETIME       NULL,
    [IsDuplicate]        BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Langley_Langley_Business_Licenses_Code]
    ON [SourceHistory].[BC_Langley_Langley_Business_Licenses]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Langley_Langley_Business_Licenses_HistEndDate]
    ON [SourceHistory].[BC_Langley_Langley_Business_Licenses]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

