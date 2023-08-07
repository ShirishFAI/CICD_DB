CREATE TABLE [SourceHistory].[BC_Victoria_Building_Permits] (
    [Code]          VARCHAR (200)  NULL,
    [PermitNo]      NVARCHAR (MAX) NULL,
    [SUBJECT]       NVARCHAR (MAX) NULL,
    [IssuedDate]    NVARCHAR (MAX) NULL,
    [Unit]          NVARCHAR (MAX) NULL,
    [House]         NVARCHAR (MAX) NULL,
    [Street]        NVARCHAR (MAX) NULL,
    [ActualUse]     NVARCHAR (MAX) NULL,
    [Neighbourhood] NVARCHAR (MAX) NULL,
    [X_LONG]        NVARCHAR (MAX) NULL,
    [Y_LAT]         NVARCHAR (MAX) NULL,
    [PermitType]    NVARCHAR (MAX) NULL,
    [HistEndDate]   DATETIME       NULL,
    [IsDuplicate]   BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Victoria_Building_Permits_Code]
    ON [SourceHistory].[BC_Victoria_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Victoria_Building_Permits_HistEndDate]
    ON [SourceHistory].[BC_Victoria_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

