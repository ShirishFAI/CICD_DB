CREATE TABLE [SourceHistory].[BC_Langley_Property_Legal_Info] (
    [Code]                VARCHAR (200)  NULL,
    [X]                   NVARCHAR (MAX) NULL,
    [Y]                   NVARCHAR (MAX) NULL,
    [Unit]                NVARCHAR (MAX) NULL,
    [House]               NVARCHAR (MAX) NULL,
    [Street]              NVARCHAR (MAX) NULL,
    [Folio]               NVARCHAR (MAX) NULL,
    [PID]                 NVARCHAR (MAX) NULL,
    [Property_Number]     NVARCHAR (MAX) NULL,
    [Legal_Type]          NVARCHAR (MAX) NULL,
    [Plan_Number]         NVARCHAR (MAX) NULL,
    [Legal_Descr]         NVARCHAR (MAX) NULL,
    [LotSize_NotVerified] NVARCHAR (MAX) NULL,
    [Lot_UnitOfMeasure]   NVARCHAR (MAX) NULL,
    [HistEndDate]         DATETIME       NULL,
    [IsDuplicate]         BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Langley_Property_Legal_Info_Code]
    ON [SourceHistory].[BC_Langley_Property_Legal_Info]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Langley_Property_Legal_Info_HistEndDate]
    ON [SourceHistory].[BC_Langley_Property_Legal_Info]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

