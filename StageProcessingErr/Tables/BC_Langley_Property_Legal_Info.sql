CREATE TABLE [StageProcessingErr].[BC_Langley_Property_Legal_Info] (
    [SourceID]            INT            NULL,
    [Code]                VARCHAR (200)  NULL,
    [ErrorStatusId]       TINYINT        NULL,
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
    [Lot_UnitOfMeasure]   NVARCHAR (MAX) NULL
);

