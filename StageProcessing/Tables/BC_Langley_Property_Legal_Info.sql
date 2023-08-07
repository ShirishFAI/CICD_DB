CREATE TABLE [StageProcessing].[BC_Langley_Property_Legal_Info] (
    [SourceID]            INT             NULL,
    [Code]                VARCHAR (200)   NULL,
    [X]                   VARCHAR (50)    NULL,
    [Y]                   VARCHAR (50)    NULL,
    [Unit]                VARCHAR (100)   NULL,
    [House]               VARCHAR (100)   NULL,
    [Street]              VARCHAR (200)   NULL,
    [Folio]               VARCHAR (200)   NULL,
    [PID]                 VARCHAR (50)    NULL,
    [Property_Number]     NVARCHAR (510)  NULL,
    [Legal_Type]          VARCHAR (200)   NULL,
    [Plan_Number]         VARCHAR (100)   NULL,
    [Legal_Descr]         VARCHAR (4000)  NULL,
    [LotSize_NotVerified] DECIMAL (17, 2) NULL,
    [Lot_UnitOfMeasure]   VARCHAR (50)    NULL,
    [ActionType]          CHAR (1)        NULL,
    [IsDuplicate]         BIT             NULL
);

