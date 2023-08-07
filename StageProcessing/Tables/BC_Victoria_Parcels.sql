CREATE TABLE [StageProcessing].[BC_Victoria_Parcels] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [FOLIO]       VARCHAR (200)   NULL,
    [HOUSE]       VARCHAR (100)   NULL,
    [LEGAL_TYPE]  VARCHAR (100)   NULL,
    [ParcelArea]  DECIMAL (17, 2) NULL,
    [PID]         VARCHAR (50)    NULL,
    [STREET]      VARCHAR (200)   NULL,
    [UNIT]        VARCHAR (100)   NULL,
    [ActualUse]   VARCHAR (255)   NULL,
    [TAXYEAR]     INT             NULL,
    [TaxLevy]     DECIMAL (17, 2) NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

