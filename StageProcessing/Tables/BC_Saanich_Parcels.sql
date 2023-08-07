CREATE TABLE [StageProcessing].[BC_Saanich_Parcels] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [JURISDICT]   VARCHAR (10)    NULL,
    [FOLIO]       VARCHAR (200)   NULL,
    [PID]         VARCHAR (50)    NULL,
    [STRUNIT]     VARCHAR (100)   NULL,
    [STRNUMBER]   VARCHAR (100)   NULL,
    [STRNAME]     VARCHAR (200)   NULL,
    [LEGPLAN]     VARCHAR (100)   NULL,
    [AREASQM]     DECIMAL (17, 2) NULL,
    [AREAHECT]    DECIMAL (17, 2) NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

