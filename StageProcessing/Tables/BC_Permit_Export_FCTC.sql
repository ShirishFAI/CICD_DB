CREATE TABLE [StageProcessing].[BC_Permit_Export_FCTC] (
    [SourceID]       INT           NULL,
    [Code]           VARCHAR (200) NULL,
    [Jurisdiction]   VARCHAR (10)  NULL,
    [RollNumber]     VARCHAR (200) NULL,
    [ServiceDate]    VARCHAR (16)  NULL,
    [PermitNumber]   VARCHAR (30)  NULL,
    [DemolitionFlag] VARCHAR (20)  NULL,
    [HashBytes]      BINARY (64)   NULL,
    [ActionType]     CHAR (1)      NULL,
    [IsDuplicate]    BIT           NULL
);

