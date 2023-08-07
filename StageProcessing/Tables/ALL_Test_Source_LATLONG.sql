CREATE TABLE [StageProcessing].[ALL_Test_Source_LATLONG] (
    [SourceID]     INT           NULL,
    [Code]         VARCHAR (200) NULL,
    [StreetNumber] VARCHAR (100) NULL,
    [StreetName]   VARCHAR (200) NULL,
    [StreetType]   VARCHAR (200) NULL,
    [PostalCode]   VARCHAR (50)  NULL,
    [City]         VARCHAR (200) NULL,
    [ProvinceCode] VARCHAR (50)  NULL,
    [HashBytes]    BINARY (64)   NULL,
    [ActionType]   CHAR (1)      NULL,
    [IsDuplicate]  BIT           NULL
);

