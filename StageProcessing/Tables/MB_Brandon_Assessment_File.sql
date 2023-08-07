CREATE TABLE [StageProcessing].[MB_Brandon_Assessment_File] (
    [SourceID]       INT             NULL,
    [Code]           VARCHAR (200)   NULL,
    [ROLL_NUMBER]    VARCHAR (200)   NULL,
    [STREET_ADDRESS] VARCHAR (500)   NULL,
    [GROSS_TAX]      DECIMAL (17, 2) NULL,
    [NET_TAX]        DECIMAL (17, 2) NULL,
    [TAX_YEAR]       INT             NULL,
    [ActionType]     CHAR (1)        NULL,
    [IsDuplicate]    BIT             NULL
);

