CREATE TABLE [StageProcessing].[NS_ALL_Assessed_Value_and_Taxable_Assessed_Value_History] (
    [SourceID]                  INT             NULL,
    [Code]                      VARCHAR (200)   NULL,
    [Municipal Unit]            VARCHAR (100)   NULL,
    [Assessment Account Number] VARCHAR (200)   NULL,
    [Civic Number]              VARCHAR (100)   NULL,
    [Civic Additional]          VARCHAR (100)   NULL,
    [Civic Direction]           VARCHAR (20)    NULL,
    [Civic Street Name]         VARCHAR (200)   NULL,
    [Civic Street Suffix]       VARCHAR (200)   NULL,
    [Civic City Name]           VARCHAR (200)   NULL,
    [Tax Year]                  INT             NULL,
    [Assessed Value]            DECIMAL (17, 2) NULL,
    [Taxable Assessed Value]    DECIMAL (17, 2) NULL,
    [Y Map Coordinate]          VARCHAR (50)    NULL,
    [X Map Coordinate]          VARCHAR (50)    NULL,
    [Map Coordinates]           VARCHAR (50)    NULL,
    [ActionType]                CHAR (1)        NULL,
    [IsDuplicate]               BIT             NULL
);

