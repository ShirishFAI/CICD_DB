CREATE TABLE [StageProcessing].[NS_ALL_Parcel_Land_Sizes] (
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
    [Acreage]                   VARCHAR (50)    NULL,
    [Square Feet]               DECIMAL (17, 2) NULL,
    [Y Map Coordinate]          VARCHAR (50)    NULL,
    [X Map Coordinate]          VARCHAR (50)    NULL,
    [Map Coordinates]           VARCHAR (50)    NULL,
    [ActionType]                CHAR (1)        NULL,
    [IsDuplicate]               BIT             NULL
);

