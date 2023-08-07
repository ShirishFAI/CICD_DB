CREATE TABLE [StageProcessing].[NS_Halifax_Building_Details] (
    [SourceID]                INT             NULL,
    [Code]                    VARCHAR (200)   NULL,
    [BUILDING_ID]             NVARCHAR (510)  NULL,
    [YEAR_OF_CONSTRUCTION]    INT             NULL,
    [TOTAL_SQUARE_FOOTAGE]    DECIMAL (17, 2) NULL,
    [USE_ID]                  NVARCHAR (510)  NULL,
    [BUILDING_CLASSIFICATION] VARCHAR (100)   NULL,
    [BUILDING_USE]            VARCHAR (255)   NULL,
    [DWELLING_UNITS]          INT             NULL,
    [PID]                     VARCHAR (50)    NULL,
    [CIVIC_NUMBER]            VARCHAR (100)   NULL,
    [STREET_NAME]             VARCHAR (200)   NULL,
    [STREET_TYPE]             VARCHAR (200)   NULL,
    [COMMUNITY_NAME]          VARCHAR (200)   NULL,
    [ActionType]              CHAR (1)        NULL,
    [IsDuplicate]             BIT             NULL
);

