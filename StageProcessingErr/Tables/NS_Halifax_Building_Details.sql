CREATE TABLE [StageProcessingErr].[NS_Halifax_Building_Details] (
    [SourceID]                INT            NULL,
    [Code]                    VARCHAR (200)  NULL,
    [ErrorStatusId]           TINYINT        NULL,
    [BUILDING_ID]             NVARCHAR (MAX) NULL,
    [YEAR_OF_CONSTRUCTION]    NVARCHAR (MAX) NULL,
    [TOTAL_SQUARE_FOOTAGE]    NVARCHAR (MAX) NULL,
    [USE_ID]                  NVARCHAR (MAX) NULL,
    [BUILDING_CLASSIFICATION] NVARCHAR (MAX) NULL,
    [BUILDING_USE]            NVARCHAR (MAX) NULL,
    [DWELLING_UNITS]          NVARCHAR (MAX) NULL,
    [PID]                     NVARCHAR (MAX) NULL,
    [CIVIC_NUMBER]            NVARCHAR (MAX) NULL,
    [STREET_NAME]             NVARCHAR (MAX) NULL,
    [STREET_TYPE]             NVARCHAR (MAX) NULL,
    [COMMUNITY_NAME]          NVARCHAR (MAX) NULL
);

