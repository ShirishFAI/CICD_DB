CREATE TABLE [SourceHistory].[NS_Halifax_Building_Details] (
    [Code]                    VARCHAR (200)  NULL,
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
    [COMMUNITY_NAME]          NVARCHAR (MAX) NULL,
    [HistEndDate]             DATETIME       NULL,
    [IsDuplicate]             BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_NS_Halifax_Building_Details_Code]
    ON [SourceHistory].[NS_Halifax_Building_Details]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_NS_Halifax_Building_Details_HistEndDate]
    ON [SourceHistory].[NS_Halifax_Building_Details]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

