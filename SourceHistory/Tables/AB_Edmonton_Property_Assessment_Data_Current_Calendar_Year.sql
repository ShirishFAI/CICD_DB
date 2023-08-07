CREATE TABLE [SourceHistory].[AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year] (
    [Code]             VARCHAR (200)  NULL,
    [Account Number]   NVARCHAR (MAX) NULL,
    [Suite]            NVARCHAR (MAX) NULL,
    [House Number]     NVARCHAR (MAX) NULL,
    [Street Name]      NVARCHAR (MAX) NULL,
    [Assessed Value]   NVARCHAR (MAX) NULL,
    [Assessment Class] NVARCHAR (MAX) NULL,
    [Neighbourhood ID] NVARCHAR (MAX) NULL,
    [Neighbourhood]    NVARCHAR (MAX) NULL,
    [Garage]           NVARCHAR (MAX) NULL,
    [Latitude]         NVARCHAR (MAX) NULL,
    [Longitude]        NVARCHAR (MAX) NULL,
    [HistEndDate]      DATETIME       NULL,
    [IsDuplicate]      BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year_Code]
    ON [SourceHistory].[AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year_HistEndDate]
    ON [SourceHistory].[AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

