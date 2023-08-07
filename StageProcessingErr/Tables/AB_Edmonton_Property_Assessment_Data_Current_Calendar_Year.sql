CREATE TABLE [StageProcessingErr].[AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [ErrorStatusId]    TINYINT        NULL,
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
    [Longitude]        NVARCHAR (MAX) NULL
);

