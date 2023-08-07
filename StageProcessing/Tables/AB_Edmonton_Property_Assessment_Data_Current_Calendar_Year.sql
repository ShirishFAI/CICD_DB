CREATE TABLE [StageProcessing].[AB_Edmonton_Property_Assessment_Data_Current_Calendar_Year] (
    [SourceID]         INT             NULL,
    [Code]             VARCHAR (200)   NULL,
    [Account Number]   NVARCHAR (510)  NULL,
    [Suite]            VARCHAR (100)   NULL,
    [House Number]     VARCHAR (100)   NULL,
    [Street Name]      VARCHAR (200)   NULL,
    [Assessed Value]   DECIMAL (17, 2) NULL,
    [Assessment Class] VARCHAR (200)   NULL,
    [Neighbourhood ID] VARCHAR (100)   NULL,
    [Neighbourhood]    VARCHAR (200)   NULL,
    [Garage]           VARCHAR (20)    NULL,
    [Latitude]         VARCHAR (50)    NULL,
    [Longitude]        VARCHAR (50)    NULL,
    [ActionType]       CHAR (1)        NULL,
    [IsDuplicate]      BIT             NULL
);

