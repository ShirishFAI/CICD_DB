CREATE TABLE [StageProcessing].[AB_Edmonton_Property_Information_Data_Current_Calendar_Year] (
    [SourceID]          INT             NULL,
    [Code]              VARCHAR (200)   NULL,
    [Account Number]    VARCHAR (200)   NULL,
    [Suite]             VARCHAR (100)   NULL,
    [House Number]      VARCHAR (100)   NULL,
    [Street Name]       VARCHAR (200)   NULL,
    [Legal Description] VARCHAR (4000)  NULL,
    [Zoning]            VARCHAR (400)   NULL,
    [Lot Size]          DECIMAL (17, 2) NULL,
    [Actual Year Built] INT             NULL,
    [Neighbourhood]     VARCHAR (200)   NULL,
    [Latitude]          VARCHAR (50)    NULL,
    [Longitude]         VARCHAR (50)    NULL,
    [ActionType]        CHAR (1)        NULL,
    [IsDuplicate]       BIT             NULL
);

