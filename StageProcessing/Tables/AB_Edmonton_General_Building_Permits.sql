CREATE TABLE [StageProcessing].[AB_Edmonton_General_Building_Permits] (
    [SourceID]             INT            NULL,
    [Code]                 VARCHAR (200)  NULL,
    [Row ID]               NVARCHAR (510) NULL,
    [ADDRESS]              VARCHAR (500)  NULL,
    [LEGAL_DESCRIPTION]    VARCHAR (4000) NULL,
    [NEIGHBOURHOOD]        VARCHAR (200)  NULL,
    [NEIGHBOURHOOD_NUMBER] VARCHAR (100)  NULL,
    [BUILDING_TYPE]        VARCHAR (255)  NULL,
    [ZONING]               VARCHAR (400)  NULL,
    [LATITUDE]             VARCHAR (50)   NULL,
    [LONGITUDE]            VARCHAR (50)   NULL,
    [LOCATION]             VARCHAR (50)   NULL,
    [ActionType]           CHAR (1)       NULL,
    [IsDuplicate]          BIT            NULL
);

