CREATE TABLE [StageProcessing].[AB_Edmonton_City_of_Edmonton_Business_Licenses] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [Address]          VARCHAR (500)  NULL,
    [Licence Number]   NVARCHAR (510) NULL,
    [Neighbourhood ID] VARCHAR (100)  NULL,
    [Neighbourhood]    VARCHAR (200)  NULL,
    [Latitude]         VARCHAR (50)   NULL,
    [Longitude]        VARCHAR (50)   NULL,
    [Location]         VARCHAR (50)   NULL,
    [ActionType]       CHAR (1)       NULL,
    [IsDuplicate]      BIT            NULL
);

