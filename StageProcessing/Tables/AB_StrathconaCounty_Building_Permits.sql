CREATE TABLE [StageProcessing].[AB_StrathconaCounty_Building_Permits] (
    [SourceID]          INT             NULL,
    [Code]              VARCHAR (200)   NULL,
    [PermitNum]         NVARCHAR (510)  NULL,
    [OriginalAddress1]  VARCHAR (500)   NULL,
    [PermitClassMapped] VARCHAR (100)   NULL,
    [Zoning]            VARCHAR (400)   NULL,
    [TotalSqFt]         DECIMAL (17, 2) NULL,
    [City]              VARCHAR (200)   NULL,
    [Province]          VARCHAR (50)    NULL,
    [Jurisdiction]      VARCHAR (100)   NULL,
    [PIN]               VARCHAR (50)    NULL,
    [Latitude]          VARCHAR (50)    NULL,
    [Longitude]         VARCHAR (50)    NULL,
    [Location]          VARCHAR (50)    NULL,
    [Plan]              VARCHAR (100)   NULL,
    [ActionType]        CHAR (1)        NULL,
    [IsDuplicate]       BIT             NULL
);

