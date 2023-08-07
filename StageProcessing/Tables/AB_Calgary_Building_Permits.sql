CREATE TABLE [StageProcessing].[AB_Calgary_Building_Permits] (
    [SourceID]          INT             NULL,
    [Code]              VARCHAR (200)   NULL,
    [PermitNum]         NVARCHAR (510)  NULL,
    [AppliedDate]       NVARCHAR (510)  NULL,
    [PermitClass]       VARCHAR (100)   NULL,
    [PermitClassGroup]  VARCHAR (200)   NULL,
    [PermitClassMapped] VARCHAR (100)   NULL,
    [HousingUnits]      INT             NULL,
    [TotalSqFt]         DECIMAL (17, 2) NULL,
    [OriginalAddress]   VARCHAR (500)   NULL,
    [Latitude]          VARCHAR (50)    NULL,
    [Longitude]         VARCHAR (50)    NULL,
    [Location]          VARCHAR (50)    NULL,
    [ActionType]        CHAR (1)        NULL,
    [IsDuplicate]       BIT             NULL
);

