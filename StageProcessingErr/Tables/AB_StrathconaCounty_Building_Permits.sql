CREATE TABLE [StageProcessingErr].[AB_StrathconaCounty_Building_Permits] (
    [SourceID]          INT            NULL,
    [Code]              VARCHAR (200)  NULL,
    [ErrorStatusId]     TINYINT        NULL,
    [PermitNum]         NVARCHAR (MAX) NULL,
    [OriginalAddress1]  NVARCHAR (MAX) NULL,
    [PermitClassMapped] NVARCHAR (MAX) NULL,
    [Zoning]            NVARCHAR (MAX) NULL,
    [TotalSqFt]         NVARCHAR (MAX) NULL,
    [City]              NVARCHAR (MAX) NULL,
    [Province]          NVARCHAR (MAX) NULL,
    [Jurisdiction]      NVARCHAR (MAX) NULL,
    [PIN]               NVARCHAR (MAX) NULL,
    [Latitude]          NVARCHAR (MAX) NULL,
    [Longitude]         NVARCHAR (MAX) NULL,
    [Location]          NVARCHAR (MAX) NULL,
    [Plan]              NVARCHAR (MAX) NULL
);

