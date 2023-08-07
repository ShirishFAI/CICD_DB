CREATE TABLE [StageProcessingErr].[AB_Calgary_Building_Permits] (
    [SourceID]          INT            NULL,
    [Code]              VARCHAR (200)  NULL,
    [ErrorStatusId]     TINYINT        NULL,
    [PermitNum]         NVARCHAR (MAX) NULL,
    [AppliedDate]       NVARCHAR (MAX) NULL,
    [PermitClass]       NVARCHAR (MAX) NULL,
    [PermitClassGroup]  NVARCHAR (MAX) NULL,
    [PermitClassMapped] NVARCHAR (MAX) NULL,
    [HousingUnits]      NVARCHAR (MAX) NULL,
    [TotalSqFt]         NVARCHAR (MAX) NULL,
    [OriginalAddress]   NVARCHAR (MAX) NULL,
    [Latitude]          NVARCHAR (MAX) NULL,
    [Longitude]         NVARCHAR (MAX) NULL,
    [Location]          NVARCHAR (MAX) NULL
);

