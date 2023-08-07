CREATE TABLE [StageProcessingErr].[MB_Winnipeg_Detailed_Building_Permit_Data] (
    [SourceID]             INT            NULL,
    [Code]                 VARCHAR (200)  NULL,
    [ErrorStatusId]        TINYINT        NULL,
    [Permit Number]        NVARCHAR (MAX) NULL,
    [Permit Group]         NVARCHAR (MAX) NULL,
    [Street Number]        NVARCHAR (MAX) NULL,
    [Street Name]          NVARCHAR (MAX) NULL,
    [Street Type]          NVARCHAR (MAX) NULL,
    [Street Direction]     NVARCHAR (MAX) NULL,
    [Unit Number]          NVARCHAR (MAX) NULL,
    [Neighbourhood Number] NVARCHAR (MAX) NULL,
    [Neighbourhood Name]   NVARCHAR (MAX) NULL,
    [Community]            NVARCHAR (MAX) NULL,
    [Location]             NVARCHAR (MAX) NULL
);

