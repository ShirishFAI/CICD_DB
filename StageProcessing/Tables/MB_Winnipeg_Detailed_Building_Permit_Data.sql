CREATE TABLE [StageProcessing].[MB_Winnipeg_Detailed_Building_Permit_Data] (
    [SourceID]             INT            NULL,
    [Code]                 VARCHAR (200)  NULL,
    [Permit Number]        NVARCHAR (510) NULL,
    [Permit Group]         VARCHAR (100)  NULL,
    [Street Number]        VARCHAR (100)  NULL,
    [Street Name]          VARCHAR (200)  NULL,
    [Street Type]          VARCHAR (200)  NULL,
    [Street Direction]     VARCHAR (20)   NULL,
    [Unit Number]          VARCHAR (100)  NULL,
    [Neighbourhood Number] VARCHAR (100)  NULL,
    [Neighbourhood Name]   VARCHAR (200)  NULL,
    [Community]            VARCHAR (100)  NULL,
    [Location]             VARCHAR (50)   NULL,
    [ActionType]           CHAR (1)       NULL,
    [IsDuplicate]          BIT            NULL
);

