CREATE TABLE [StageProcessing].[ON_Brampton_Building_Permits] (
    [SourceID]     INT             NULL,
    [Code]         VARCHAR (200)   NULL,
    [X]            VARCHAR (50)    NULL,
    [Y]            VARCHAR (50)    NULL,
    [ADDRESS]      VARCHAR (500)   NULL,
    [FOLDERRSN]    NVARCHAR (510)  NULL,
    [PERMITNUMBER] NVARCHAR (510)  NULL,
    [SUBDESC]      VARCHAR (255)   NULL,
    [WORKDESC]     VARCHAR (100)   NULL,
    [GFA]          DECIMAL (17, 2) NULL,
    [BEDROOMS]     DECIMAL (17, 2) NULL,
    [STOREYS]      VARCHAR (100)   NULL,
    [ActionType]   CHAR (1)        NULL,
    [IsDuplicate]  BIT             NULL
);

