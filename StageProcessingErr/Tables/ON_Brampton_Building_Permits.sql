CREATE TABLE [StageProcessingErr].[ON_Brampton_Building_Permits] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [X]             NVARCHAR (MAX) NULL,
    [Y]             NVARCHAR (MAX) NULL,
    [ADDRESS]       NVARCHAR (MAX) NULL,
    [FOLDERRSN]     NVARCHAR (MAX) NULL,
    [PERMITNUMBER]  NVARCHAR (MAX) NULL,
    [SUBDESC]       NVARCHAR (MAX) NULL,
    [WORKDESC]      NVARCHAR (MAX) NULL,
    [GFA]           NVARCHAR (MAX) NULL,
    [BEDROOMS]      NVARCHAR (MAX) NULL,
    [STOREYS]       NVARCHAR (MAX) NULL
);

