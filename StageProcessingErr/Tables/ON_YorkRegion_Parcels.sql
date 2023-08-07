CREATE TABLE [StageProcessingErr].[ON_YorkRegion_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [PAR_GIS_ID]    NVARCHAR (MAX) NULL,
    [LOCATION]      NVARCHAR (MAX) NULL,
    [MUNNAME]       NVARCHAR (MAX) NULL,
    [PLANNUM]       NVARCHAR (MAX) NULL,
    [Shape__Area]   NVARCHAR (MAX) NULL,
    [Shape__Length] NVARCHAR (MAX) NULL
);

