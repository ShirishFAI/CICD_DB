CREATE TABLE [StageProcessing].[ON_YorkRegion_Parcels] (
    [SourceID]      INT             NULL,
    [Code]          VARCHAR (200)   NULL,
    [PAR_GIS_ID]    NVARCHAR (510)  NULL,
    [LOCATION]      VARCHAR (500)   NULL,
    [MUNNAME]       VARCHAR (200)   NULL,
    [PLANNUM]       VARCHAR (100)   NULL,
    [Shape__Area]   DECIMAL (17, 2) NULL,
    [Shape__Length] VARCHAR (50)    NULL,
    [ActionType]    CHAR (1)        NULL,
    [IsDuplicate]   BIT             NULL
);

