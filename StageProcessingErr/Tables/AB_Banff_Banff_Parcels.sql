CREATE TABLE [StageProcessingErr].[AB_Banff_Banff_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [ROLL]          NVARCHAR (MAX) NULL,
    [GIS_ID]        NVARCHAR (MAX) NULL,
    [Full_Addre]    NVARCHAR (MAX) NULL,
    [Acres]         NVARCHAR (MAX) NULL,
    [SqFt]          NVARCHAR (MAX) NULL,
    [Shape_area]    NVARCHAR (MAX) NULL,
    [Shape_len]     NVARCHAR (MAX) NULL
);

