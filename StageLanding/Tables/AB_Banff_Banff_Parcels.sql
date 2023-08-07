CREATE TABLE [StageLanding].[AB_Banff_Banff_Parcels] (
    [FID]        NVARCHAR (MAX) NULL,
    [ROLL]       NVARCHAR (MAX) NULL,
    [GIS_ID]     NVARCHAR (MAX) NULL,
    [GIS_ID_SER] NVARCHAR (MAX) NULL,
    [Full_Addre] NVARCHAR (MAX) NULL,
    [Perimeter]  NVARCHAR (MAX) NULL,
    [Area]       NVARCHAR (MAX) NULL,
    [Acres]      NVARCHAR (MAX) NULL,
    [Hectares]   NVARCHAR (MAX) NULL,
    [SqFt]       NVARCHAR (MAX) NULL,
    [Shape_area] NVARCHAR (MAX) NULL,
    [Shape_len]  NVARCHAR (MAX) NULL,
    [SourceID]   INT            IDENTITY (1, 1) NOT NULL,
    [Code]       VARCHAR (200)  NULL
);

