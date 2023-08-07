CREATE TABLE [StageProcessingErr].[AB_Lethbridge_Land_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [Neighbourhood] NVARCHAR (MAX) NULL,
    [Shape_Length]  NVARCHAR (MAX) NULL,
    [Shape_Area]    NVARCHAR (MAX) NULL,
    [IsResidential] NVARCHAR (MAX) NULL,
    [AreaSpec]      NVARCHAR (MAX) NULL,
    [ZoneDesc]      NVARCHAR (MAX) NULL,
    [ParcelID]      NVARCHAR (MAX) NULL,
    [Roll]          NVARCHAR (MAX) NULL,
    [PlanNumber]    NVARCHAR (MAX) NULL,
    [Lot]           NVARCHAR (MAX) NULL,
    [Address]       NVARCHAR (MAX) NULL,
    [Zone]          NVARCHAR (MAX) NULL
);

