CREATE TABLE [StageLanding].[AB_Lethbridge_Land_Parcels] (
    [OBJECTID]      NVARCHAR (MAX) NULL,
    [Neighbourhood] NVARCHAR (MAX) NULL,
    [CityArea]      NVARCHAR (MAX) NULL,
    [Shape_Length]  NVARCHAR (MAX) NULL,
    [Shape_Area]    NVARCHAR (MAX) NULL,
    [IsResidential] NVARCHAR (MAX) NULL,
    [PerimSpec]     NVARCHAR (MAX) NULL,
    [AreaSpec]      NVARCHAR (MAX) NULL,
    [ZoneDesc]      NVARCHAR (MAX) NULL,
    [ParcelID]      NVARCHAR (MAX) NULL,
    [Roll]          NVARCHAR (MAX) NULL,
    [PlanNumber]    NVARCHAR (MAX) NULL,
    [Block]         NVARCHAR (MAX) NULL,
    [Lot]           NVARCHAR (MAX) NULL,
    [Address]       NVARCHAR (MAX) NULL,
    [Zone]          NVARCHAR (MAX) NULL,
    [LandDev]       NVARCHAR (MAX) NULL,
    [SourceID]      INT            IDENTITY (1, 1) NOT NULL,
    [Code]          VARCHAR (200)  NULL
);

