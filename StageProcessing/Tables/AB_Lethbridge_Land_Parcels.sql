CREATE TABLE [StageProcessing].[AB_Lethbridge_Land_Parcels] (
    [SourceID]      INT             NULL,
    [Code]          VARCHAR (200)   NULL,
    [Neighbourhood] VARCHAR (200)   NULL,
    [Shape_Length]  VARCHAR (50)    NULL,
    [Shape_Area]    DECIMAL (17, 2) NULL,
    [IsResidential] VARCHAR (250)   NULL,
    [AreaSpec]      VARCHAR (50)    NULL,
    [ZoneDesc]      VARCHAR (400)   NULL,
    [ParcelID]      NVARCHAR (510)  NULL,
    [Roll]          VARCHAR (200)   NULL,
    [PlanNumber]    VARCHAR (100)   NULL,
    [Lot]           VARCHAR (50)    NULL,
    [Address]       VARCHAR (500)   NULL,
    [Zone]          VARCHAR (400)   NULL,
    [ActionType]    CHAR (1)        NULL,
    [IsDuplicate]   BIT             NULL
);

