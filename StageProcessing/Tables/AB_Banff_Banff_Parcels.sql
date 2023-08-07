CREATE TABLE [StageProcessing].[AB_Banff_Banff_Parcels] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [ROLL]        VARCHAR (200)   NULL,
    [GIS_ID]      NVARCHAR (510)  NULL,
    [Full_Addre]  VARCHAR (500)   NULL,
    [Acres]       VARCHAR (50)    NULL,
    [SqFt]        DECIMAL (17, 2) NULL,
    [Shape_area]  DECIMAL (17, 2) NULL,
    [Shape_len]   VARCHAR (50)    NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

