CREATE TABLE [StageProcessing].[ON_Mississauga_2018_Office_Directory_Buildings] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [BLDG_ID]     NVARCHAR (510)  NULL,
    [Address]     VARCHAR (500)   NULL,
    [OFF_GFA_m2]  DECIMAL (17, 2) NULL,
    [OFF_GFAft2]  DECIMAL (17, 2) NULL,
    [Storeys]     VARCHAR (100)   NULL,
    [Year_Built]  INT             NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

