CREATE TABLE [StageProcessing].[ON_Mississauga_2017_Residential_Directory_Townhouse_Complexes] (
    [SourceID]      INT             NULL,
    [Code]          VARCHAR (200)   NULL,
    [COMPLEX_ID]    NVARCHAR (510)  NULL,
    [Str_Number]    VARCHAR (100)   NULL,
    [StreetName]    VARCHAR (200)   NULL,
    [Zoning]        VARCHAR (400)   NULL,
    [Area_Ha]       DECIMAL (17, 2) NULL,
    [Area_Acre]     VARCHAR (50)    NULL,
    [Parking]       DECIMAL (17, 2) NULL,
    [GFA_RES_m2]    DECIMAL (17, 2) NULL,
    [GFARes_ft2]    DECIMAL (17, 2) NULL,
    [BLDG_Type]     VARCHAR (255)   NULL,
    [Tenure_RES]    VARCHAR (50)    NULL,
    [Storeys]       VARCHAR (100)   NULL,
    [Shape__Area]   DECIMAL (17, 2) NULL,
    [Shape__Length] VARCHAR (50)    NULL,
    [ActionType]    CHAR (1)        NULL,
    [IsDuplicate]   BIT             NULL
);

