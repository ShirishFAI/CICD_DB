CREATE TABLE [StageLanding].[ON_Mississauga_2018_Office_Directory_Buildings] (
    [FID]        NVARCHAR (MAX) NULL,
    [BLDG_ID]    NVARCHAR (MAX) NULL,
    [BLDG_Image] NVARCHAR (MAX) NULL,
    [BLDG_Name]  NVARCHAR (MAX) NULL,
    [Address]    NVARCHAR (MAX) NULL,
    [OfficeType] NVARCHAR (MAX) NULL,
    [OFF_GFA_m2] NVARCHAR (MAX) NULL,
    [OFF_GFAft2] NVARCHAR (MAX) NULL,
    [Storeys]    NVARCHAR (MAX) NULL,
    [Year_Built] NVARCHAR (MAX) NULL,
    [SourceID]   INT            IDENTITY (1, 1) NOT NULL,
    [Code]       VARCHAR (200)  NULL
);

