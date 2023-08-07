CREATE TABLE [SourceHistory].[ON_Mississauga_2017_Residential_Directory_Townhouse_Complexes] (
    [Code]          VARCHAR (200)  NULL,
    [COMPLEX_ID]    NVARCHAR (MAX) NULL,
    [Str_Number]    NVARCHAR (MAX) NULL,
    [StreetName]    NVARCHAR (MAX) NULL,
    [Zoning]        NVARCHAR (MAX) NULL,
    [Area_Ha]       NVARCHAR (MAX) NULL,
    [Area_Acre]     NVARCHAR (MAX) NULL,
    [Parking]       NVARCHAR (MAX) NULL,
    [GFA_RES_m2]    NVARCHAR (MAX) NULL,
    [GFARes_ft2]    NVARCHAR (MAX) NULL,
    [BLDG_Type]     NVARCHAR (MAX) NULL,
    [Tenure_RES]    NVARCHAR (MAX) NULL,
    [Storeys]       NVARCHAR (MAX) NULL,
    [Shape__Area]   NVARCHAR (MAX) NULL,
    [Shape__Length] NVARCHAR (MAX) NULL,
    [HistEndDate]   DATETIME       NULL,
    [IsDuplicate]   BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_ON_Mississauga_2017_Residential_Directory_Townhouse_Complexes_Code]
    ON [SourceHistory].[ON_Mississauga_2017_Residential_Directory_Townhouse_Complexes]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_ON_Mississauga_2017_Residential_Directory_Townhouse_Complexes_HistEndDate]
    ON [SourceHistory].[ON_Mississauga_2017_Residential_Directory_Townhouse_Complexes]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

