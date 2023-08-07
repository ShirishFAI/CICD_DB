CREATE TABLE [SourceHistory].[AB_RedDeer_Building_Permits] (
    [Code]            VARCHAR (200)  NULL,
    [PermitNumber]    NVARCHAR (MAX) NULL,
    [Type]            NVARCHAR (MAX) NULL,
    [PlanNumber]      NVARCHAR (MAX) NULL,
    [Address]         NVARCHAR (MAX) NULL,
    [LandUseDistrict] NVARCHAR (MAX) NULL,
    [HistEndDate]     DATETIME       NULL,
    [IsDuplicate]     BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_RedDeer_Building_Permits_Code]
    ON [SourceHistory].[AB_RedDeer_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_RedDeer_Building_Permits_HistEndDate]
    ON [SourceHistory].[AB_RedDeer_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

