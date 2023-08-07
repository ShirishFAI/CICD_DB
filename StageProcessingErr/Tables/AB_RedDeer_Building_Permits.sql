CREATE TABLE [StageProcessingErr].[AB_RedDeer_Building_Permits] (
    [SourceID]        INT            NULL,
    [Code]            VARCHAR (200)  NULL,
    [ErrorStatusId]   TINYINT        NULL,
    [PermitNumber]    NVARCHAR (MAX) NULL,
    [Type]            NVARCHAR (MAX) NULL,
    [PlanNumber]      NVARCHAR (MAX) NULL,
    [Address]         NVARCHAR (MAX) NULL,
    [LandUseDistrict] NVARCHAR (MAX) NULL
);

