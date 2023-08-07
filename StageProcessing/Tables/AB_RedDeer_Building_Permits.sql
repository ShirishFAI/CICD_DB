CREATE TABLE [StageProcessing].[AB_RedDeer_Building_Permits] (
    [SourceID]        INT            NULL,
    [Code]            VARCHAR (200)  NULL,
    [PermitNumber]    NVARCHAR (510) NULL,
    [Type]            VARCHAR (100)  NULL,
    [PlanNumber]      VARCHAR (100)  NULL,
    [Address]         VARCHAR (500)  NULL,
    [LandUseDistrict] VARCHAR (100)  NULL,
    [ActionType]      CHAR (1)       NULL,
    [IsDuplicate]     BIT            NULL
);

