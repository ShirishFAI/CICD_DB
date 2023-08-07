CREATE TABLE [StageLanding].[AB_RedDeer_Building_Permits] (
    [PermitNumber]    NVARCHAR (MAX) NULL,
    [ReportCode]      NVARCHAR (MAX) NULL,
    [PermitDate]      NVARCHAR (MAX) NULL,
    [Type]            NVARCHAR (MAX) NULL,
    [SubType]         NVARCHAR (MAX) NULL,
    [Description]     NVARCHAR (MAX) NULL,
    [Status]          NVARCHAR (MAX) NULL,
    [Contractor]      NVARCHAR (MAX) NULL,
    [Block]           NVARCHAR (MAX) NULL,
    [PlanNumber]      NVARCHAR (MAX) NULL,
    [Lot]             NVARCHAR (MAX) NULL,
    [Address]         NVARCHAR (MAX) NULL,
    [LandUseDistrict] NVARCHAR (MAX) NULL,
    [Fee]             NVARCHAR (MAX) NULL,
    [Value]           NVARCHAR (MAX) NULL,
    [SourceID]        INT            IDENTITY (1, 1) NOT NULL,
    [Code]            VARCHAR (200)  NULL
);

