CREATE TABLE [StageProcessing].[AB_Banff_BuildingPermits] (
    [SourceID]                INT            NULL,
    [Code]                    VARCHAR (200)  NULL,
    [Roll_No]                 VARCHAR (200)  NULL,
    [Building_Permit_No]      NVARCHAR (510) NULL,
    [Date_Received]           NVARCHAR (510) NULL,
    [Plan]                    VARCHAR (100)  NULL,
    [Unit_No]                 VARCHAR (100)  NULL,
    [Street_No]               VARCHAR (100)  NULL,
    [Street_Name]             VARCHAR (200)  NULL,
    [Description]             VARCHAR (255)  NULL,
    [No_Of_New_Dweling_Units] INT            NULL,
    [ActionType]              CHAR (1)       NULL,
    [IsDuplicate]             BIT            NULL
);

