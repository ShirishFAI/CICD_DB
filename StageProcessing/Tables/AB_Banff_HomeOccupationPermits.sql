CREATE TABLE [StageProcessing].[AB_Banff_HomeOccupationPermits] (
    [SourceID]       INT            NULL,
    [Code]           VARCHAR (200)  NULL,
    [Roll_No]        VARCHAR (200)  NULL,
    [Application_No] NVARCHAR (510) NULL,
    [Date_Received]  NVARCHAR (510) NULL,
    [Plan]           VARCHAR (100)  NULL,
    [Unit_No]        VARCHAR (100)  NULL,
    [Street_No]      VARCHAR (100)  NULL,
    [Street_Name]    VARCHAR (200)  NULL,
    [ActionType]     CHAR (1)       NULL,
    [IsDuplicate]    BIT            NULL
);

