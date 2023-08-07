CREATE TABLE [StageProcessing].[BC_Langley_Langley_Business_Licenses] (
    [SourceID]           INT            NULL,
    [Code]               VARCHAR (200)  NULL,
    [X]                  VARCHAR (50)   NULL,
    [Y]                  VARCHAR (50)   NULL,
    [PropertyNumber]     NVARCHAR (510) NULL,
    [Civic_Unit]         VARCHAR (100)  NULL,
    [Civic_House]        VARCHAR (100)  NULL,
    [Civic_Street]       VARCHAR (200)  NULL,
    [CommunityName]      VARCHAR (100)  NULL,
    [Category]           NVARCHAR (510) NULL,
    [NAICS_Primary]      VARCHAR (200)  NULL,
    [NAICS_Primary_Desc] VARCHAR (500)  NULL,
    [ActionType]         CHAR (1)       NULL,
    [IsDuplicate]        BIT            NULL
);

