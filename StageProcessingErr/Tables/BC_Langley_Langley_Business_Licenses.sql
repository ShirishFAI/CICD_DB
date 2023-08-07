CREATE TABLE [StageProcessingErr].[BC_Langley_Langley_Business_Licenses] (
    [SourceID]           INT            NULL,
    [Code]               VARCHAR (200)  NULL,
    [ErrorStatusId]      TINYINT        NULL,
    [X]                  NVARCHAR (MAX) NULL,
    [Y]                  NVARCHAR (MAX) NULL,
    [PropertyNumber]     NVARCHAR (MAX) NULL,
    [Civic_Unit]         NVARCHAR (MAX) NULL,
    [Civic_House]        NVARCHAR (MAX) NULL,
    [Civic_Street]       NVARCHAR (MAX) NULL,
    [CommunityName]      NVARCHAR (MAX) NULL,
    [Category]           NVARCHAR (MAX) NULL,
    [NAICS_Primary]      NVARCHAR (MAX) NULL,
    [NAICS_Primary_Desc] NVARCHAR (MAX) NULL
);

