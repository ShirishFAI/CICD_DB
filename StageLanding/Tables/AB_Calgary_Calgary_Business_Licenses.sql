CREATE TABLE [StageLanding].[AB_Calgary_Calgary_Business_Licenses] (
    [TRADENAME]     NVARCHAR (MAX) NULL,
    [ADDRESS]       NVARCHAR (MAX) NULL,
    [LICENCETYPES]  NVARCHAR (MAX) NULL,
    [COMDISTNM]     NVARCHAR (MAX) NULL,
    [JOBSTATUSDESC] NVARCHAR (MAX) NULL,
    [JOBCREATED]    NVARCHAR (MAX) NULL,
    [longitude]     NVARCHAR (MAX) NULL,
    [latitude]      NVARCHAR (MAX) NULL,
    [location]      NVARCHAR (MAX) NULL,
    [Count]         NVARCHAR (MAX) NULL,
    [SourceID]      INT            IDENTITY (1, 1) NOT NULL,
    [Code]          VARCHAR (200)  NULL
);

