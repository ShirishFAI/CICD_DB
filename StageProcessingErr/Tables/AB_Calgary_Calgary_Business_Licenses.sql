CREATE TABLE [StageProcessingErr].[AB_Calgary_Calgary_Business_Licenses] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [TRADENAME]     NVARCHAR (MAX) NULL,
    [ADDRESS]       NVARCHAR (MAX) NULL,
    [LICENCETYPES]  NVARCHAR (MAX) NULL,
    [longitude]     NVARCHAR (MAX) NULL,
    [latitude]      NVARCHAR (MAX) NULL,
    [location]      NVARCHAR (MAX) NULL
);

