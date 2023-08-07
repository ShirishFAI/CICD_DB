CREATE TABLE [StageProcessing].[AB_Calgary_Calgary_Business_Licenses] (
    [SourceID]     INT            NULL,
    [Code]         VARCHAR (200)  NULL,
    [TRADENAME]    NVARCHAR (510) NULL,
    [ADDRESS]      VARCHAR (500)  NULL,
    [LICENCETYPES] NVARCHAR (510) NULL,
    [longitude]    VARCHAR (50)   NULL,
    [latitude]     VARCHAR (50)   NULL,
    [location]     VARCHAR (50)   NULL,
    [ActionType]   CHAR (1)       NULL,
    [IsDuplicate]  BIT            NULL
);

