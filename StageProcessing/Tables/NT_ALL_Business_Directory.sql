CREATE TABLE [StageProcessing].[NT_ALL_Business_Directory] (
    [SourceID]     INT            NULL,
    [Code]         VARCHAR (200)  NULL,
    [BUSINESSNAME] NVARCHAR (510) NULL,
    [BUSINESSTYPE] VARCHAR (200)  NULL,
    [ADDRESS1]     VARCHAR (500)  NULL,
    [CITY]         VARCHAR (200)  NULL,
    [PROVINCE]     VARCHAR (50)   NULL,
    [POSTALCODE]   VARCHAR (50)   NULL,
    [ActionType]   CHAR (1)       NULL,
    [IsDuplicate]  BIT            NULL
);

