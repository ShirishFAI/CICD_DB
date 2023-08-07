CREATE TABLE [StageProcessing].[ON_Mississauga_2017_Mississauga_Business_Directory] (
    [SourceID]    INT            NULL,
    [Code]        VARCHAR (200)  NULL,
    [BID]         NVARCHAR (510) NULL,
    [Name]        VARCHAR (200)  NULL,
    [StreetNo]    VARCHAR (100)  NULL,
    [StreetName]  VARCHAR (200)  NULL,
    [UnitNo]      VARCHAR (100)  NULL,
    [PostalCode]  VARCHAR (50)   NULL,
    [NAICSCode]   VARCHAR (50)   NULL,
    [NAICSTitle]  VARCHAR (200)  NULL,
    [ActionType]  CHAR (1)       NULL,
    [IsDuplicate] BIT            NULL
);

