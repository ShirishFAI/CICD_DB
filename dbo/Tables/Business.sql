CREATE TABLE [dbo].[Business] (
    [ID]                   INT           IDENTITY (1, 2) NOT NULL,
    [Code]                 VARCHAR (200) NULL,
    [MasterAddressID]      VARCHAR (100) NULL,
    [BusinessCategory]     VARCHAR (200) NULL,
    [BusinessCode]         VARCHAR (50)  NULL,
    [BusinessDescription]  VARCHAR (MAX) NULL,
    [BusinessType]         VARCHAR (200) NULL,
    [NaicsCode]            VARCHAR (200) NULL,
    [NaicsDescription]     VARCHAR (500) NULL,
    [ProvinceCode]         VARCHAR (50)  NULL,
    [Company]              VARCHAR (200) NULL,
    [DateCreatedUTC]       DATETIME      NULL,
    [LastModifiedDateUTC]  DATETIME      NULL,
    [Data_Source_ID]       INT           NULL,
    [Data_Source_Priority] INT           NULL,
    [IsDuplicate]          INT           CONSTRAINT [DEFAULT_Business_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Business_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE UNIQUE CLUSTERED INDEX [IX_Business_Code]
    ON [dbo].[Business]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Business_LastModifiedDateUTC]
    ON [dbo].[Business]([LastModifiedDateUTC] ASC) WITH (FILLFACTOR = 80);

