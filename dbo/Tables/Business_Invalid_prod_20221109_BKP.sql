CREATE TABLE [dbo].[Business_Invalid_prod_20221109_BKP] (
    [ID]                   INT           NULL,
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
    [IsPermanentlyInvalid] BIT           NULL,
    [ReProcess]            BIT           NULL,
    [InvalidRuleId]        VARCHAR (20)  NULL
);

