CREATE TABLE [dbo].[PIN_Invalid_prod_20221109_BKP] (
    [ID]                   INT           NULL,
    [Code]                 VARCHAR (200) NULL,
    [PIN]                  VARCHAR (50)  NULL,
    [OriginalPIN]          VARCHAR (200) NULL,
    [ProvinceCode]         VARCHAR (50)  NULL,
    [DateCreatedUTC]       DATETIME      NULL,
    [LastModifiedDateUTC]  DATETIME      NULL,
    [Data_Source_ID]       INT           NULL,
    [Data_Source_Priority] INT           NULL,
    [IsPermanentlyInvalid] BIT           NULL,
    [ReProcess]            BIT           NULL,
    [InvalidRuleId]        VARCHAR (20)  NULL
);

