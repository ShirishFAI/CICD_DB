CREATE TABLE [dbo].[PIN_Invalid] (
    [ID]                   INT           IDENTITY (1, 1) NOT NULL,
    [Code]                 VARCHAR (200) NULL,
    [PIN]                  VARCHAR (50)  NULL,
    [OriginalPIN]          VARCHAR (200) NULL,
    [ProvinceCode]         VARCHAR (50)  NULL,
    [DateCreatedUTC]       DATETIME      NULL,
    [LastModifiedDateUTC]  DATETIME      NULL,
    [Data_Source_ID]       INT           NULL,
    [Data_Source_Priority] INT           NULL,
    [IsPermanentlyInvalid] BIT           DEFAULT ((0)) NULL,
    [ReProcess]            BIT           DEFAULT ((0)) NULL,
    [InvalidRuleId]        VARCHAR (20)  NULL,
    [IsDuplicate]          INT           CONSTRAINT [DEFAULT_PIN_Invalid_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_PIN_Invalid_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE CLUSTERED INDEX [IX_PIN_Invalid_Code]
    ON [dbo].[PIN_Invalid]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_PIN_Invalid_LastModifiedDateUTC]
    ON [dbo].[PIN_Invalid]([LastModifiedDateUTC] DESC) WITH (FILLFACTOR = 80);

