CREATE TABLE [dbo].[PIN] (
    [ID]                   INT           IDENTITY (1, 1) NOT NULL,
    [Code]                 VARCHAR (200) NULL,
    [PIN]                  VARCHAR (50)  NULL,
    [OriginalPIN]          VARCHAR (200) NULL,
    [ProvinceCode]         VARCHAR (50)  NULL,
    [DateCreatedUTC]       DATETIME      NULL,
    [LastModifiedDateUTC]  DATETIME      NULL,
    [Data_Source_ID]       INT           NULL,
    [Data_Source_Priority] INT           NULL,
    [IsDuplicate]          INT           CONSTRAINT [DEFAULT_PIN_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_PIN_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE UNIQUE CLUSTERED INDEX [IX_PIN_Code]
    ON [dbo].[PIN]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_PIN_LastModifiedDateUTC]
    ON [dbo].[PIN]([LastModifiedDateUTC] ASC) WITH (FILLFACTOR = 80);

