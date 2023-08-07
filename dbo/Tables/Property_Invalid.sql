CREATE TABLE [dbo].[Property_Invalid] (
    [ID]                   INT           IDENTITY (1, 1) NOT NULL,
    [Code]                 VARCHAR (200) NULL,
    [MasterAddressId]      VARCHAR (200) NULL,
    [PIN]                  VARCHAR (50)  NULL,
    [ProvinceCode]         VARCHAR (50)  NULL,
    [ARN]                  VARCHAR (200) NULL,
    [JurCode]              VARCHAR (10)  NULL,
    [DateCreatedUTC]       DATETIME      NULL,
    [LastModifiedDateUTC]  DATETIME      NULL,
    [IsPermanentlyInvalid] BIT           DEFAULT ((0)) NULL,
    [ReProcess]            BIT           DEFAULT ((0)) NULL,
    [InvalidRuleId]        VARCHAR (20)  NULL,
    [IsDuplicate]          INT           CONSTRAINT [DEFAULT_Property_Invalid_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Property_Invalid_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE CLUSTERED INDEX [IX_Property_Invalid_Code]
    ON [dbo].[Property_Invalid]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Property_Invalid_LastModifiedDateUTC]
    ON [dbo].[Property_Invalid]([LastModifiedDateUTC] DESC) WITH (FILLFACTOR = 80);

