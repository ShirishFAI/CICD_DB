CREATE TABLE [dbo].[Property] (
    [ID]                  INT           IDENTITY (1, 1) NOT NULL,
    [Code]                VARCHAR (200) NULL,
    [MasterAddressId]     VARCHAR (200) NULL,
    [PIN]                 VARCHAR (50)  NULL,
    [ProvinceCode]        VARCHAR (50)  NULL,
    [ARN]                 VARCHAR (200) NULL,
    [JurCode]             VARCHAR (10)  NULL,
    [DateCreatedUTC]      DATETIME      NULL,
    [LastModifiedDateUTC] DATETIME      NULL,
    [IsDuplicate]         INT           CONSTRAINT [DEFAULT_Property_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Property_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE UNIQUE CLUSTERED INDEX [IX_Property_Code]
    ON [dbo].[Property]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Property_LastModifiedDateUTC]
    ON [dbo].[Property]([LastModifiedDateUTC] ASC) WITH (FILLFACTOR = 80);

