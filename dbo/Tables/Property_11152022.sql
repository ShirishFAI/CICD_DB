CREATE TABLE [dbo].[Property_11152022] (
    [ID]                  INT           IDENTITY (1, 1) NOT NULL,
    [Code]                VARCHAR (200) NULL,
    [MasterAddressId]     VARCHAR (200) NULL,
    [PIN]                 VARCHAR (50)  NULL,
    [ProvinceCode]        VARCHAR (50)  NULL,
    [ARN]                 VARCHAR (200) NULL,
    [JurCode]             VARCHAR (10)  NULL,
    [DateCreatedUTC]      DATETIME      NULL,
    [LastModifiedDateUTC] DATETIME      NULL,
    [IsDuplicate]         INT           NOT NULL
);

