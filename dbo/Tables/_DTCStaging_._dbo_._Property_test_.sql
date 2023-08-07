CREATE TABLE [dbo].[[DTCStaging]].[dbo]].[Property_test]]] (
    [ID]                  INT           NOT NULL,
    [Code]                VARCHAR (200) NOT NULL,
    [MasterAddressId]     VARCHAR (50)  NOT NULL,
    [PIN]                 VARCHAR (50)  NULL,
    [ProvinceCode]        VARCHAR (50)  NOT NULL,
    [ARN]                 VARCHAR (200) NOT NULL,
    [JurCode]             VARCHAR (10)  NOT NULL,
    [DateCreatedUTC]      DATETIME      NOT NULL,
    [LastModifiedDateUTC] DATETIME      NOT NULL
);

