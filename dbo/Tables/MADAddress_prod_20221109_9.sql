﻿CREATE TABLE [dbo].[MADAddress_prod_20221109_9] (
    [ID]                  INT           NULL,
    [SourceAddressID]     VARCHAR (100) NULL,
    [MADAddressID]        INT           NULL,
    [UnitNumber]          VARCHAR (50)  NULL,
    [StreetNumber]        VARCHAR (50)  NULL,
    [StreetName]          VARCHAR (50)  NULL,
    [StreetType]          VARCHAR (50)  NULL,
    [StreetDirection]     VARCHAR (50)  NULL,
    [FSA]                 CHAR (3)      NULL,
    [PostalCode]          VARCHAR (7)   NULL,
    [City]                VARCHAR (50)  NULL,
    [ProvinceCode]        VARCHAR (10)  NULL,
    [Country]             VARCHAR (50)  NULL,
    [Latitude]            VARCHAR (50)  NULL,
    [Longitude]           VARCHAR (50)  NULL,
    [CityAlternative]     VARCHAR (50)  NULL,
    [CityNameFR]          VARCHAR (50)  NULL,
    [ProvinceNameFR]      VARCHAR (50)  NULL,
    [Filename]            VARCHAR (100) NULL,
    [FullAddress]         VARCHAR (500) NULL,
    [UnitCode]            VARCHAR (50)  NULL,
    [CreatedDate]         DATETIME      NULL,
    [UpdatedDate]         DATETIME      NULL,
    [Status]              VARCHAR (50)  NULL,
    [NewAddressID]        INT           NULL,
    [DateCreatedUTC]      DATETIME      NULL,
    [LastModifiedDateUTC] DATETIME      NULL
);

