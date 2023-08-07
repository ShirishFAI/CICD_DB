CREATE TABLE [StageProcessing].[InternalCE] (
    [SourceID]                INT            NULL,
    [Code]                    VARCHAR (200)  NULL,
    [UnitNumber]              VARCHAR (100)  NULL,
    [StreetNumber]            VARCHAR (100)  NULL,
    [StreetName]              VARCHAR (200)  NULL,
    [StreetType]              VARCHAR (200)  NULL,
    [Province]                VARCHAR (50)   NULL,
    [City]                    VARCHAR (200)  NULL,
    [LegalDescription]        VARCHAR (4000) NULL,
    [EstateType]              VARCHAR (50)   NULL,
    [Zoning]                  VARCHAR (400)  NULL,
    [PropertyTypeDescription] VARCHAR (200)  NULL,
    [AddressId]               NVARCHAR (510) NULL,
    [PropertyIDNumber]        VARCHAR (50)   NULL,
    [Country]                 VARCHAR (30)   NULL,
    [PostalCode]              VARCHAR (50)   NULL,
    [ActionType]              CHAR (1)       NULL,
    [IsDuplicate]             BIT            NULL
);

