CREATE TABLE [StageProcessing].[InternalMMS] (
    [SourceID]                  INT            NULL,
    [Code]                      VARCHAR (200)  NULL,
    [AddressID]                 NVARCHAR (510) NULL,
    [City]                      VARCHAR (200)  NULL,
    [EstateType]                VARCHAR (50)   NULL,
    [IsNewHome]                 VARCHAR (5)    NULL,
    [LegalDescription]          VARCHAR (4000) NULL,
    [MobileMiniHomeRequirement] VARCHAR (5)    NULL,
    [Municipality]              VARCHAR (100)  NULL,
    [OccupancyType]             VARCHAR (50)   NULL,
    [PINNumber]                 VARCHAR (50)   NULL,
    [PostalCode]                VARCHAR (50)   NULL,
    [PropertyType]              VARCHAR (200)  NULL,
    [Province]                  VARCHAR (50)   NULL,
    [StreetAddress1]            VARCHAR (500)  NULL,
    [Country]                   VARCHAR (30)   NULL,
    [ActionType]                CHAR (1)       NULL,
    [IsDuplicate]               BIT            NULL
);

