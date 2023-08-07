CREATE TABLE [StageProcessingErr].[InternalMMS] (
    [SourceID]                  INT            NULL,
    [Code]                      VARCHAR (200)  NULL,
    [ErrorStatusId]             TINYINT        NULL,
    [AddressID]                 NVARCHAR (510) NULL,
    [City]                      NVARCHAR (510) NULL,
    [EstateType]                NVARCHAR (510) NULL,
    [IsNewHome]                 NVARCHAR (510) NULL,
    [LegalDescription]          NVARCHAR (MAX) NULL,
    [MobileMiniHomeRequirement] NVARCHAR (510) NULL,
    [Municipality]              NVARCHAR (510) NULL,
    [OccupancyType]             NVARCHAR (510) NULL,
    [PINNumber]                 NVARCHAR (510) NULL,
    [PostalCode]                NVARCHAR (510) NULL,
    [PropertyType]              NVARCHAR (510) NULL,
    [Province]                  NVARCHAR (510) NULL,
    [StreetAddress1]            NVARCHAR (510) NULL,
    [Country]                   NVARCHAR (510) NULL
);

