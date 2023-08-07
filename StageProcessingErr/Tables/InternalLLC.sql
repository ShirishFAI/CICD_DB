CREATE TABLE [StageProcessingErr].[InternalLLC] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [ErrorStatusId]    TINYINT        NULL,
    [UnitNumber]       NVARCHAR (510) NULL,
    [StreetNumber]     NVARCHAR (510) NULL,
    [Streetaddress1]   NVARCHAR (510) NULL,
    [StreetAddress2]   NVARCHAR (510) NULL,
    [Municipality]     NVARCHAR (510) NULL,
    [Province]         NVARCHAR (510) NULL,
    [City]             NVARCHAR (510) NULL,
    [LegalDescription] NVARCHAR (MAX) NULL,
    [IsNewHome]        NVARCHAR (510) NULL,
    [EstateType]       NVARCHAR (510) NULL,
    [OccupancyType]    NVARCHAR (510) NULL,
    [PropertyType]     NVARCHAR (510) NULL,
    [RentAssignment]   NVARCHAR (510) NULL,
    [AddressId]        NVARCHAR (510) NULL,
    [AnnualTaxAmount]  NVARCHAR (510) NULL,
    [ARN]              NVARCHAR (510) NULL,
    [Country]          NVARCHAR (510) NULL,
    [PostalCode]       NVARCHAR (510) NULL
);

