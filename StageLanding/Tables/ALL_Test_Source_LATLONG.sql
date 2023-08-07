CREATE TABLE [StageLanding].[ALL_Test_Source_LATLONG] (
    [StreetNumber] NVARCHAR (MAX) NULL,
    [StreetName]   NVARCHAR (MAX) NULL,
    [StreetType]   NVARCHAR (MAX) NULL,
    [PostalCode]   NVARCHAR (MAX) NULL,
    [City]         NVARCHAR (MAX) NULL,
    [ProvinceCode] NVARCHAR (MAX) NULL,
    [SourceID]     INT            IDENTITY (1, 1) NOT NULL,
    [Code]         VARCHAR (200)  NULL
);

