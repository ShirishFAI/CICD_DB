CREATE TABLE [Stage].[DataFactoryLoad] (
    [FileName]              VARCHAR (200) NULL,
    [SourceName]            VARCHAR (100) NULL,
    [SourceDesc]            VARCHAR (250) NULL,
    [SubSourceName]         VARCHAR (100) NULL,
    [SubSourceDesc]         VARCHAR (250) NULL,
    [SubSourceType]         VARCHAR (50)  NULL,
    [SourceInstanceDetails] VARCHAR (200) NULL,
    [AddressLine]           VARCHAR (500) NULL,
    [PostalCode]            VARCHAR (20)  NULL,
    [City]                  VARCHAR (100) NULL,
    [Province]              VARCHAR (50)  NULL,
    [SourceAddressId]       VARCHAR (100) NULL,
    [Latitude]              VARCHAR (50)  NULL,
    [Longitude]             VARCHAR (50)  NULL,
    [AddressStatus]         VARCHAR (100) NULL,
    [MasterAddressId]       BIGINT        NULL,
    [InsertTimeStamp]       DATETIME      NULL
);

