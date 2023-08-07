CREATE TABLE [dbo].[Address] (
    [ID]                                           INT           IDENTITY (1, 1) NOT NULL,
    [Code]                                         VARCHAR (200) NULL,
    [MasterAddressID]                              VARCHAR (100) NULL,
    [UnitNumber]                                   VARCHAR (100) NULL,
    [StreetNumber]                                 VARCHAR (100) NULL,
    [StreetName]                                   VARCHAR (200) NULL,
    [StreetType]                                   VARCHAR (200) NULL,
    [StreetDirection]                              VARCHAR (20)  NULL,
    [City]                                         VARCHAR (200) NULL,
    [PostalCode]                                   VARCHAR (50)  NULL,
    [ProvinceCode]                                 VARCHAR (50)  NULL,
    [FSA]                                          VARCHAR (3)   NULL,
    [District]                                     VARCHAR (100) NULL,
    [JurCode]                                      VARCHAR (10)  NULL,
    [Country]                                      VARCHAR (30)  NULL,
    [FullAddress]                                  VARCHAR (500) NULL,
    [Latitude]                                     VARCHAR (50)  NULL,
    [Longitude]                                    VARCHAR (50)  NULL,
    [LatitudeLongitude]                            VARCHAR (50)  NULL,
    [Neighbourhood]                                VARCHAR (100) NULL,
    [NeighbourhoodDescription]                     VARCHAR (200) NULL,
    [Municipality]                                 VARCHAR (100) NULL,
    [Region]                                       VARCHAR (50)  NULL,
    [Township]                                     VARCHAR (10)  NULL,
    [Range]                                        VARCHAR (10)  NULL,
    [LandDistrict]                                 VARCHAR (10)  NULL,
    [LandDistrictName]                             VARCHAR (100) NULL,
    [AreaDescription]                              VARCHAR (100) NULL,
    [JurDescription]                               VARCHAR (100) NULL,
    [SchoolDistrictDescription]                    VARCHAR (50)  NULL,
    [CrossStreet]                                  VARCHAR (100) NULL,
    [Community]                                    VARCHAR (100) NULL,
    [IsMunicipalAddress]                           VARCHAR (5)   NULL,
    [DateCreatedUTC]                               DATETIME      NULL,
    [LastModifiedDateUTC]                          DATETIME      NULL,
    [IsMADSent]                                    BIT           NULL,
    [MADSentDateUTC]                               DATETIME      NULL,
    [IsMADReceived]                                BIT           NULL,
    [MADReceivedDateUTC]                           DATETIME      NULL,
    [Data_Source_ID]                               INT           NULL,
    [Data_Source_Priority]                         INT           NULL,
    [RegionalHospitalDistrict_DistrictDescription] VARCHAR (100) NULL,
    [SchoolDistrict]                               VARCHAR (100) NULL,
    [IsDuplicate]                                  INT           CONSTRAINT [DEFAULT_Address_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Address_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE UNIQUE CLUSTERED INDEX [IX_Address_Code]
    ON [dbo].[Address]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Address_LastModifiedDateUTC]
    ON [dbo].[Address]([LastModifiedDateUTC] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Address_MADColumns]
    ON [dbo].[Address]([IsMADSent] ASC, [MADSentDateUTC] ASC, [IsMADReceived] ASC, [MADReceivedDateUTC] ASC) WITH (FILLFACTOR = 80);

