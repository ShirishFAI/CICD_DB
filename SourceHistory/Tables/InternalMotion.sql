CREATE TABLE [SourceHistory].[InternalMotion] (
    [Code]              VARCHAR (200)  NULL,
    [UnitNumber]        NVARCHAR (510) NULL,
    [StreetNumber]      NVARCHAR (510) NULL,
    [StreetAddress1]    NVARCHAR (510) NULL,
    [StreetAddress2]    NVARCHAR (510) NULL,
    [Province]          NVARCHAR (510) NULL,
    [City]              NVARCHAR (510) NULL,
    [District]          NVARCHAR (510) NULL,
    [LegalDescription]  NVARCHAR (MAX) NULL,
    [IsRenovatedLotNum] NVARCHAR (510) NULL,
    [MetesAndBounds]    NVARCHAR (510) NULL,
    [EstateType]        NVARCHAR (510) NULL,
    [OccupancyType]     NVARCHAR (510) NULL,
    [Zoning]            NVARCHAR (510) NULL,
    [PropertyType]      NVARCHAR (510) NULL,
    [AppraisedValue]    NVARCHAR (510) NULL,
    [AddressId]         NVARCHAR (510) NULL,
    [PropertyIdNumber]  NVARCHAR (510) NULL,
    [Country]           NVARCHAR (510) NULL,
    [PostalCode]        NVARCHAR (510) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternalMotion_Code]
    ON [SourceHistory].[InternalMotion]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternalMotion_HistEndDate]
    ON [SourceHistory].[InternalMotion]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

