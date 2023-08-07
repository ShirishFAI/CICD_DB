CREATE TABLE [SourceHistory].[InternalMMS] (
    [Code]                      VARCHAR (200)  NULL,
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
    [Country]                   NVARCHAR (510) NULL,
    [HistEndDate]               DATETIME       NULL,
    [IsDuplicate]               BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternalMMS_Code]
    ON [SourceHistory].[InternalMMS]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternalMMS_HistEndDate]
    ON [SourceHistory].[InternalMMS]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

