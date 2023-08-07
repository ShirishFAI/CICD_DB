CREATE TABLE [SourceHistory].[InternalCE] (
    [Code]                    VARCHAR (200)  NULL,
    [UnitNumber]              NVARCHAR (510) NULL,
    [StreetNumber]            NVARCHAR (510) NULL,
    [StreetName]              NVARCHAR (510) NULL,
    [StreetType]              NVARCHAR (510) NULL,
    [Province]                NVARCHAR (510) NULL,
    [City]                    NVARCHAR (510) NULL,
    [LegalDescription]        NVARCHAR (MAX) NULL,
    [EstateType]              NVARCHAR (510) NULL,
    [Zoning]                  NVARCHAR (510) NULL,
    [PropertyTypeDescription] NVARCHAR (510) NULL,
    [AddressId]               NVARCHAR (510) NULL,
    [PropertyIDNumber]        NVARCHAR (510) NULL,
    [Country]                 NVARCHAR (510) NULL,
    [PostalCode]              NVARCHAR (510) NULL,
    [HistEndDate]             DATETIME       NULL,
    [IsDuplicate]             BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternalCE_Code]
    ON [SourceHistory].[InternalCE]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternalCE_HistEndDate]
    ON [SourceHistory].[InternalCE]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

