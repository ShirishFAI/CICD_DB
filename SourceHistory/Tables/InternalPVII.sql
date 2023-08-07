CREATE TABLE [SourceHistory].[InternalPVII] (
    [Code]             VARCHAR (200)  NULL,
    [LegalDescription] NVARCHAR (MAX) NULL,
    [GVSEligible]      NVARCHAR (510) NULL,
    [LoanAmt]          NVARCHAR (510) NULL,
    [LendingValue]     NVARCHAR (510) NULL,
    [GuaranteeValue]   NVARCHAR (510) NULL,
    [UnitNumber]       NVARCHAR (510) NULL,
    [StreetNumber]     NVARCHAR (510) NULL,
    [StreetName]       NVARCHAR (510) NULL,
    [StreetType]       NVARCHAR (510) NULL,
    [StreetDirection]  NVARCHAR (510) NULL,
    [Province]         NVARCHAR (510) NULL,
    [City]             NVARCHAR (510) NULL,
    [AddressId]        NVARCHAR (510) NULL,
    [Country]          NVARCHAR (510) NULL,
    [PostalCode]       NVARCHAR (510) NULL,
    [HistEndDate]      DATETIME       NULL,
    [IsDuplicate]      BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternalPVII_Code]
    ON [SourceHistory].[InternalPVII]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternalPVII_HistEndDate]
    ON [SourceHistory].[InternalPVII]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

