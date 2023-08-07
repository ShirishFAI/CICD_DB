CREATE TABLE [SourceHistory].[InternalCUC] (
    [Code]                VARCHAR (200)  NULL,
    [LegalDescription]    NVARCHAR (MAX) NULL,
    [Province]            NVARCHAR (510) NULL,
    [IsNativeLand]        NVARCHAR (510) NULL,
    [IsEnergy]            NVARCHAR (510) NULL,
    [IsVacantLand]        NVARCHAR (510) NULL,
    [IsMunicipalAddress]  NVARCHAR (510) NULL,
    [PropertyType]        NVARCHAR (510) NULL,
    [TransactionType]     NVARCHAR (510) NULL,
    [PurchasePrice]       NVARCHAR (510) NULL,
    [UnitNumber]          NVARCHAR (510) NULL,
    [StreetNumber]        NVARCHAR (510) NULL,
    [StreetAddress1]      NVARCHAR (510) NULL,
    [StreetAddress2]      NVARCHAR (510) NULL,
    [Municipality]        NVARCHAR (510) NULL,
    [City]                NVARCHAR (510) NULL,
    [AddressID]           NVARCHAR (510) NULL,
    [PIN]                 NVARCHAR (510) NULL,
    [Country]             NVARCHAR (510) NULL,
    [PostalCode]          NVARCHAR (510) NULL,
    [OrganizationName]    NVARCHAR (510) NULL,
    [CustomerClosingDate] NVARCHAR (510) NULL,
    [HistEndDate]         DATETIME       NULL,
    [IsDuplicate]         BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternalCUC_Code]
    ON [SourceHistory].[InternalCUC]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternalCUC_HistEndDate]
    ON [SourceHistory].[InternalCUC]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

