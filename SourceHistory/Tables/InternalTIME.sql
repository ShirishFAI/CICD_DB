CREATE TABLE [SourceHistory].[InternalTIME] (
    [Code]              VARCHAR (200)  NULL,
    [AddressID]         NVARCHAR (510) NULL,
    [City]              NVARCHAR (510) NULL,
    [EstateTypeCode]    NVARCHAR (510) NULL,
    [OccupancyTypeCode] NVARCHAR (510) NULL,
    [PropertyTypeCode]  NVARCHAR (510) NULL,
    [TransactionType]   NVARCHAR (510) NULL,
    [Zoning]            NVARCHAR (510) NULL,
    [Country]           NVARCHAR (510) NULL,
    [LegalDescription]  NVARCHAR (MAX) NULL,
    [Province]          NVARCHAR (510) NULL,
    [NewHomeEasement]   NVARCHAR (510) NULL,
    [PostalCode]        NVARCHAR (510) NULL,
    [PrimaryProperty]   NVARCHAR (510) NULL,
    [PropertyIDNumber]  NVARCHAR (510) NULL,
    [PurchasePrice]     NVARCHAR (510) NULL,
    [FullAddress]       NVARCHAR (510) NULL,
    [Sequence]          NVARCHAR (510) NULL,
    [POSDate]           NVARCHAR (510) NULL,
    [ClosingDate]       NVARCHAR (510) NULL,
    [StatusID]          NVARCHAR (510) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternalTIME_Code]
    ON [SourceHistory].[InternalTIME]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternalTIME_HistEndDate]
    ON [SourceHistory].[InternalTIME]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

