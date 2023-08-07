﻿CREATE TABLE [SourceHistory].[InternaliAVM] (
    [Code]                    VARCHAR (200)  NULL,
    [AddressId]               NVARCHAR (510) NULL,
    [City]                    NVARCHAR (510) NULL,
    [Country]                 NVARCHAR (510) NULL,
    [InsuredValue]            NVARCHAR (510) NULL,
    [LegalDescription]        NVARCHAR (MAX) NULL,
    [MpacConfidenceLevel]     NVARCHAR (510) NULL,
    [MpacHighConfidenceLimit] NVARCHAR (510) NULL,
    [MpacLowConfidenceLimt]   NVARCHAR (510) NULL,
    [MpacPropertyType]        NVARCHAR (510) NULL,
    [MpacValue]               NVARCHAR (510) NULL,
    [OccupancyType]           NVARCHAR (510) NULL,
    [OrderDate]               NVARCHAR (510) NULL,
    [PostalCode]              NVARCHAR (510) NULL,
    [PropertyType]            NVARCHAR (510) NULL,
    [Province]                NVARCHAR (510) NULL,
    [SaleType]                NVARCHAR (510) NULL,
    [StreetName]              NVARCHAR (510) NULL,
    [StreetNumber]            NVARCHAR (510) NULL,
    [StreetType]              NVARCHAR (510) NULL,
    [TeranetValue]            NVARCHAR (510) NULL,
    [UnitNumber]              NVARCHAR (510) NULL,
    [ValuePurchasePrice]      NVARCHAR (510) NULL,
    [Zoning]                  NVARCHAR (510) NULL,
    [CompleteDate]            NVARCHAR (510) NULL,
    [HistEndDate]             DATETIME       NULL,
    [IsDuplicate]             BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_InternaliAVM_Code]
    ON [SourceHistory].[InternaliAVM]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_InternaliAVM_HistEndDate]
    ON [SourceHistory].[InternaliAVM]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

