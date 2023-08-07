CREATE TABLE [dbo].[Sales_prod_20221109_backup] (
    [ID]                    INT             NULL,
    [Code]                  VARCHAR (200)   NULL,
    [MasterAddressID]       VARCHAR (100)   NULL,
    [LastSaleDate]          DATE            NULL,
    [SaleType]              VARCHAR (100)   NULL,
    [PurchasePrice]         DECIMAL (17, 2) NULL,
    [OriginalPurchasePrice] DECIMAL (17, 2) NULL,
    [BuyerName]             VARCHAR (200)   NULL,
    [PriceSold]             DECIMAL (17, 2) NULL,
    [LastSaleAmount]        DECIMAL (17, 2) NULL,
    [LastSaleYear]          DECIMAL (17, 2) NULL,
    [ClosingDate]           DATETIME        NULL,
    [POSDateSales]          DATETIME        NULL,
    [StatusID]              INT             NULL,
    [DateCreatedUTC]        DATETIME        NULL,
    [LastModifiedDateUTC]   DATETIME        NULL,
    [Data_Source_ID]        INT             NULL,
    [Data_Source_Priority]  INT             NULL
);

