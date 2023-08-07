CREATE TABLE [dbo].[Sales_Invalid] (
    [ID]                    INT             IDENTITY (1, 1) NOT NULL,
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
    [Data_Source_Priority]  INT             NULL,
    [IsPermanentlyInvalid]  BIT             DEFAULT ((0)) NULL,
    [ReProcess]             BIT             DEFAULT ((0)) NULL,
    [InvalidRuleId]         VARCHAR (20)    NULL,
    [IsDuplicate]           INT             CONSTRAINT [DEFAULT_Sales_Invalid_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Sales_Invalid_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE CLUSTERED INDEX [IX_Sales_Invalid_Code]
    ON [dbo].[Sales_Invalid]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Sales_Invalid_LastModifiedDateUTC]
    ON [dbo].[Sales_Invalid]([LastModifiedDateUTC] DESC) WITH (FILLFACTOR = 80);

