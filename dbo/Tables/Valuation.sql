CREATE TABLE [dbo].[Valuation] (
    [ID]                      INT             IDENTITY (1, 1) NOT NULL,
    [Code]                    VARCHAR (200)   NULL,
    [MasterAddressID]         VARCHAR (100)   NULL,
    [PIN]                     VARCHAR (50)    NULL,
    [ProvinceCode]            VARCHAR (50)    NULL,
    [ARN]                     VARCHAR (200)   NULL,
    [JurCode]                 VARCHAR (10)    NULL,
    [EstimatedValue]          DECIMAL (17, 2) NULL,
    [HighValue]               DECIMAL (17, 2) NULL,
    [LowValue]                DECIMAL (17, 2) NULL,
    [CompleteDate]            DATE            NULL,
    [MPACValue]               DECIMAL (17, 2) NULL,
    [TERANETValue]            DECIMAL (17, 2) NULL,
    [InsuredValue]            DECIMAL (17, 2) NULL,
    [MPACConfidenceLevel]     VARCHAR (50)    NULL,
    [MPACPropertyType]        VARCHAR (50)    NULL,
    [POSDate]                 DATE            NULL,
    [MPACLowConfidenceLimit]  VARCHAR (50)    NULL,
    [MPACHighConfidenceLimit] VARCHAR (50)    NULL,
    [ValuePurchasePrice]      DECIMAL (17, 2) NULL,
    [AppraisedValue]          DECIMAL (17, 2) NULL,
    [DateCreatedUTC]          DATETIME        NULL,
    [LastModifiedDateUTC]     DATETIME        NULL,
    [Data_Source_ID]          INT             NULL,
    [Data_Source_Priority]    INT             NULL,
    [IsDuplicate]             INT             CONSTRAINT [DEFAULT_Valuation_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Valuation_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE UNIQUE CLUSTERED INDEX [IX_Valuation_Code]
    ON [dbo].[Valuation]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Valuation_LastModifiedDateUTC]
    ON [dbo].[Valuation]([LastModifiedDateUTC] ASC) WITH (FILLFACTOR = 80);

