CREATE TABLE [dbo].[Taxation] (
    [ID]                   INT             IDENTITY (1, 1) NOT NULL,
    [Code]                 VARCHAR (200)   NULL,
    [ARN]                  VARCHAR (200)   NULL,
    [JurCode]              VARCHAR (10)    NULL,
    [AssessmentYear]       INT             NULL,
    [AssessmentValue]      DECIMAL (17, 2) NULL,
    [AnnualTaxAmount]      DECIMAL (17, 2) NULL,
    [TaxYear]              INT             NULL,
    [TaxAssessedValue]     DECIMAL (17, 2) NULL,
    [NetTax]               DECIMAL (17, 2) NULL,
    [GrossTax]             DECIMAL (17, 2) NULL,
    [AssessmentClass]      VARCHAR (200)   NULL,
    [ProvinceCode]         VARCHAR (50)    NULL,
    [DateCreatedUTC]       DATETIME        NULL,
    [LastModifiedDateUTC]  DATETIME        NULL,
    [Data_Source_ID]       INT             NULL,
    [Data_Source_Priority] INT             NULL,
    [IsDuplicate]          INT             CONSTRAINT [DEFAULT_Taxation_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Taxation_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE UNIQUE CLUSTERED INDEX [IX_Taxation_Code]
    ON [dbo].[Taxation]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Taxation_LastModifiedDateUTC]
    ON [dbo].[Taxation]([LastModifiedDateUTC] ASC) WITH (FILLFACTOR = 80);

