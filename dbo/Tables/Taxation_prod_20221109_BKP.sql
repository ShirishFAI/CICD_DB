CREATE TABLE [dbo].[Taxation_prod_20221109_BKP] (
    [ID]                   INT             NULL,
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
    [Data_Source_Priority] INT             NULL
);

