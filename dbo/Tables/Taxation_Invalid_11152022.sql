CREATE TABLE [dbo].[Taxation_Invalid_11152022] (
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
    [IsPermanentlyInvalid] BIT             NULL,
    [ReProcess]            BIT             NULL,
    [InvalidRuleId]        VARCHAR (20)    NULL,
    [IsDuplicate]          INT             NOT NULL
);

