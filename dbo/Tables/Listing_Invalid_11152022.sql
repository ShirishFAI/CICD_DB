﻿CREATE TABLE [dbo].[Listing_Invalid_11152022] (
    [ID]                   INT             IDENTITY (1, 1) NOT NULL,
    [Code]                 VARCHAR (200)   NULL,
    [MasterAddressID]      VARCHAR (100)   NULL,
    [PIN]                  VARCHAR (50)    NULL,
    [ProvinceCode]         VARCHAR (50)    NULL,
    [ARN]                  VARCHAR (200)   NULL,
    [JurCode]              VARCHAR (10)    NULL,
    [MLSNumber]            VARCHAR (50)    NULL,
    [SellerName]           VARCHAR (100)   NULL,
    [DateEnd]              DATE            NULL,
    [DateStart]            DATE            NULL,
    [DateUpdate]           DATE            NULL,
    [ListDays]             INT             NULL,
    [ListType]             VARCHAR (50)    NULL,
    [ListStatus]           VARCHAR (50)    NULL,
    [ListHistory]          VARCHAR (500)   NULL,
    [PriceAsked]           DECIMAL (17, 2) NULL,
    [FCTTransactionType]   VARCHAR (200)   NULL,
    [LoanAmt]              DECIMAL (17, 2) NULL,
    [LendingValue]         DECIMAL (17, 2) NULL,
    [GuaranteeValue]       DECIMAL (17, 2) NULL,
    [OwnershipType]        VARCHAR (100)   NULL,
    [RentAssignment]       VARCHAR (200)   NULL,
    [DateCreatedUTC]       DATETIME        NULL,
    [LastModifiedDateUTC]  DATETIME        NULL,
    [Data_Source_ID]       INT             NULL,
    [Data_Source_Priority] INT             NULL,
    [IsPermanentlyInvalid] BIT             NULL,
    [ReProcess]            BIT             NULL,
    [InvalidRuleId]        VARCHAR (20)    NULL,
    [IsDuplicate]          INT             NOT NULL
);

