﻿CREATE TABLE [dbo].[Permit_Invalid_prod_20221109_BKP] (
    [ID]                      INT           NULL,
    [Code]                    VARCHAR (200) NULL,
    [MasterAddressId]         VARCHAR (255) NULL,
    [ProvinceCode]            VARCHAR (50)  NULL,
    [JurCode]                 VARCHAR (10)  NULL,
    [ARN]                     VARCHAR (200) NULL,
    [AppliedDate]             VARCHAR (255) NULL,
    [DateOfDecision]          VARCHAR (255) NULL,
    [IssueDate]               VARCHAR (16)  NULL,
    [MustCommenceDate]        VARCHAR (255) NULL,
    [CompletedDate]           VARCHAR (255) NULL,
    [CanceledRefusedDate]     VARCHAR (255) NULL,
    [DatePermitExpires]       VARCHAR (255) NULL,
    [ValueOfConstruction]     VARCHAR (255) NULL,
    [PermitClass]             VARCHAR (255) NULL,
    [PermitDescription]       VARCHAR (255) NULL,
    [PermitType]              VARCHAR (20)  NULL,
    [PermitFee]               VARCHAR (255) NULL,
    [PermitNumber]            VARCHAR (30)  NULL,
    [PermitStatus]            VARCHAR (255) NULL,
    [DwellingUnitsCreated]    VARCHAR (255) NULL,
    [DwellingUnitsDemolished] VARCHAR (255) NULL,
    [UnitsNetChange]          VARCHAR (255) NULL,
    [DateCreatedUTC]          DATETIME      NULL,
    [LastModifiedDateUTC]     DATETIME      NULL,
    [Data_Source_ID]          INT           NULL,
    [Data_Source_Priority]    INT           NULL,
    [IsPermanentlyInvalid]    BIT           NULL,
    [ReProcess]               BIT           NULL,
    [InvalidRuleId]           VARCHAR (20)  NULL
);

