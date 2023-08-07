﻿CREATE TABLE [dbo].[PIN_09232022] (
    [ID]                   INT           IDENTITY (1, 1) NOT NULL,
    [Code]                 VARCHAR (200) NULL,
    [PIN]                  VARCHAR (50)  NULL,
    [OriginalPIN]          VARCHAR (200) NULL,
    [ProvinceCode]         VARCHAR (50)  NULL,
    [DateCreatedUTC]       DATETIME      NULL,
    [LastModifiedDateUTC]  DATETIME      NULL,
    [Data_Source_ID]       INT           NULL,
    [Data_Source_Priority] INT           NULL,
    [IsDuplicate]          INT           NOT NULL
);

