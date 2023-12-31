﻿CREATE TABLE [StageLanding].[NT_ALL_Business_Directory] (
    [BUSINESSNAME] NVARCHAR (MAX) NULL,
    [BUSINESSTYPE] NVARCHAR (MAX) NULL,
    [ADDRESS1]     NVARCHAR (MAX) NULL,
    [ADDRESS2]     NVARCHAR (MAX) NULL,
    [ADDRESS3]     NVARCHAR (MAX) NULL,
    [CITY]         NVARCHAR (MAX) NULL,
    [PROVINCE]     NVARCHAR (MAX) NULL,
    [POSTALCODE]   NVARCHAR (MAX) NULL,
    [PHONE]        NVARCHAR (MAX) NULL,
    [EMAILADDRESS] NVARCHAR (MAX) NULL,
    [SourceID]     INT            IDENTITY (1, 1) NOT NULL,
    [Code]         VARCHAR (200)  NULL
);

