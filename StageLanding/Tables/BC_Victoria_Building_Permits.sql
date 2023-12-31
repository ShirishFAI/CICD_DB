﻿CREATE TABLE [StageLanding].[BC_Victoria_Building_Permits] (
    [gislink]         NVARCHAR (MAX) NULL,
    [CATEGORY]        NVARCHAR (MAX) NULL,
    [type]            NVARCHAR (MAX) NULL,
    [PermitNo]        NVARCHAR (MAX) NULL,
    [SUBJECT]         NVARCHAR (MAX) NULL,
    [Status]          NVARCHAR (MAX) NULL,
    [Purpose]         NVARCHAR (MAX) NULL,
    [IssuedDate]      NVARCHAR (MAX) NULL,
    [BldgValue]       NVARCHAR (MAX) NULL,
    [Unit]            NVARCHAR (MAX) NULL,
    [House]           NVARCHAR (MAX) NULL,
    [Street]          NVARCHAR (MAX) NULL,
    [completed_date]  NVARCHAR (MAX) NULL,
    [CREATED_DATE]    NVARCHAR (MAX) NULL,
    [ParcelType]      NVARCHAR (MAX) NULL,
    [AUC]             NVARCHAR (MAX) NULL,
    [ActualUse]       NVARCHAR (MAX) NULL,
    [Neighbourhood]   NVARCHAR (MAX) NULL,
    [X_LONG]          NVARCHAR (MAX) NULL,
    [Y_LAT]           NVARCHAR (MAX) NULL,
    [ContactType]     NVARCHAR (MAX) NULL,
    [Name]            NVARCHAR (MAX) NULL,
    [mailing_address] NVARCHAR (MAX) NULL,
    [phone]           NVARCHAR (MAX) NULL,
    [cell]            NVARCHAR (MAX) NULL,
    [email]           NVARCHAR (MAX) NULL,
    [fax]             NVARCHAR (MAX) NULL,
    [AUC_Group]       NVARCHAR (MAX) NULL,
    [PermitType]      NVARCHAR (MAX) NULL,
    [SourceID]        INT            IDENTITY (1, 1) NOT NULL,
    [Code]            VARCHAR (200)  NULL
);

