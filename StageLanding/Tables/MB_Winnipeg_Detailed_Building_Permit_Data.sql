﻿CREATE TABLE [StageLanding].[MB_Winnipeg_Detailed_Building_Permit_Data] (
    [Issue Date]              NVARCHAR (MAX) NULL,
    [Permit Number]           NVARCHAR (MAX) NULL,
    [Parent Permit Number]    NVARCHAR (MAX) NULL,
    [Permit Group]            NVARCHAR (MAX) NULL,
    [Permit Type]             NVARCHAR (MAX) NULL,
    [Sub Type]                NVARCHAR (MAX) NULL,
    [Work Type]               NVARCHAR (MAX) NULL,
    [Street Number]           NVARCHAR (MAX) NULL,
    [Street Name]             NVARCHAR (MAX) NULL,
    [Street Type]             NVARCHAR (MAX) NULL,
    [Street Direction]        NVARCHAR (MAX) NULL,
    [Unit Type]               NVARCHAR (MAX) NULL,
    [Unit Number]             NVARCHAR (MAX) NULL,
    [Neighbourhood Number]    NVARCHAR (MAX) NULL,
    [Neighbourhood Name]      NVARCHAR (MAX) NULL,
    [Community]               NVARCHAR (MAX) NULL,
    [Applicant Business Name] NVARCHAR (MAX) NULL,
    [Dwelling Units Created]  NVARCHAR (MAX) NULL,
    [Dwelling Units Lost]     NVARCHAR (MAX) NULL,
    [Location]                NVARCHAR (MAX) NULL,
    [X Coordinate NAD83]      NVARCHAR (MAX) NULL,
    [Y Coordinate NAD83]      NVARCHAR (MAX) NULL,
    [SourceID]                INT            IDENTITY (1, 1) NOT NULL,
    [Code]                    VARCHAR (200)  NULL
);

