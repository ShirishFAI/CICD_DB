﻿CREATE TABLE [StageLanding].[BC_Burnaby_Legal_Parcels] (
    [OBJECTID]       NVARCHAR (MAX) NULL,
    [LOT_ID]         NVARCHAR (MAX) NULL,
    [ROOT_PLAN]      NVARCHAR (MAX) NULL,
    [LTO_PID]        NVARCHAR (MAX) NULL,
    [ROLL_NUMBER]    NVARCHAR (MAX) NULL,
    [ST_NMBR]        NVARCHAR (MAX) NULL,
    [ST_NAME]        NVARCHAR (MAX) NULL,
    [ST_TYPE]        NVARCHAR (MAX) NULL,
    [ADDRESS]        NVARCHAR (MAX) NULL,
    [PLAN]           NVARCHAR (MAX) NULL,
    [DL]             NVARCHAR (MAX) NULL,
    [BLOCK]          NVARCHAR (MAX) NULL,
    [LOT]            NVARCHAR (MAX) NULL,
    [BBY_PID]        NVARCHAR (MAX) NULL,
    [STRATA]         NVARCHAR (MAX) NULL,
    [AIR_SPACE]      NVARCHAR (MAX) NULL,
    [ANGLE]          NVARCHAR (MAX) NULL,
    [AGGREGATE]      NVARCHAR (MAX) NULL,
    [AGGR_ROLL]      NVARCHAR (MAX) NULL,
    [ST_SFX]         NVARCHAR (MAX) NULL,
    [ST_NAME_NUM]    NVARCHAR (MAX) NULL,
    [HeritageStatus] NVARCHAR (MAX) NULL,
    [SHAPE_Length]   NVARCHAR (MAX) NULL,
    [GIS_ID]         NVARCHAR (MAX) NULL,
    [SourceID]       INT            IDENTITY (1, 1) NOT NULL,
    [Code]           VARCHAR (200)  NULL
);

