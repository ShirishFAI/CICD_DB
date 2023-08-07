CREATE TABLE [StageLanding].[BC_Victoria_Parcels] (
    [FOLIO]      NVARCHAR (MAX) NULL,
    [FullAddr]   NVARCHAR (MAX) NULL,
    [GISLINK]    NVARCHAR (MAX) NULL,
    [HOUSE]      NVARCHAR (MAX) NULL,
    [LEGAL_TYPE] NVARCHAR (MAX) NULL,
    [ParcelArea] NVARCHAR (MAX) NULL,
    [PID]        NVARCHAR (MAX) NULL,
    [STREET]     NVARCHAR (MAX) NULL,
    [UNIT]       NVARCHAR (MAX) NULL,
    [AUC]        NVARCHAR (MAX) NULL,
    [ActualUse]  NVARCHAR (MAX) NULL,
    [TAXYEAR]    NVARCHAR (MAX) NULL,
    [TaxLevy]    NVARCHAR (MAX) NULL,
    [SourceID]   INT            IDENTITY (1, 1) NOT NULL,
    [Code]       VARCHAR (200)  NULL
);

