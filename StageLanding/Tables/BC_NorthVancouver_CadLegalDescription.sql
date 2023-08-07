CREATE TABLE [StageLanding].[BC_NorthVancouver_CadLegalDescription] (
    [OBJECTID]         NVARCHAR (MAX) NULL,
    [gislink]          NVARCHAR (MAX) NULL,
    [propertynumber]   NVARCHAR (MAX) NULL,
    [pid]              NVARCHAR (MAX) NULL,
    [ltonumber]        NVARCHAR (MAX) NULL,
    [folio]            NVARCHAR (MAX) NULL,
    [unit]             NVARCHAR (MAX) NULL,
    [lot]              NVARCHAR (MAX) NULL,
    [block]            NVARCHAR (MAX) NULL,
    [plannumber]       NVARCHAR (MAX) NULL,
    [districtlot]      NVARCHAR (MAX) NULL,
    [landdistrict]     NVARCHAR (MAX) NULL,
    [legaldescription] NVARCHAR (MAX) NULL,
    [legaltype]        NVARCHAR (MAX) NULL,
    [taxconsolidation] NVARCHAR (MAX) NULL,
    [propertytpe]      NVARCHAR (MAX) NULL,
    [legalid]          NVARCHAR (MAX) NULL,
    [datestamp]        NVARCHAR (MAX) NULL,
    [GlobalID]         NVARCHAR (MAX) NULL,
    [SourceID]         INT            IDENTITY (1, 1) NOT NULL,
    [Code]             VARCHAR (200)  NULL
);

