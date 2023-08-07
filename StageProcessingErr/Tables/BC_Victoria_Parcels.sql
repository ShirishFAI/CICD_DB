CREATE TABLE [StageProcessingErr].[BC_Victoria_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [FOLIO]         NVARCHAR (MAX) NULL,
    [HOUSE]         NVARCHAR (MAX) NULL,
    [LEGAL_TYPE]    NVARCHAR (MAX) NULL,
    [ParcelArea]    NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [STREET]        NVARCHAR (MAX) NULL,
    [UNIT]          NVARCHAR (MAX) NULL,
    [ActualUse]     NVARCHAR (MAX) NULL,
    [TAXYEAR]       NVARCHAR (MAX) NULL,
    [TaxLevy]       NVARCHAR (MAX) NULL
);

