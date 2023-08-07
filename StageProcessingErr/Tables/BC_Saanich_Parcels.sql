CREATE TABLE [StageProcessingErr].[BC_Saanich_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [JURISDICT]     NVARCHAR (MAX) NULL,
    [FOLIO]         NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [STRUNIT]       NVARCHAR (MAX) NULL,
    [STRNUMBER]     NVARCHAR (MAX) NULL,
    [STRNAME]       NVARCHAR (MAX) NULL,
    [LEGPLAN]       NVARCHAR (MAX) NULL,
    [AREASQM]       NVARCHAR (MAX) NULL,
    [AREAHECT]      NVARCHAR (MAX) NULL
);

