CREATE TABLE [StageProcessingErr].[BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [Jur]           NVARCHAR (MAX) NULL,
    [folio]         NVARCHAR (MAX) NULL,
    [Legal_Desc]    NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [Civic_Addr]    NVARCHAR (MAX) NULL,
    [COMMUNITY]     NVARCHAR (MAX) NULL
);

