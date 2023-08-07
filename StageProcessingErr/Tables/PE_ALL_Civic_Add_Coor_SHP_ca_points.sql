CREATE TABLE [StageProcessingErr].[PE_ALL_Civic_Add_Coor_SHP_ca_points] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [STREET_NO]     NVARCHAR (MAX) NULL,
    [STREET_NM]     NVARCHAR (MAX) NULL,
    [COMM_NM]       NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [UNIQUE_ID]     NVARCHAR (MAX) NULL
);

