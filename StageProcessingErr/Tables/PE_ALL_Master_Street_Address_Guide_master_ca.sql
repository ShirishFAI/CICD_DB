CREATE TABLE [StageProcessingErr].[PE_ALL_Master_Street_Address_Guide_master_ca] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [X_COORD]       NVARCHAR (MAX) NULL,
    [Y_COORD]       NVARCHAR (MAX) NULL,
    [STREET_NO]     NVARCHAR (MAX) NULL,
    [STREET_NM]     NVARCHAR (MAX) NULL,
    [COMM_NM]       NVARCHAR (MAX) NULL,
    [PID]           NVARCHAR (MAX) NULL,
    [INFO]          NVARCHAR (MAX) NULL,
    [APT_NO]        NVARCHAR (MAX) NULL,
    [UNIQUE_ID]     NVARCHAR (MAX) NULL
);

