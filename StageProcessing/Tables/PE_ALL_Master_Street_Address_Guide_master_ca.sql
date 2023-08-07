CREATE TABLE [StageProcessing].[PE_ALL_Master_Street_Address_Guide_master_ca] (
    [SourceID]    INT            NULL,
    [Code]        VARCHAR (200)  NULL,
    [X_COORD]     VARCHAR (50)   NULL,
    [Y_COORD]     VARCHAR (50)   NULL,
    [STREET_NO]   VARCHAR (100)  NULL,
    [STREET_NM]   VARCHAR (200)  NULL,
    [COMM_NM]     VARCHAR (200)  NULL,
    [PID]         VARCHAR (50)   NULL,
    [INFO]        VARCHAR (255)  NULL,
    [APT_NO]      VARCHAR (100)  NULL,
    [UNIQUE_ID]   NVARCHAR (510) NULL,
    [ActionType]  CHAR (1)       NULL,
    [IsDuplicate] BIT            NULL
);

