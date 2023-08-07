CREATE TABLE [SourceHistory].[PE_ALL_Master_Street_Address_Guide_master_ca] (
    [Code]        VARCHAR (200)  NULL,
    [X_COORD]     NVARCHAR (MAX) NULL,
    [Y_COORD]     NVARCHAR (MAX) NULL,
    [STREET_NO]   NVARCHAR (MAX) NULL,
    [STREET_NM]   NVARCHAR (MAX) NULL,
    [COMM_NM]     NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [INFO]        NVARCHAR (MAX) NULL,
    [APT_NO]      NVARCHAR (MAX) NULL,
    [UNIQUE_ID]   NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_PE_ALL_Master_Street_Address_Guide_master_ca_Code]
    ON [SourceHistory].[PE_ALL_Master_Street_Address_Guide_master_ca]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_PE_ALL_Master_Street_Address_Guide_master_ca_HistEndDate]
    ON [SourceHistory].[PE_ALL_Master_Street_Address_Guide_master_ca]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

