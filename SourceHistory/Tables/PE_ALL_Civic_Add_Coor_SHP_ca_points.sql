CREATE TABLE [SourceHistory].[PE_ALL_Civic_Add_Coor_SHP_ca_points] (
    [Code]        VARCHAR (200)  NULL,
    [STREET_NO]   NVARCHAR (MAX) NULL,
    [STREET_NM]   NVARCHAR (MAX) NULL,
    [COMM_NM]     NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [UNIQUE_ID]   NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_PE_ALL_Civic_Add_Coor_SHP_ca_points_Code]
    ON [SourceHistory].[PE_ALL_Civic_Add_Coor_SHP_ca_points]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_PE_ALL_Civic_Add_Coor_SHP_ca_points_HistEndDate]
    ON [SourceHistory].[PE_ALL_Civic_Add_Coor_SHP_ca_points]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

