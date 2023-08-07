CREATE TABLE [StageLanding].[PE_ALL_Civic_Add_Coor_SHP_ca_points] (
    [X_COORD]    NVARCHAR (MAX) NULL,
    [Y_COORD]    NVARCHAR (MAX) NULL,
    [STREET_NO]  NVARCHAR (MAX) NULL,
    [STREET_NM]  NVARCHAR (MAX) NULL,
    [COMM_NM]    NVARCHAR (MAX) NULL,
    [COUNTY]     NVARCHAR (MAX) NULL,
    [PID]        NVARCHAR (MAX) NULL,
    [SERIAL]     NVARCHAR (MAX) NULL,
    [ESN]        NVARCHAR (MAX) NULL,
    [UNIQUE_ID]  NVARCHAR (MAX) NULL,
    [last_edi_1] NVARCHAR (MAX) NULL,
    [SourceID]   INT            IDENTITY (1, 1) NOT NULL,
    [Code]       VARCHAR (200)  NULL
);

