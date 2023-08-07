CREATE TABLE [StageProcessing].[PE_ALL_Civic_Add_Coor_SHP_ca_points] (
    [SourceID]    INT            NULL,
    [Code]        VARCHAR (200)  NULL,
    [STREET_NO]   VARCHAR (100)  NULL,
    [STREET_NM]   VARCHAR (200)  NULL,
    [COMM_NM]     VARCHAR (200)  NULL,
    [PID]         VARCHAR (50)   NULL,
    [UNIQUE_ID]   NVARCHAR (510) NULL,
    [ActionType]  CHAR (1)       NULL,
    [IsDuplicate] BIT            NULL
);

