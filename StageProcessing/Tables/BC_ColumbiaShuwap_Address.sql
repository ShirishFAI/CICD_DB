CREATE TABLE [StageProcessing].[BC_ColumbiaShuwap_Address] (
    [SourceID]    INT            NULL,
    [Code]        VARCHAR (200)  NULL,
    [IDHOUSE]     NVARCHAR (510) NULL,
    [IDPARCEL]    NVARCHAR (510) NULL,
    [ROLL]        VARCHAR (200)  NULL,
    [HOUSENUM]    VARCHAR (100)  NULL,
    [APT_BAY]     VARCHAR (100)  NULL,
    [STREET]      VARCHAR (200)  NULL,
    [HOUSETYPE]   VARCHAR (20)   NULL,
    [ActionType]  CHAR (1)       NULL,
    [IsDuplicate] BIT            NULL
);

