CREATE TABLE [StageProcessing].[BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp] (
    [SourceID]    INT            NULL,
    [Code]        VARCHAR (200)  NULL,
    [Jur]         VARCHAR (10)   NULL,
    [folio]       NVARCHAR (510) NULL,
    [Legal_Desc]  VARCHAR (4000) NULL,
    [PID]         VARCHAR (50)   NULL,
    [Civic_Addr]  VARCHAR (500)  NULL,
    [COMMUNITY]   VARCHAR (200)  NULL,
    [ActionType]  CHAR (1)       NULL,
    [IsDuplicate] BIT            NULL
);

