CREATE TABLE [StageProcessing].[BC_Victoria_Building_Permits] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [PermitNo]      NVARCHAR (510) NULL,
    [SUBJECT]       VARCHAR (200)  NULL,
    [IssuedDate]    NVARCHAR (510) NULL,
    [Unit]          VARCHAR (100)  NULL,
    [House]         VARCHAR (100)  NULL,
    [Street]        VARCHAR (200)  NULL,
    [ActualUse]     VARCHAR (100)  NULL,
    [Neighbourhood] VARCHAR (200)  NULL,
    [X_LONG]        VARCHAR (50)   NULL,
    [Y_LAT]         VARCHAR (50)   NULL,
    [PermitType]    VARCHAR (100)  NULL,
    [ActionType]    CHAR (1)       NULL,
    [IsDuplicate]   BIT            NULL
);

