CREATE TABLE [StageProcessingErr].[BC_Victoria_Building_Permits] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [PermitNo]      NVARCHAR (MAX) NULL,
    [SUBJECT]       NVARCHAR (MAX) NULL,
    [IssuedDate]    NVARCHAR (MAX) NULL,
    [Unit]          NVARCHAR (MAX) NULL,
    [House]         NVARCHAR (MAX) NULL,
    [Street]        NVARCHAR (MAX) NULL,
    [ActualUse]     NVARCHAR (MAX) NULL,
    [Neighbourhood] NVARCHAR (MAX) NULL,
    [X_LONG]        NVARCHAR (MAX) NULL,
    [Y_LAT]         NVARCHAR (MAX) NULL,
    [PermitType]    NVARCHAR (MAX) NULL
);

