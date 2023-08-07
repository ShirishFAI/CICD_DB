CREATE TABLE [StageProcessingErr].[BC_Burnaby_Legal_Parcels] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [ErrorStatusId] TINYINT        NULL,
    [LOT_ID]        NVARCHAR (MAX) NULL,
    [LTO_PID]       NVARCHAR (MAX) NULL,
    [ROLL_NUMBER]   NVARCHAR (MAX) NULL,
    [ADDRESS]       NVARCHAR (MAX) NULL,
    [PLAN]          NVARCHAR (MAX) NULL,
    [STRATA]        NVARCHAR (MAX) NULL,
    [SHAPE_Length]  NVARCHAR (MAX) NULL
);

