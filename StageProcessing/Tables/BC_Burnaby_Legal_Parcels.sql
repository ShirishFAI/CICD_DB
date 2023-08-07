CREATE TABLE [StageProcessing].[BC_Burnaby_Legal_Parcels] (
    [SourceID]     INT            NULL,
    [Code]         VARCHAR (200)  NULL,
    [LOT_ID]       NVARCHAR (510) NULL,
    [LTO_PID]      VARCHAR (50)   NULL,
    [ROLL_NUMBER]  VARCHAR (200)  NULL,
    [ADDRESS]      VARCHAR (500)  NULL,
    [PLAN]         VARCHAR (100)  NULL,
    [STRATA]       VARCHAR (5)    NULL,
    [SHAPE_Length] VARCHAR (50)   NULL,
    [ActionType]   CHAR (1)       NULL,
    [IsDuplicate]  BIT            NULL
);

