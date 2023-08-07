CREATE TABLE [ETLAudit].[ETLProcess_bkp] (
    [RunId]             INT           NULL,
    [ProcessCategoryId] INT           NULL,
    [ProcessId]         INT           NULL,
    [CurrentStage]      VARCHAR (200) NULL,
    [UTC_StartedAt]     DATETIME      NULL,
    [UTC_CompletedAt]   DATETIME      NULL,
    [CurrentStatus]     INT           NULL
);

