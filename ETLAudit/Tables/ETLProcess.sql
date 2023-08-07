CREATE TABLE [ETLAudit].[ETLProcess] (
    [RunId]             INT           NULL,
    [ProcessCategoryId] INT           NULL,
    [ProcessId]         INT           NULL,
    [CurrentStage]      VARCHAR (200) NULL,
    [UTC_StartedAt]     DATETIME      NULL,
    [UTC_CompletedAt]   DATETIME      NULL,
    [CurrentStatus]     INT           NULL,
    CONSTRAINT [FK_ETLProcess_CurrentStatus] FOREIGN KEY ([CurrentStatus]) REFERENCES [ETLProcess].[ETLStatus] ([StatusId]),
    CONSTRAINT [FK_ETLProcess_ProcessCategoryId] FOREIGN KEY ([ProcessCategoryId]) REFERENCES [ETLProcess].[ETLProcessCategory] ([ProcessCategoryId]),
    CONSTRAINT [FK_ETLProcess_ProcessId] FOREIGN KEY ([ProcessId]) REFERENCES [ETLProcess].[ETLProcess] ([ProcessId]),
    CONSTRAINT [FK_ETLProcess_RunId] FOREIGN KEY ([RunId]) REFERENCES [ETLAudit].[ETLProcessCategory] ([RunId])
);

