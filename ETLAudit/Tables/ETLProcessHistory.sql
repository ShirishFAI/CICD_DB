CREATE TABLE [ETLAudit].[ETLProcessHistory] (
    [RunId]             INT           NULL,
    [ProcessCategoryId] INT           NULL,
    [ProcessId]         INT           NULL,
    [Stage]             VARCHAR (200) NULL,
    [Inserted]          INT           NULL,
    [Updated]           INT           NULL,
    [Deleted]           INT           NULL,
    [UTC_StartedAt]     DATETIME      NULL,
    [UTC_CompletedAt]   DATETIME      NULL,
    [CurrentStatus]     INT           NULL,
    CONSTRAINT [FK_ETLProcessHistory_CurrentStatus] FOREIGN KEY ([CurrentStatus]) REFERENCES [ETLProcess].[ETLStatus] ([StatusId]),
    CONSTRAINT [FK_ETLProcessHistory_ProcessCategoryId] FOREIGN KEY ([ProcessCategoryId]) REFERENCES [ETLProcess].[ETLProcessCategory] ([ProcessCategoryId]),
    CONSTRAINT [FK_ETLProcessHistory_ProcessId] FOREIGN KEY ([ProcessId]) REFERENCES [ETLProcess].[ETLProcess] ([ProcessId]),
    CONSTRAINT [FK_ETLProcessHistory_RunId] FOREIGN KEY ([RunId]) REFERENCES [ETLAudit].[ETLProcessCategory] ([RunId])
);

