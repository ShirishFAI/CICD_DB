CREATE TABLE [ETLProcess].[ETLSourceQueries] (
    [ProcessId]   INT           NOT NULL,
    [SourceQuery] VARCHAR (MAX) NULL,
    CONSTRAINT [FK_ETLProcess] FOREIGN KEY ([ProcessId]) REFERENCES [ETLProcess].[ETLProcess] ([ProcessId])
);

