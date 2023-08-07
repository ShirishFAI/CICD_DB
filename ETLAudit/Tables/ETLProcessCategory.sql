CREATE TABLE [ETLAudit].[ETLProcessCategory] (
    [RunId]             INT      IDENTITY (1, 1) NOT NULL,
    [ProcessCategoryId] INT      NULL,
    [UTC_StartedAt]     DATETIME NULL,
    [UTC_CompletedAt]   DATETIME NULL,
    [CurrentStatus]     INT      NULL,
    CONSTRAINT [PK_ETLProcessCategory_RunId] PRIMARY KEY CLUSTERED ([RunId] ASC) WITH (FILLFACTOR = 80),
    CONSTRAINT [FK_ETLProcessCategory_CurrentStatus] FOREIGN KEY ([CurrentStatus]) REFERENCES [ETLProcess].[ETLStatus] ([StatusId])
);

