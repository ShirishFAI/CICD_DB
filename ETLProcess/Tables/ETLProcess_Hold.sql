CREATE TABLE [ETLProcess].[ETLProcess_Hold] (
    [ProcessCategoryId]    INT           NOT NULL,
    [ProcessId]            INT           IDENTITY (1, 1) NOT NULL,
    [ProcessName]          VARCHAR (100) NULL,
    [IsAddressReliable]    BIT           NULL,
    [Data_Source_Priority] INT           NULL,
    [UTC_CreatedDate]      DATETIME      NOT NULL,
    [UTC_UpdatedDate]      DATETIME      NOT NULL,
    [ActiveFlag]           BIT           NULL,
    CONSTRAINT [PK_ETLProcess_Hold_ProcessID] PRIMARY KEY CLUSTERED ([ProcessId] ASC) WITH (FILLFACTOR = 80),
    CONSTRAINT [FK_ETLProcess_ETLProcess_Hold] FOREIGN KEY ([ProcessCategoryId]) REFERENCES [ETLProcess].[ETLProcessCategory] ([ProcessCategoryId]),
    CONSTRAINT [UK_ETLProcess_Hold_ProcessName] UNIQUE NONCLUSTERED ([ProcessName] ASC) WITH (FILLFACTOR = 80)
);

