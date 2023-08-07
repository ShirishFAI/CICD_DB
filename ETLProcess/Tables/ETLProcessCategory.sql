CREATE TABLE [ETLProcess].[ETLProcessCategory] (
    [ProcessCategoryId]   INT           NOT NULL,
    [ProcessCategoryName] VARCHAR (100) NULL,
    [UTC_CreatedDate]     DATETIME      NOT NULL,
    [UTC_UpdatedDate]     DATETIME      NOT NULL,
    [ActiveFlag]          BIT           NULL,
    CONSTRAINT [PK_ETLProcessCategory] PRIMARY KEY CLUSTERED ([ProcessCategoryId] ASC) WITH (FILLFACTOR = 80),
    CONSTRAINT [UK_ETLProcessCategory_ProcessCategoryName] UNIQUE NONCLUSTERED ([ProcessCategoryName] ASC) WITH (FILLFACTOR = 80)
);

