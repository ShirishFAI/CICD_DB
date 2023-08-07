CREATE TABLE [ETLAudit].[ETLProcessCategory_bkp] (
    [RunId]             INT      IDENTITY (1, 1) NOT NULL,
    [ProcessCategoryId] INT      NULL,
    [UTC_StartedAt]     DATETIME NULL,
    [UTC_CompletedAt]   DATETIME NULL,
    [CurrentStatus]     INT      NULL
);

