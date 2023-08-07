CREATE TABLE [ETLAudit].[ETLProcessHistory_bkp] (
    [RunId]             INT           NULL,
    [ProcessCategoryId] INT           NULL,
    [ProcessId]         INT           NULL,
    [Stage]             VARCHAR (200) NULL,
    [Inserted]          INT           NULL,
    [Updated]           INT           NULL,
    [Deleted]           INT           NULL,
    [UTC_StartedAt]     DATETIME      NULL,
    [UTC_CompletedAt]   DATETIME      NULL,
    [CurrentStatus]     INT           NULL
);

