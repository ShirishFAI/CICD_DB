CREATE TABLE [ETLProcess].[ETLStatus] (
    [StatusId]        INT           NOT NULL,
    [Status]          VARCHAR (100) NULL,
    [UTC_CreatedDate] DATETIME      NULL,
    [UTC_UpdatedDate] DATETIME      NULL,
    CONSTRAINT [PK_ETLStatus_StatusId] PRIMARY KEY CLUSTERED ([StatusId] ASC) WITH (FILLFACTOR = 80),
    CONSTRAINT [UK_ETLStatus_Status] UNIQUE NONCLUSTERED ([Status] ASC) WITH (FILLFACTOR = 80)
);

