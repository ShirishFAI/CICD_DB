CREATE TABLE [ETLProcess].[ETLErrorRecordStatus] (
    [ErrorStatusId]          INT           NOT NULL,
    [ErrorStatusDescription] VARCHAR (200) NULL,
    [UTC_CreatedDate]        DATETIME      NULL,
    [UTC_UpdatedDate]        DATETIME      NULL,
    CONSTRAINT [PK_ETLErrorRecordStatus_ErrorStatusId] PRIMARY KEY CLUSTERED ([ErrorStatusId] ASC) WITH (FILLFACTOR = 80)
);

