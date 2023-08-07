CREATE TABLE [ETLProcess].[AzureFunctionLog] (
    [ID]              INT              IDENTITY (1, 1) NOT NULL,
    [GUID]            UNIQUEIDENTIFIER NULL,
    [MessageText]     VARCHAR (8000)   NULL,
    [UTC_StartedDate] DATETIME         DEFAULT (getutcdate()) NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);

