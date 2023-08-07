CREATE TABLE [ETLProcess].[MS_IncrementalAttributes] (
    [ID]              INT           IDENTITY (1, 1) NOT NULL,
    [EntityName]      VARCHAR (100) NULL,
    [AttributeName]   VARCHAR (100) NULL,
    [ActiveFlag]      BIT           NULL,
    [UTC_CreatedDate] DATETIME      NULL,
    [UTC_UpdatedDate] DATETIME      NULL
);

