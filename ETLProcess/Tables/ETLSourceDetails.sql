CREATE TABLE [ETLProcess].[ETLSourceDetails] (
    [Id]           INT           IDENTITY (1, 1) NOT NULL,
    [SourceName]   VARCHAR (50)  NULL,
    [ServerName]   VARCHAR (100) NULL,
    [ServerIP]     VARCHAR (50)  NULL,
    [ProcessName]  VARCHAR (100) NULL,
    [DatabaseName] VARCHAR (50)  NULL,
    [UserName]     VARCHAR (100) NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC) WITH (FILLFACTOR = 80)
);

