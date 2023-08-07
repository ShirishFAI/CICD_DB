CREATE TABLE [ETLProcess].[ETLStoredProcedureErrors] (
    [Id]              INT            IDENTITY (1, 1) NOT NULL,
    [ProcessCategory] VARCHAR (100)  NULL,
    [ProcessName]     VARCHAR (100)  NULL,
    [ErrorNumber]     INT            NULL,
    [ErrorSeverity]   INT            NULL,
    [ErrorState]      INT            NULL,
    [ErrorProcedure]  VARCHAR (200)  NULL,
    [ErrorLine]       INT            NULL,
    [ErrorMessage]    VARCHAR (2000) NULL,
    [ErrorDate]       DATETIME       NULL
);

