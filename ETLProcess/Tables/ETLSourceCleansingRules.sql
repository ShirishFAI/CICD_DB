CREATE TABLE [ETLProcess].[ETLSourceCleansingRules] (
    [CleansingRuleId]    INT           IDENTITY (1, 1) NOT NULL,
    [ColumnName]         VARCHAR (100) NULL,
    [CleansingRule]      VARCHAR (500) NULL,
    [ReplaceValue]       VARCHAR (500) NULL,
    [CleansingRule_Desc] VARCHAR (200) NULL,
    [Function]           VARCHAR (100) NULL,
    [UTC_CreatedDate]    DATETIME      NULL,
    [UTC_UpdatedDate]    DATETIME      NULL,
    [ActiveFlag]         BIT           NULL,
    CONSTRAINT [PK_ETLSourceCleansingRules_CleansingRuleId] PRIMARY KEY CLUSTERED ([CleansingRuleId] ASC) WITH (FILLFACTOR = 80)
);

