CREATE TABLE [StageLanding].[BCAWeeklyXML_Test] (
    [Id]             INT      IDENTITY (1, 1) NOT NULL,
    [XMLData]        XML      NULL,
    [LoadedDateTime] DATETIME NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC) WITH (FILLFACTOR = 80)
);

