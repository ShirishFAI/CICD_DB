CREATE TABLE [ETLProcess].[MS_Automation_Command] (
    [ID]              INT           IDENTITY (1, 1) NOT NULL,
    [Strategy]        VARCHAR (50)  NULL,
    [ActiveFlag]      BIT           DEFAULT ((0)) NULL,
    [ProcessType]     VARCHAR (200) NULL,
    [Command]         VARCHAR (500) NULL,
    [ProfiseeURL]     VARCHAR (200) NULL,
    [UtilityPath]     VARCHAR (500) NULL,
    [CreatedDate]     DATETIME      NULL,
    [LastUpdatedDate] DATETIME      DEFAULT (getutcdate()) NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);

