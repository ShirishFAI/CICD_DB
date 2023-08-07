CREATE TABLE [ETLProcess].[ETLNotifications] (
    [NotificationId] INT           IDENTITY (1, 1) NOT NULL,
    [Category]       VARCHAR (100) NULL,
    [FirstName]      VARCHAR (50)  NULL,
    [LastName]       VARCHAR (50)  NULL,
    [EmailId]        VARCHAR (100) NULL,
    [TextId]         VARCHAR (200) NULL,
    [ActiveFlag]     BIT           NULL,
    CONSTRAINT [PK_ETLNotifications_NotificationId] PRIMARY KEY CLUSTERED ([NotificationId] ASC) WITH (FILLFACTOR = 80)
);

