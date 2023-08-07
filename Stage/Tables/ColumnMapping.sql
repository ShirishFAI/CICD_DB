CREATE TABLE [Stage].[ColumnMapping] (
    [Id]                    INT           IDENTITY (1, 1) NOT NULL,
    [SourceColumnName]      VARCHAR (200) NULL,
    [DestinationColumnName] VARCHAR (100) NULL,
    [IsKey]                 VARCHAR (2)   NULL,
    [ConcatOrder]           VARCHAR (2)   NULL,
    [ConvFunction]          VARCHAR (500) NULL,
    [SourceFileName]        VARCHAR (200) NULL
);

