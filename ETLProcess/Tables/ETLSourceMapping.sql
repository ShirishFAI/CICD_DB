CREATE TABLE [ETLProcess].[ETLSourceMapping] (
    [MappingId]                 INT           IDENTITY (1, 1) NOT NULL,
    [ProcessId]                 VARCHAR (100) NULL,
    [SourceColumnName]          VARCHAR (100) NULL,
    [DestinationColumnName]     VARCHAR (100) NULL,
    [IsKey]                     BIT           NULL,
    [DestinationColumnDataType] VARCHAR (50)  NULL,
    [ConcatOrder]               INT           NULL,
    [ConvFunction]              VARCHAR (100) NULL,
    CONSTRAINT [PK_ETLSourceMapping_MappingId] PRIMARY KEY CLUSTERED ([MappingId] ASC) WITH (FILLFACTOR = 80)
);

