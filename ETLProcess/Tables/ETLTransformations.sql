CREATE TABLE [ETLProcess].[ETLTransformations] (
    [TransformationId] INT            IDENTITY (1, 1) NOT NULL,
    [ColumnName]       VARCHAR (100)  NULL,
    [SourceValue]      VARCHAR (500)  NULL,
    [TransformValue]   VARCHAR (500)  NULL,
    [Function]         VARCHAR (1000) NULL,
    [UTC_CreatedDate]  DATETIME       NULL,
    [UTC_UpdatedDate]  DATETIME       NULL,
    [ActiveFlag]       BIT            NULL,
    CONSTRAINT [PK_ETLTransformations_TransformationId] PRIMARY KEY CLUSTERED ([TransformationId] ASC) WITH (FILLFACTOR = 80)
);

