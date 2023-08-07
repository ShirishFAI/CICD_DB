CREATE TABLE [StageProcessing].[MB_Brandon_Building_Permits] (
    [SourceID]      INT            NULL,
    [Code]          VARCHAR (200)  NULL,
    [Permit Number] NVARCHAR (510) NULL,
    [Permit Type]   VARCHAR (100)  NULL,
    [Address]       VARCHAR (500)  NULL,
    [Roll Number]   VARCHAR (200)  NULL,
    [ActionType]    CHAR (1)       NULL,
    [IsDuplicate]   BIT            NULL
);

