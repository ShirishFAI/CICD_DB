CREATE TABLE [StageProcessing].[BC_NorthVancouver_CadLegalDescription] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [propertynumber]   NVARCHAR (510) NULL,
    [pid]              VARCHAR (50)   NULL,
    [folio]            VARCHAR (200)  NULL,
    [plannumber]       VARCHAR (100)  NULL,
    [legaldescription] VARCHAR (4000) NULL,
    [legaltype]        VARCHAR (100)  NULL,
    [ActionType]       CHAR (1)       NULL,
    [IsDuplicate]      BIT            NULL
);

