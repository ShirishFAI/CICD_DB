CREATE TABLE [StageProcessingErr].[BC_Permit_Export_FCTC] (
    [SourceID]       INT            NULL,
    [Code]           VARCHAR (200)  NULL,
    [ErrorStatusId]  TINYINT        NULL,
    [Jurisdiction]   NVARCHAR (MAX) NULL,
    [RollNumber]     NVARCHAR (MAX) NULL,
    [ServiceDate]    NVARCHAR (MAX) NULL,
    [PermitNumber]   NVARCHAR (MAX) NULL,
    [DemolitionFlag] NVARCHAR (MAX) NULL
);

