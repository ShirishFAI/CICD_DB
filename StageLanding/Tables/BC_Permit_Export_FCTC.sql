CREATE TABLE [StageLanding].[BC_Permit_Export_FCTC] (
    [Area]           NVARCHAR (MAX) NULL,
    [Jurisdiction]   NVARCHAR (MAX) NULL,
    [RollNumber]     NVARCHAR (MAX) NULL,
    [ServiceDate]    NVARCHAR (MAX) NULL,
    [PermitNumber]   NVARCHAR (MAX) NULL,
    [DemolitionFlag] NVARCHAR (MAX) NULL,
    [SourceID]       INT            IDENTITY (1, 1) NOT NULL,
    [Code]           VARCHAR (200)  NULL
);

