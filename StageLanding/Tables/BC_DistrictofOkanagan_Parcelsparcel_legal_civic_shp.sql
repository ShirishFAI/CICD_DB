CREATE TABLE [StageLanding].[BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp] (
    [Jur]        NVARCHAR (MAX) NULL,
    [folio]      NVARCHAR (MAX) NULL,
    [Legal_Desc] NVARCHAR (MAX) NULL,
    [PID]        NVARCHAR (MAX) NULL,
    [Civic_Addr] NVARCHAR (MAX) NULL,
    [Civic_Ad_1] NVARCHAR (MAX) NULL,
    [Civic_Ad_2] NVARCHAR (MAX) NULL,
    [Civic_Ad_3] NVARCHAR (MAX) NULL,
    [Civic_Ad_4] NVARCHAR (MAX) NULL,
    [COMMUNITY]  NVARCHAR (MAX) NULL,
    [SourceID]   INT            IDENTITY (1, 1) NOT NULL,
    [Code]       VARCHAR (200)  NULL
);

