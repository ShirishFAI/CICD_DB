CREATE TABLE [SourceHistory].[BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp] (
    [Code]        VARCHAR (200)  NULL,
    [Jur]         NVARCHAR (MAX) NULL,
    [folio]       NVARCHAR (MAX) NULL,
    [Legal_Desc]  NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [Civic_Addr]  NVARCHAR (MAX) NULL,
    [COMMUNITY]   NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp_Code]
    ON [SourceHistory].[BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp_HistEndDate]
    ON [SourceHistory].[BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

