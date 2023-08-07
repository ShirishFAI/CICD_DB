CREATE TABLE [SourceHistory].[AB_Edmonton_General_Building_Permits] (
    [Code]                 VARCHAR (200)  NULL,
    [Row ID]               NVARCHAR (MAX) NULL,
    [ADDRESS]              NVARCHAR (MAX) NULL,
    [LEGAL_DESCRIPTION]    NVARCHAR (MAX) NULL,
    [NEIGHBOURHOOD]        NVARCHAR (MAX) NULL,
    [NEIGHBOURHOOD_NUMBER] NVARCHAR (MAX) NULL,
    [BUILDING_TYPE]        NVARCHAR (MAX) NULL,
    [ZONING]               NVARCHAR (MAX) NULL,
    [LATITUDE]             NVARCHAR (MAX) NULL,
    [LONGITUDE]            NVARCHAR (MAX) NULL,
    [LOCATION]             NVARCHAR (MAX) NULL,
    [HistEndDate]          DATETIME       NULL,
    [IsDuplicate]          BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_Edmonton_General_Building_Permits_Code]
    ON [SourceHistory].[AB_Edmonton_General_Building_Permits]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_Edmonton_General_Building_Permits_HistEndDate]
    ON [SourceHistory].[AB_Edmonton_General_Building_Permits]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

