CREATE TABLE [SourceHistory].[BC_Nanaimo_BusinessLicences] (
    [Code]                 VARCHAR (200)  NULL,
    [Licence]              NVARCHAR (MAX) NULL,
    [CivicAddressUnit]     NVARCHAR (MAX) NULL,
    [CivicAddressHouse]    NVARCHAR (MAX) NULL,
    [CivicAddressStreet]   NVARCHAR (MAX) NULL,
    [CivicAddressCity]     NVARCHAR (MAX) NULL,
    [CivicAddressProvince] NVARCHAR (MAX) NULL,
    [BusinessDescription]  NVARCHAR (MAX) NULL,
    [EDO_NAICS_Desc]       NVARCHAR (MAX) NULL,
    [NAICSCategory]        NVARCHAR (MAX) NULL,
    [LEGAL_ID]             NVARCHAR (MAX) NULL,
    [Latitude]             NVARCHAR (MAX) NULL,
    [Longitude]            NVARCHAR (MAX) NULL,
    [HistEndDate]          DATETIME       NULL,
    [IsDuplicate]          BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Nanaimo_BusinessLicences_Code]
    ON [SourceHistory].[BC_Nanaimo_BusinessLicences]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Nanaimo_BusinessLicences_HistEndDate]
    ON [SourceHistory].[BC_Nanaimo_BusinessLicences]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

