CREATE TABLE [SourceHistory].[BC_Surrey_Property_Detail_Listing_2017] (
    [Code]              VARCHAR (200)  NULL,
    [UNIT]              NVARCHAR (MAX) NULL,
    [HOUSE]             NVARCHAR (MAX) NULL,
    [STREET]            NVARCHAR (MAX) NULL,
    [POSTAL_CODE]       NVARCHAR (MAX) NULL,
    [PID]               NVARCHAR (MAX) NULL,
    [FOLIO]             NVARCHAR (MAX) NULL,
    [PLAN_NUMBER]       NVARCHAR (MAX) NULL,
    [LOT_SIZE]          NVARCHAR (MAX) NULL,
    [LEGAL_DESCRIPTION] NVARCHAR (MAX) NULL,
    [ZONE]              NVARCHAR (MAX) NULL,
    [ZONE_DESCRIPTION]  NVARCHAR (MAX) NULL,
    [ASSESSMENT_YEAR]   NVARCHAR (MAX) NULL,
    [GROSS_ASSESSMENT]  NVARCHAR (MAX) NULL,
    [Latitude]          NVARCHAR (MAX) NULL,
    [Longitude]         NVARCHAR (MAX) NULL,
    [LOT_UNITOFMEASURE] NVARCHAR (MAX) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Surrey_Property_Detail_Listing_2017_Code]
    ON [SourceHistory].[BC_Surrey_Property_Detail_Listing_2017]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Surrey_Property_Detail_Listing_2017_HistEndDate]
    ON [SourceHistory].[BC_Surrey_Property_Detail_Listing_2017]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

