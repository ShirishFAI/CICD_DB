CREATE TABLE [SourceHistory].[BC_Vancouver_property_tax_report_2018] (
    [Code]                      VARCHAR (200)  NULL,
    [PID]                       NVARCHAR (MAX) NULL,
    [LEGAL_TYPE]                NVARCHAR (MAX) NULL,
    [FOLIO]                     NVARCHAR (MAX) NULL,
    [ZONE_NAME]                 NVARCHAR (MAX) NULL,
    [ZONE_CATEGORY]             NVARCHAR (MAX) NULL,
    [PLAN]                      NVARCHAR (MAX) NULL,
    [FROM_CIVIC_NUMBER]         NVARCHAR (MAX) NULL,
    [TO_CIVIC_NUMBER]           NVARCHAR (MAX) NULL,
    [STREET_NAME]               NVARCHAR (MAX) NULL,
    [PROPERTY_POSTAL_CODE]      NVARCHAR (MAX) NULL,
    [NARRATIVE_LEGAL_LINE1]     NVARCHAR (MAX) NULL,
    [NARRATIVE_LEGAL_LINE2]     NVARCHAR (MAX) NULL,
    [NARRATIVE_LEGAL_LINE3]     NVARCHAR (MAX) NULL,
    [NARRATIVE_LEGAL_LINE4]     NVARCHAR (MAX) NULL,
    [NARRATIVE_LEGAL_LINE5]     NVARCHAR (MAX) NULL,
    [CURRENT_LAND_VALUE]        NVARCHAR (MAX) NULL,
    [CURRENT_IMPROVEMENT_VALUE] NVARCHAR (MAX) NULL,
    [TAX_ASSESSMENT_YEAR]       NVARCHAR (MAX) NULL,
    [YEAR_BUILT]                NVARCHAR (MAX) NULL,
    [TAX_LEVY]                  NVARCHAR (MAX) NULL,
    [NEIGHBOURHOOD_CODE]        NVARCHAR (MAX) NULL,
    [JurCode]                   NVARCHAR (MAX) NULL,
    [HashBytes]                 BINARY (64)    NULL,
    [HistEndDate]               DATETIME       NULL,
    [IsDuplicate]               BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Vancouver_property_tax_report_2018_Code]
    ON [SourceHistory].[BC_Vancouver_property_tax_report_2018]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Vancouver_property_tax_report_2018_HistEndDate]
    ON [SourceHistory].[BC_Vancouver_property_tax_report_2018]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

