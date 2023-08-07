CREATE TABLE [SourceHistory].[MB_Brandon_Property_Info] (
    [Code]              VARCHAR (200)  NULL,
    [dROLLNMBR]         NVARCHAR (MAX) NULL,
    [STREET_ADDRESS]    NVARCHAR (MAX) NULL,
    [dZONINGCD]         NVARCHAR (MAX) NULL,
    [dNMBRDWELLINGS]    NVARCHAR (MAX) NULL,
    [dYEARBUILT]        NVARCHAR (MAX) NULL,
    [daPROPMEASFRONT_1] NVARCHAR (MAX) NULL,
    [dPROPMEASLOT]      NVARCHAR (MAX) NULL,
    [GROSSTX]           NVARCHAR (MAX) NULL,
    [TOTAL_VAL]         NVARCHAR (MAX) NULL,
    [HistEndDate]       DATETIME       NULL,
    [IsDuplicate]       BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_MB_Brandon_Property_Info_Code]
    ON [SourceHistory].[MB_Brandon_Property_Info]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_MB_Brandon_Property_Info_HistEndDate]
    ON [SourceHistory].[MB_Brandon_Property_Info]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

