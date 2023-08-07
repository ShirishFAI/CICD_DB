CREATE TABLE [SourceHistory].[BC_Burnaby_Legal_Parcels] (
    [Code]         VARCHAR (200)  NULL,
    [LOT_ID]       NVARCHAR (MAX) NULL,
    [LTO_PID]      NVARCHAR (MAX) NULL,
    [ROLL_NUMBER]  NVARCHAR (MAX) NULL,
    [ADDRESS]      NVARCHAR (MAX) NULL,
    [PLAN]         NVARCHAR (MAX) NULL,
    [STRATA]       NVARCHAR (MAX) NULL,
    [SHAPE_Length] NVARCHAR (MAX) NULL,
    [HistEndDate]  DATETIME       NULL,
    [IsDuplicate]  BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Burnaby_Legal_Parcels_Code]
    ON [SourceHistory].[BC_Burnaby_Legal_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Burnaby_Legal_Parcels_HistEndDate]
    ON [SourceHistory].[BC_Burnaby_Legal_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

