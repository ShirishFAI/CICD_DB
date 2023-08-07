CREATE TABLE [SourceHistory].[BC_ColumbiaShuwap_Property] (
    [Code]        VARCHAR (200)  NULL,
    [IDParcel]    NVARCHAR (MAX) NULL,
    [PID]         NVARCHAR (MAX) NULL,
    [Legal_Plan]  NVARCHAR (MAX) NULL,
    [Land_Distr]  NVARCHAR (MAX) NULL,
    [Legal_Desc]  NVARCHAR (MAX) NULL,
    [Roll]        NVARCHAR (MAX) NULL,
    [House_Type]  NVARCHAR (MAX) NULL,
    [Address]     NVARCHAR (MAX) NULL,
    [Zoning]      NVARCHAR (MAX) NULL,
    [Last_Sale_]  NVARCHAR (MAX) NULL,
    [Last_Sale1]  NVARCHAR (MAX) NULL,
    [Lot_Dimens]  NVARCHAR (MAX) NULL,
    [Lot_Area]    NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_ColumbiaShuwap_Property_Code]
    ON [SourceHistory].[BC_ColumbiaShuwap_Property]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_ColumbiaShuwap_Property_HistEndDate]
    ON [SourceHistory].[BC_ColumbiaShuwap_Property]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

