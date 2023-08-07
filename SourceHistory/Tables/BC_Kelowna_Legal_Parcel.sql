CREATE TABLE [SourceHistory].[BC_Kelowna_Legal_Parcel] (
    [Code]        VARCHAR (200)  NULL,
    [KID]         NVARCHAR (MAX) NULL,
    [plan_no]     NVARCHAR (MAX) NULL,
    [str_unit]    NVARCHAR (MAX) NULL,
    [str_num]     NVARCHAR (MAX) NULL,
    [str_dir]     NVARCHAR (MAX) NULL,
    [city]        NVARCHAR (MAX) NULL,
    [postal_code] NVARCHAR (MAX) NULL,
    [strname]     NVARCHAR (MAX) NULL,
    [strtype]     NVARCHAR (MAX) NULL,
    [HistEndDate] DATETIME       NULL,
    [IsDuplicate] BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_BC_Kelowna_Legal_Parcel_Code]
    ON [SourceHistory].[BC_Kelowna_Legal_Parcel]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_BC_Kelowna_Legal_Parcel_HistEndDate]
    ON [SourceHistory].[BC_Kelowna_Legal_Parcel]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

