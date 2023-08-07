CREATE TABLE [SourceHistory].[MB_Winnipeg_Detailed_Building_Permit_Data] (
    [Code]                 VARCHAR (200)  NULL,
    [Permit Number]        NVARCHAR (MAX) NULL,
    [Permit Group]         NVARCHAR (MAX) NULL,
    [Street Number]        NVARCHAR (MAX) NULL,
    [Street Name]          NVARCHAR (MAX) NULL,
    [Street Type]          NVARCHAR (MAX) NULL,
    [Street Direction]     NVARCHAR (MAX) NULL,
    [Unit Number]          NVARCHAR (MAX) NULL,
    [Neighbourhood Number] NVARCHAR (MAX) NULL,
    [Neighbourhood Name]   NVARCHAR (MAX) NULL,
    [Community]            NVARCHAR (MAX) NULL,
    [Location]             NVARCHAR (MAX) NULL,
    [HistEndDate]          DATETIME       NULL,
    [IsDuplicate]          BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_MB_Winnipeg_Detailed_Building_Permit_Data_Code]
    ON [SourceHistory].[MB_Winnipeg_Detailed_Building_Permit_Data]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_MB_Winnipeg_Detailed_Building_Permit_Data_HistEndDate]
    ON [SourceHistory].[MB_Winnipeg_Detailed_Building_Permit_Data]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

