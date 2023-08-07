CREATE TABLE [SourceHistory].[MB_Winnipeg_Assessment_Parcels] (
    [Code]                    VARCHAR (200)  NULL,
    [Roll Number]             NVARCHAR (MAX) NULL,
    [Street Number]           NVARCHAR (MAX) NULL,
    [Unit Number]             NVARCHAR (MAX) NULL,
    [Street Direction]        NVARCHAR (MAX) NULL,
    [Street Name]             NVARCHAR (MAX) NULL,
    [Street Type]             NVARCHAR (MAX) NULL,
    [Neighbourhood Area]      NVARCHAR (MAX) NULL,
    [Total Living Area]       NVARCHAR (MAX) NULL,
    [Building Type]           NVARCHAR (MAX) NULL,
    [Basement]                NVARCHAR (MAX) NULL,
    [Basement Finish]         NVARCHAR (MAX) NULL,
    [Year Built]              NVARCHAR (MAX) NULL,
    [Rooms]                   NVARCHAR (MAX) NULL,
    [Air Conditioning]        NVARCHAR (MAX) NULL,
    [Fire Place]              NVARCHAR (MAX) NULL,
    [Attached Garage]         NVARCHAR (MAX) NULL,
    [Detached Garage]         NVARCHAR (MAX) NULL,
    [Pool]                    NVARCHAR (MAX) NULL,
    [Number Floors (Condo)]   NVARCHAR (MAX) NULL,
    [Property Use Code]       NVARCHAR (MAX) NULL,
    [Assessed Land Area]      NVARCHAR (MAX) NULL,
    [Zoning]                  NVARCHAR (MAX) NULL,
    [Total Assessed Value]    NVARCHAR (MAX) NULL,
    [Current Assessment Year] NVARCHAR (MAX) NULL,
    [Location]                NVARCHAR (MAX) NULL,
    [HistEndDate]             DATETIME       NULL,
    [IsDuplicate]             BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_MB_Winnipeg_Assessment_Parcels_Code]
    ON [SourceHistory].[MB_Winnipeg_Assessment_Parcels]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_MB_Winnipeg_Assessment_Parcels_HistEndDate]
    ON [SourceHistory].[MB_Winnipeg_Assessment_Parcels]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

