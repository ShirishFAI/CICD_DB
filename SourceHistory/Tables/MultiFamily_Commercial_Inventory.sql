CREATE TABLE [SourceHistory].[MultiFamily_Commercial_Inventory] (
    [Code]                VARCHAR (200)  NULL,
    [Jurisdiction]        NVARCHAR (MAX) NULL,
    [Roll Number]         NVARCHAR (MAX) NULL,
    [Year Built]          NVARCHAR (MAX) NULL,
    [Number of Storeys]   NVARCHAR (MAX) NULL,
    [Gross Leasable Area] NVARCHAR (MAX) NULL,
    [ParkingTotal]        NVARCHAR (MAX) NULL,
    [ParkingType]         NVARCHAR (MAX) NULL,
    [NumberOfUnits]       NVARCHAR (MAX) NULL,
    [NumberOfBedrooms]    NVARCHAR (MAX) NULL,
    [Gross Building Area] NVARCHAR (MAX) NULL,
    [Total Balcony Area]  NVARCHAR (MAX) NULL,
    [Mezzanine Area]      NVARCHAR (MAX) NULL,
    [Type of Heating]     NVARCHAR (MAX) NULL,
    [Elevators]           NVARCHAR (MAX) NULL,
    [Other Buildings]     NVARCHAR (MAX) NULL,
    [School District]     NVARCHAR (MAX) NULL,
    [Zoning]              NVARCHAR (MAX) NULL,
    [HashBytes]           BINARY (64)    NULL,
    [HistEndDate]         DATETIME       NULL,
    [IsDuplicate]         BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_MultiFamily_Commercial_Inventory_Code]
    ON [SourceHistory].[MultiFamily_Commercial_Inventory]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_MultiFamily_Commercial_Inventory_HistEndDate]
    ON [SourceHistory].[MultiFamily_Commercial_Inventory]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

