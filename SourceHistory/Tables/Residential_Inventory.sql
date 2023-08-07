CREATE TABLE [SourceHistory].[Residential_Inventory] (
    [Code]                    VARCHAR (200)  NULL,
    [JurCode]                 NVARCHAR (MAX) NULL,
    [ARN]                     NVARCHAR (MAX) NULL,
    [MB Year Built]           NVARCHAR (MAX) NULL,
    [MB Total Finished Area]  NVARCHAR (MAX) NULL,
    [MB Num Storeys]          NVARCHAR (MAX) NULL,
    [NumberOfWashroom]        NVARCHAR (MAX) NULL,
    [Num Bedrooms]            NVARCHAR (MAX) NULL,
    [Num Dens]                NVARCHAR (MAX) NULL,
    [Type of Foundation]      NVARCHAR (MAX) NULL,
    [ParkingTotal]            NVARCHAR (MAX) NULL,
    [ParkingType]             NVARCHAR (MAX) NULL,
    [Pool Flag]               NVARCHAR (MAX) NULL,
    [Other Building Flag]     NVARCHAR (MAX) NULL,
    [Land Width]              NVARCHAR (MAX) NULL,
    [Land Depth]              NVARCHAR (MAX) NULL,
    [Land Sq Measure]         NVARCHAR (MAX) NULL,
    [Land Area]               NVARCHAR (MAX) NULL,
    [Inc Floor Num]           NVARCHAR (MAX) NULL,
    [Basement Finish Area]    NVARCHAR (MAX) NULL,
    [Basement Total Area]     NVARCHAR (MAX) NULL,
    [Deck Sq Footage]         NVARCHAR (MAX) NULL,
    [Deck Sq Footage Covered] NVARCHAR (MAX) NULL,
    [FirePlace]               NVARCHAR (MAX) NULL,
    [Zoning]                  NVARCHAR (MAX) NULL,
    [HashBytes]               BINARY (64)    NULL,
    [HistEndDate]             DATETIME       NULL,
    [IsDuplicate]             BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_Residential_Inventory_Code]
    ON [SourceHistory].[Residential_Inventory]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_Residential_Inventory_HistEndDate]
    ON [SourceHistory].[Residential_Inventory]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

