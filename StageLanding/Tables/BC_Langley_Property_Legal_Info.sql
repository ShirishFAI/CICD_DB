CREATE TABLE [StageLanding].[BC_Langley_Property_Legal_Info] (
    [X]                   NVARCHAR (MAX) NULL,
    [Y]                   NVARCHAR (MAX) NULL,
    [Unit]                NVARCHAR (MAX) NULL,
    [House]               NVARCHAR (MAX) NULL,
    [Street]              NVARCHAR (MAX) NULL,
    [GISLink]             NVARCHAR (MAX) NULL,
    [Folio]               NVARCHAR (MAX) NULL,
    [PID]                 NVARCHAR (MAX) NULL,
    [Property_Number]     NVARCHAR (MAX) NULL,
    [Legal_Type]          NVARCHAR (MAX) NULL,
    [Lot]                 NVARCHAR (MAX) NULL,
    [Plan_Number]         NVARCHAR (MAX) NULL,
    [Legal_Descr]         NVARCHAR (MAX) NULL,
    [LotSize_NotVerified] NVARCHAR (MAX) NULL,
    [Lot_UnitOfMeasure]   NVARCHAR (MAX) NULL,
    [Latitude]            NVARCHAR (MAX) NULL,
    [Longitude]           NVARCHAR (MAX) NULL,
    [FID]                 NVARCHAR (MAX) NULL,
    [SourceID]            INT            IDENTITY (1, 1) NOT NULL,
    [Code]                VARCHAR (200)  NULL
);

