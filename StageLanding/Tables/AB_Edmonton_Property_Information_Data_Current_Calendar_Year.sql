CREATE TABLE [StageLanding].[AB_Edmonton_Property_Information_Data_Current_Calendar_Year] (
    [Account Number]    NVARCHAR (MAX) NULL,
    [Suite]             NVARCHAR (MAX) NULL,
    [House Number]      NVARCHAR (MAX) NULL,
    [Street Name]       NVARCHAR (MAX) NULL,
    [Legal Description] NVARCHAR (MAX) NULL,
    [Zoning]            NVARCHAR (MAX) NULL,
    [Lot Size]          NVARCHAR (MAX) NULL,
    [Actual Year Built] NVARCHAR (MAX) NULL,
    [Neighbourhood ID]  NVARCHAR (MAX) NULL,
    [Neighbourhood]     NVARCHAR (MAX) NULL,
    [Ward]              NVARCHAR (MAX) NULL,
    [Latitude]          NVARCHAR (MAX) NULL,
    [Longitude]         NVARCHAR (MAX) NULL,
    [SourceID]          INT            IDENTITY (1, 1) NOT NULL,
    [Code]              VARCHAR (200)  NULL
);

