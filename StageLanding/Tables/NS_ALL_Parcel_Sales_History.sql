CREATE TABLE [StageLanding].[NS_ALL_Parcel_Sales_History] (
    [Municipal Unit]            NVARCHAR (MAX) NULL,
    [Assessment Account Number] NVARCHAR (MAX) NULL,
    [Civic Number]              NVARCHAR (MAX) NULL,
    [Civic Additional]          NVARCHAR (MAX) NULL,
    [Civic Direction]           NVARCHAR (MAX) NULL,
    [Civic Street Name]         NVARCHAR (MAX) NULL,
    [Civic Suffix]              NVARCHAR (MAX) NULL,
    [Civic City Name]           NVARCHAR (MAX) NULL,
    [Sale Price]                NVARCHAR (MAX) NULL,
    [Sale Date]                 NVARCHAR (MAX) NULL,
    [Parcels In Sale]           NVARCHAR (MAX) NULL,
    [Y Map Coordinate]          NVARCHAR (MAX) NULL,
    [X Map Coordinate]          NVARCHAR (MAX) NULL,
    [Map Coordinates]           NVARCHAR (MAX) NULL,
    [SourceID]                  INT            IDENTITY (1, 1) NOT NULL,
    [Code]                      VARCHAR (200)  NULL
);

