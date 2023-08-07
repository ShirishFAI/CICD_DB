CREATE TABLE [StageLanding].[NS_ALL_Assessed_Value_and_Taxable_Assessed_Value_History] (
    [Municipal Unit]            NVARCHAR (MAX) NULL,
    [Assessment Account Number] NVARCHAR (MAX) NULL,
    [Civic Number]              NVARCHAR (MAX) NULL,
    [Civic Additional]          NVARCHAR (MAX) NULL,
    [Civic Direction]           NVARCHAR (MAX) NULL,
    [Civic Street Name]         NVARCHAR (MAX) NULL,
    [Civic Street Suffix]       NVARCHAR (MAX) NULL,
    [Civic City Name]           NVARCHAR (MAX) NULL,
    [Tax Year]                  NVARCHAR (MAX) NULL,
    [Assessed Value]            NVARCHAR (MAX) NULL,
    [Taxable Assessed Value]    NVARCHAR (MAX) NULL,
    [Y Map Coordinate]          NVARCHAR (MAX) NULL,
    [X Map Coordinate]          NVARCHAR (MAX) NULL,
    [Map Coordinates]           NVARCHAR (MAX) NULL,
    [SourceID]                  INT            IDENTITY (1, 1) NOT NULL,
    [Code]                      VARCHAR (200)  NULL
);

