﻿CREATE TABLE [StageLanding].[NS_ALL_Residential_Dwelling_Characteristics] (
    [Municipal Unit]            NVARCHAR (MAX) NULL,
    [Assessment Account Number] NVARCHAR (MAX) NULL,
    [Civic Number]              NVARCHAR (MAX) NULL,
    [Civic Additional]          NVARCHAR (MAX) NULL,
    [Civic Direction]           NVARCHAR (MAX) NULL,
    [Civic Street Name]         NVARCHAR (MAX) NULL,
    [Civic Street Suffix]       NVARCHAR (MAX) NULL,
    [Civic City Name]           NVARCHAR (MAX) NULL,
    [Living Units]              NVARCHAR (MAX) NULL,
    [Year Built]                NVARCHAR (MAX) NULL,
    [Square Foot Living Area]   NVARCHAR (MAX) NULL,
    [Style]                     NVARCHAR (MAX) NULL,
    [Bedrooms]                  NVARCHAR (MAX) NULL,
    [Bathrooms]                 NVARCHAR (MAX) NULL,
    [Under Construction]        NVARCHAR (MAX) NULL,
    [Construction Grade]        NVARCHAR (MAX) NULL,
    [Finished Basement]         NVARCHAR (MAX) NULL,
    [Garage]                    NVARCHAR (MAX) NULL,
    [Y Map Coordinate]          NVARCHAR (MAX) NULL,
    [X Map Coordinate]          NVARCHAR (MAX) NULL,
    [Map Coordinates]           NVARCHAR (MAX) NULL,
    [SourceID]                  INT            IDENTITY (1, 1) NOT NULL,
    [Code]                      VARCHAR (200)  NULL
);
