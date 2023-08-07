﻿CREATE TABLE [StageProcessingErr].[NS_ALL_Parcel_Sales_History] (
    [SourceID]                  INT            NULL,
    [Code]                      VARCHAR (200)  NULL,
    [ErrorStatusId]             TINYINT        NULL,
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
    [Y Map Coordinate]          NVARCHAR (MAX) NULL,
    [X Map Coordinate]          NVARCHAR (MAX) NULL,
    [Map Coordinates]           NVARCHAR (MAX) NULL
);

