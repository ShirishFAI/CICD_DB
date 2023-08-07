﻿CREATE TABLE [StageProcessing].[MultiFamily_Commercial_Inventory] (
    [SourceID]            INT             NULL,
    [Code]                VARCHAR (200)   NULL,
    [Jurisdiction]        VARCHAR (10)    NULL,
    [Roll Number]         VARCHAR (200)   NULL,
    [Year Built]          INT             NULL,
    [Number of Storeys]   VARCHAR (100)   NULL,
    [Gross Leasable Area] DECIMAL (17, 2) NULL,
    [ParkingTotal]        DECIMAL (17, 2) NULL,
    [ParkingType]         VARCHAR (20)    NULL,
    [NumberOfUnits]       INT             NULL,
    [NumberOfBedrooms]    DECIMAL (17, 2) NULL,
    [Gross Building Area] DECIMAL (17, 2) NULL,
    [Total Balcony Area]  VARCHAR (20)    NULL,
    [Mezzanine Area]      VARCHAR (20)    NULL,
    [Type of Heating]     VARCHAR (50)    NULL,
    [Elevators]           VARCHAR (50)    NULL,
    [Other Buildings]     VARCHAR (20)    NULL,
    [School District]     VARCHAR (100)   NULL,
    [Zoning]              VARCHAR (400)   NULL,
    [HashBytes]           BINARY (64)     NULL,
    [ActionType]          CHAR (1)        NULL,
    [IsDuplicate]         BIT             NULL
);

