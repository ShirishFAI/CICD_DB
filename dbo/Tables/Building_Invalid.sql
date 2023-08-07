﻿CREATE TABLE [dbo].[Building_Invalid] (
    [ID]                     INT             IDENTITY (1, 1) NOT NULL,
    [Code]                   VARCHAR (200)   NULL,
    [MasterAddressID]        VARCHAR (100)   NULL,
    [PIN]                    VARCHAR (50)    NULL,
    [ProvinceCode]           VARCHAR (50)    NULL,
    [BuildingDescription]    VARCHAR (255)   NULL,
    [BuildingFeet]           DECIMAL (17, 2) NULL,
    [BuildingHeight]         DECIMAL (17, 2) NULL,
    [BuildingLength]         DECIMAL (17, 2) NULL,
    [BuildingM2]             DECIMAL (17, 2) NULL,
    [BuildingMeasureUnit]    VARCHAR (20)    NULL,
    [BuildingMetre]          VARCHAR (20)    NULL,
    [TypeOfPermit]           VARCHAR (100)   NULL,
    [BuildingSqft]           DECIMAL (17, 2) NULL,
    [BuildingStyle]          VARCHAR (100)   NULL,
    [BuildingType]           VARCHAR (255)   NULL,
    [BuildingTypeCode]       VARCHAR (20)    NULL,
    [YearBuilt]              INT             NULL,
    [NumberOfStories]        VARCHAR (100)   NULL,
    [NumberOfUnits]          INT             NULL,
    [LivingAreaSQFT]         DECIMAL (17, 2) NULL,
    [HouseTypeCode]          VARCHAR (20)    NULL,
    [CondoLevel]             DECIMAL (17, 2) NULL,
    [CondominumClause]       VARCHAR (100)   NULL,
    [CondoPlanNumber]        DECIMAL (17, 2) NULL,
    [CondoUnitNumber]        DECIMAL (17, 2) NULL,
    [HouseArea]              VARCHAR (100)   NULL,
    [FrontDirection]         VARCHAR (20)    NULL,
    [CondoExposure]          VARCHAR (20)    NULL,
    [NumberOfBedrooms]       DECIMAL (17, 2) NULL,
    [BedroomPlus]            VARCHAR (20)    NULL,
    [BedroomString]          VARCHAR (20)    NULL,
    [NumberOfWashroom]       VARCHAR (20)    NULL,
    [Furnished]              VARCHAR (20)    NULL,
    [DenFront]               VARCHAR (20)    NULL,
    [Description1]           VARCHAR (500)   NULL,
    [Description2]           VARCHAR (500)   NULL,
    [Amenities0]             VARCHAR (20)    NULL,
    [Amenities1]             VARCHAR (20)    NULL,
    [Amenities2]             VARCHAR (20)    NULL,
    [Amenities3]             VARCHAR (20)    NULL,
    [Amenities4]             VARCHAR (20)    NULL,
    [Pool]                   VARCHAR (20)    NULL,
    [Level]                  DECIMAL (17, 2) NULL,
    [Locker]                 VARCHAR (20)    NULL,
    [MaintenanceFee]         DECIMAL (17, 2) NULL,
    [GarageType]             VARCHAR (20)    NULL,
    [ParkingType]            VARCHAR (20)    NULL,
    [Parking]                VARCHAR (20)    NULL,
    [ParkingGarage]          VARCHAR (20)    NULL,
    [ParkingText]            VARCHAR (50)    NULL,
    [ParkingTotal]           DECIMAL (17, 2) NULL,
    [UtilitiesIncluded]      VARCHAR (50)    NULL,
    [Water]                  VARCHAR (50)    NULL,
    [ConstructionMaterial]   VARCHAR (50)    NULL,
    [ConstructionStatus]     VARCHAR (20)    NULL,
    [ExteriorFinish]         VARCHAR (50)    NULL,
    [RoofMaterial]           VARCHAR (50)    NULL,
    [RoofStyle]              VARCHAR (50)    NULL,
    [Sewer]                  VARCHAR (50)    NULL,
    [FoundationType]         VARCHAR (50)    NULL,
    [AirConditioning]        VARCHAR (20)    NULL,
    [Fireplace]              VARCHAR (20)    NULL,
    [FireplaceFuel]          VARCHAR (20)    NULL,
    [FireplaceType]          VARCHAR (20)    NULL,
    [Heating]                VARCHAR (20)    NULL,
    [HeatingFuel]            VARCHAR (20)    NULL,
    [BasementType]           VARCHAR (20)    NULL,
    [Basement]               VARCHAR (20)    NULL,
    [FinishedBasement]       VARCHAR (20)    NULL,
    [Rooms_0_Desc]           VARCHAR (500)   NULL,
    [Rooms_0_Level]          DECIMAL (17, 2) NULL,
    [Rooms_0_Size]           DECIMAL (17, 2) NULL,
    [Rooms_0_Type]           VARCHAR (20)    NULL,
    [Rooms_1_Desc]           VARCHAR (500)   NULL,
    [Rooms_1_Level]          DECIMAL (17, 2) NULL,
    [Rooms_1_Size]           DECIMAL (17, 2) NULL,
    [Rooms_1_Type]           VARCHAR (20)    NULL,
    [Rooms_2_Desc]           VARCHAR (500)   NULL,
    [Rooms_2_Level]          DECIMAL (17, 2) NULL,
    [Rooms_2_Size]           DECIMAL (17, 2) NULL,
    [Rooms_2_Type]           VARCHAR (20)    NULL,
    [Rooms_3_Desc]           VARCHAR (500)   NULL,
    [Rooms_3_Level]          DECIMAL (17, 2) NULL,
    [Rooms_3_Size]           DECIMAL (17, 2) NULL,
    [Rooms_3_Type]           VARCHAR (20)    NULL,
    [Rooms_4_Desc]           VARCHAR (500)   NULL,
    [Rooms_4_Level]          DECIMAL (17, 2) NULL,
    [Rooms_4_Size]           DECIMAL (17, 2) NULL,
    [Rooms_4_Type]           VARCHAR (20)    NULL,
    [Rooms_5_Desc]           VARCHAR (500)   NULL,
    [Rooms_5_Level]          DECIMAL (17, 2) NULL,
    [Rooms_5_Size]           DECIMAL (17, 2) NULL,
    [Rooms_5_Type]           VARCHAR (20)    NULL,
    [Rooms_6_Desc]           VARCHAR (500)   NULL,
    [Rooms_6_Level]          DECIMAL (17, 2) NULL,
    [Rooms_6_Size]           DECIMAL (17, 2) NULL,
    [Rooms_6_Type]           VARCHAR (20)    NULL,
    [Rooms_7_Desc]           VARCHAR (500)   NULL,
    [Rooms_7_Level]          DECIMAL (17, 2) NULL,
    [Rooms_7_Size]           DECIMAL (17, 2) NULL,
    [Rooms_7_Type]           VARCHAR (20)    NULL,
    [Rooms_8_Desc]           VARCHAR (500)   NULL,
    [Rooms_8_Level]          DECIMAL (17, 2) NULL,
    [Rooms_8_Size]           DECIMAL (17, 2) NULL,
    [Rooms_8_Type]           VARCHAR (20)    NULL,
    [Rooms_9_Desc]           VARCHAR (500)   NULL,
    [Rooms_9_Level]          DECIMAL (17, 2) NULL,
    [Rooms_9_Size]           DECIMAL (17, 2) NULL,
    [Rooms_9_Type]           VARCHAR (20)    NULL,
    [Rooms_10_Desc]          VARCHAR (500)   NULL,
    [Rooms_10_Level]         DECIMAL (17, 2) NULL,
    [Rooms_10_Size]          DECIMAL (17, 2) NULL,
    [Rooms_10_Type]          VARCHAR (20)    NULL,
    [Rooms_11_Desc]          VARCHAR (500)   NULL,
    [Rooms_11_Level]         DECIMAL (17, 2) NULL,
    [Rooms_11_Size]          DECIMAL (17, 2) NULL,
    [Rooms_11_Type]          VARCHAR (20)    NULL,
    [IsCondo]                VARCHAR (5)     NULL,
    [AttachedGarage]         VARCHAR (20)    NULL,
    [DetachedGarage]         VARCHAR (20)    NULL,
    [Fuel]                   VARCHAR (20)    NULL,
    [Garage]                 VARCHAR (20)    NULL,
    [IsMobileHome]           VARCHAR (5)     NULL,
    [IsNewHome]              VARCHAR (5)     NULL,
    [NewHomeEasement]        VARCHAR (5)     NULL,
    [EstateTypeCode]         VARCHAR (50)    NULL,
    [OccupancyTypeCode]      VARCHAR (50)    NULL,
    [PropertyCategory]       VARCHAR (250)   NULL,
    [PropertyClassification] VARCHAR (100)   NULL,
    [DateCreatedUTC]         DATETIME        NULL,
    [LastModifiedDateUTC]    DATETIME        NULL,
    [Data_Source_ID]         INT             NULL,
    [Data_Source_Priority]   INT             NULL,
    [IsPermanentlyInvalid]   BIT             DEFAULT ((0)) NULL,
    [ReProcess]              BIT             DEFAULT ((0)) NULL,
    [InvalidRuleId]          VARCHAR (20)    NULL,
    [BasementFinishArea]     VARCHAR (20)    NULL,
    [BasementTotalArea]      VARCHAR (20)    NULL,
    [DeckSqFootage]          DECIMAL (18)    NULL,
    [DeckSqFootageCovered]   DECIMAL (18)    NULL,
    [Elevators]              VARCHAR (50)    NULL,
    [MezzanineArea]          VARCHAR (20)    NULL,
    [NumDens]                VARCHAR (20)    NULL,
    [OtherBuildingFlag]      VARCHAR (20)    NULL,
    [TotalBalconyArea]       VARCHAR (20)    NULL,
    [TypeofHeating]          VARCHAR (50)    NULL,
    [IsDuplicate]            INT             CONSTRAINT [DEFAULT_Building_Invalid_IsDuplicate] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Building_Invalid_ID] PRIMARY KEY NONCLUSTERED ([ID] ASC) WITH (FILLFACTOR = 80)
);


GO
CREATE CLUSTERED INDEX [IX_Building_Invalid_Code]
    ON [dbo].[Building_Invalid]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Building_Invalid_LastModifiedDateUTC]
    ON [dbo].[Building_Invalid]([LastModifiedDateUTC] DESC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [IX_Building_Invalid_ProvinceCode]
    ON [dbo].[Building_Invalid]([ProvinceCode] ASC) WITH (FILLFACTOR = 80);

