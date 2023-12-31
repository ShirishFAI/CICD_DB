﻿CREATE TABLE [dbo].[Parcel_11152022] (
    [ID]                               INT             IDENTITY (1, 1) NOT NULL,
    [Code]                             VARCHAR (200)   NULL,
    [MasterAddressID]                  VARCHAR (100)   NULL,
    [PIN]                              VARCHAR (50)    NULL,
    [ProvinceCode]                     VARCHAR (50)    NULL,
    [Acreage]                          VARCHAR (50)    NULL,
    [LotDepth]                         VARCHAR (50)    NULL,
    [LotFrontage]                      VARCHAR (50)    NULL,
    [IsNativeLand]                     VARCHAR (5)     NULL,
    [IsEnergy]                         VARCHAR (5)     NULL,
    [IsVacantLand]                     VARCHAR (5)     NULL,
    [IsRenovatedLotNum]                VARCHAR (5)     NULL,
    [MetesAndBounds]                   VARCHAR (5)     NULL,
    [PrimaryProperty]                  VARCHAR (5)     NULL,
    [GVSEligible]                      VARCHAR (5)     NULL,
    [LotMeasureUnit]                   VARCHAR (50)    NULL,
    [LotSQM]                           DECIMAL (17, 2) NULL,
    [LotSQFT]                          DECIMAL (17, 2) NULL,
    [LotHA]                            DECIMAL (17, 2) NULL,
    [LandSQFT]                         DECIMAL (17, 2) NULL,
    [LotDescription]                   VARCHAR (MAX)   NULL,
    [LotSize]                          DECIMAL (17, 2) NULL,
    [LandType]                         VARCHAR (100)   NULL,
    [LandUse]                          VARCHAR (100)   NULL,
    [PlanNumber]                       VARCHAR (100)   NULL,
    [ZoningDescription]                VARCHAR (400)   NULL,
    [ZoningCode]                       VARCHAR (400)   NULL,
    [PropertyTypeCode]                 VARCHAR (200)   NULL,
    [PropertyUse]                      VARCHAR (100)   NULL,
    [Easement]                         VARCHAR (200)   NULL,
    [LegalDescription]                 VARCHAR (4000)  NULL,
    [Sequence]                         CHAR (1)        NULL,
    [DateCreatedUTC]                   DATETIME        NULL,
    [LastModifiedDateUTC]              DATETIME        NULL,
    [Data_Source_ID]                   INT             NULL,
    [Data_Source_Priority]             INT             NULL,
    [IsPartLot]                        VARCHAR (5)     NULL,
    [LegalDescriptionBlock]            VARCHAR (4000)  NULL,
    [LegalDescriptionDistrictLot]      VARCHAR (4000)  NULL,
    [LegalDescriptionExceptPlan]       VARCHAR (4000)  NULL,
    [LegalDescriptionLegalSubdivision] VARCHAR (4000)  NULL,
    [LegalDescriptionLegalText]        VARCHAR (4000)  NULL,
    [LegalDescriptionLot]              VARCHAR (4000)  NULL,
    [LegalDescriptionParcel]           VARCHAR (4000)  NULL,
    [LegalDescriptionPart1]            VARCHAR (4000)  NULL,
    [LegalDescriptionPart2]            VARCHAR (4000)  NULL,
    [LegalDescriptionPart3]            VARCHAR (4000)  NULL,
    [LegalDescriptionPart4]            VARCHAR (4000)  NULL,
    [LegalDescriptionPortion]          VARCHAR (4000)  NULL,
    [LegalDescriptionSection]          VARCHAR (4000)  NULL,
    [LegalDescriptionStrataLot]        VARCHAR (4000)  NULL,
    [LegalDescriptionSubBlock]         VARCHAR (4000)  NULL,
    [LegalDescriptionSubLot]           VARCHAR (4000)  NULL,
    [IsDuplicate]                      INT             NOT NULL
);

