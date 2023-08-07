CREATE TABLE [StageLanding].[BC_Nanaimo_BusinessLicences] (
    [Licence]                  NVARCHAR (MAX) NULL,
    [CivicAddressUnit]         NVARCHAR (MAX) NULL,
    [CivicAddressHouse]        NVARCHAR (MAX) NULL,
    [CivicAddressStreet]       NVARCHAR (MAX) NULL,
    [CivicAddressCity]         NVARCHAR (MAX) NULL,
    [CivicAddressProvince]     NVARCHAR (MAX) NULL,
    [BusinessDescription]      NVARCHAR (MAX) NULL,
    [EDO_NAICS_Desc]           NVARCHAR (MAX) NULL,
    [NAICSCategory]            NVARCHAR (MAX) NULL,
    [NAICSDetail]              NVARCHAR (MAX) NULL,
    [TradeName]                NVARCHAR (MAX) NULL,
    [LEGAL_ID]                 NVARCHAR (MAX) NULL,
    [GISLINK]                  NVARCHAR (MAX) NULL,
    [NAICSCategoryDescription] NVARCHAR (MAX) NULL,
    [NAICSDetailDescription]   NVARCHAR (MAX) NULL,
    [Latitude]                 NVARCHAR (MAX) NULL,
    [Longitude]                NVARCHAR (MAX) NULL,
    [approval_type_date]       NVARCHAR (MAX) NULL,
    [SourceID]                 INT            IDENTITY (1, 1) NOT NULL,
    [Code]                     VARCHAR (200)  NULL
);

