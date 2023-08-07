CREATE TABLE [StageProcessing].[BC_Nanaimo_BusinessLicences] (
    [SourceID]             INT            NULL,
    [Code]                 VARCHAR (200)  NULL,
    [Licence]              NVARCHAR (510) NULL,
    [CivicAddressUnit]     VARCHAR (100)  NULL,
    [CivicAddressHouse]    VARCHAR (100)  NULL,
    [CivicAddressStreet]   VARCHAR (200)  NULL,
    [CivicAddressCity]     VARCHAR (200)  NULL,
    [CivicAddressProvince] VARCHAR (50)   NULL,
    [BusinessDescription]  VARCHAR (200)  NULL,
    [EDO_NAICS_Desc]       VARCHAR (500)  NULL,
    [NAICSCategory]        VARCHAR (200)  NULL,
    [LEGAL_ID]             NVARCHAR (510) NULL,
    [Latitude]             VARCHAR (50)   NULL,
    [Longitude]            VARCHAR (50)   NULL,
    [ActionType]           CHAR (1)       NULL,
    [IsDuplicate]          BIT            NULL
);

