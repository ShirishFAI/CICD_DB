CREATE TABLE [StageProcessingErr].[BC_Nanaimo_BusinessLicences] (
    [SourceID]             INT            NULL,
    [Code]                 VARCHAR (200)  NULL,
    [ErrorStatusId]        TINYINT        NULL,
    [Licence]              NVARCHAR (MAX) NULL,
    [CivicAddressUnit]     NVARCHAR (MAX) NULL,
    [CivicAddressHouse]    NVARCHAR (MAX) NULL,
    [CivicAddressStreet]   NVARCHAR (MAX) NULL,
    [CivicAddressCity]     NVARCHAR (MAX) NULL,
    [CivicAddressProvince] NVARCHAR (MAX) NULL,
    [BusinessDescription]  NVARCHAR (MAX) NULL,
    [EDO_NAICS_Desc]       NVARCHAR (MAX) NULL,
    [NAICSCategory]        NVARCHAR (MAX) NULL,
    [LEGAL_ID]             NVARCHAR (MAX) NULL,
    [Latitude]             NVARCHAR (MAX) NULL,
    [Longitude]            NVARCHAR (MAX) NULL
);

