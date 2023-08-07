CREATE TABLE [StageProcessingErr].[AB_Banff_BusinessLicense] (
    [SourceID]          INT            NULL,
    [Code]              VARCHAR (200)  NULL,
    [ErrorStatusId]     TINYINT        NULL,
    [BUSINESS_NAME]     NVARCHAR (MAX) NULL,
    [LICENSE_NUMBER]    NVARCHAR (MAX) NULL,
    [UNIT]              NVARCHAR (MAX) NULL,
    [STREET_NUMBER]     NVARCHAR (MAX) NULL,
    [STREET_NAME]       NVARCHAR (MAX) NULL,
    [PROPOSED_BUSINESS] NVARCHAR (MAX) NULL
);

