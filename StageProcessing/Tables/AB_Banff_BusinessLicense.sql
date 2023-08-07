CREATE TABLE [StageProcessing].[AB_Banff_BusinessLicense] (
    [SourceID]          INT            NULL,
    [Code]              VARCHAR (200)  NULL,
    [BUSINESS_NAME]     VARCHAR (200)  NULL,
    [LICENSE_NUMBER]    NVARCHAR (510) NULL,
    [UNIT]              VARCHAR (100)  NULL,
    [STREET_NUMBER]     VARCHAR (100)  NULL,
    [STREET_NAME]       VARCHAR (200)  NULL,
    [PROPOSED_BUSINESS] VARCHAR (200)  NULL,
    [ActionType]        CHAR (1)       NULL,
    [IsDuplicate]       BIT            NULL
);

