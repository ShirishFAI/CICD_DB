CREATE TABLE [StageProcessing].[BC_Surrey_Property_Detail_Listing_2017] (
    [SourceID]          INT             NULL,
    [Code]              VARCHAR (200)   NULL,
    [UNIT]              VARCHAR (100)   NULL,
    [HOUSE]             VARCHAR (100)   NULL,
    [STREET]            VARCHAR (200)   NULL,
    [POSTAL_CODE]       VARCHAR (50)    NULL,
    [PID]               VARCHAR (50)    NULL,
    [FOLIO]             VARCHAR (200)   NULL,
    [PLAN_NUMBER]       VARCHAR (100)   NULL,
    [LOT_SIZE]          DECIMAL (17, 2) NULL,
    [LEGAL_DESCRIPTION] VARCHAR (4000)  NULL,
    [ZONE]              VARCHAR (400)   NULL,
    [ZONE_DESCRIPTION]  VARCHAR (400)   NULL,
    [ASSESSMENT_YEAR]   INT             NULL,
    [GROSS_ASSESSMENT]  DECIMAL (17, 2) NULL,
    [Latitude]          VARCHAR (50)    NULL,
    [Longitude]         VARCHAR (50)    NULL,
    [LOT_UNITOFMEASURE] VARCHAR (50)    NULL,
    [ActionType]        CHAR (1)        NULL,
    [IsDuplicate]       BIT             NULL
);

