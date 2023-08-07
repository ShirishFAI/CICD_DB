CREATE TABLE [StageProcessing].[InternalPVII] (
    [SourceID]         INT             NULL,
    [Code]             VARCHAR (200)   NULL,
    [LegalDescription] VARCHAR (4000)  NULL,
    [GVSEligible]      VARCHAR (5)     NULL,
    [LoanAmt]          DECIMAL (17, 2) NULL,
    [LendingValue]     DECIMAL (17, 2) NULL,
    [GuaranteeValue]   DECIMAL (17, 2) NULL,
    [UnitNumber]       VARCHAR (100)   NULL,
    [StreetNumber]     VARCHAR (100)   NULL,
    [StreetName]       VARCHAR (200)   NULL,
    [StreetType]       VARCHAR (200)   NULL,
    [StreetDirection]  VARCHAR (20)    NULL,
    [Province]         VARCHAR (50)    NULL,
    [City]             VARCHAR (200)   NULL,
    [AddressId]        NVARCHAR (510)  NULL,
    [Country]          VARCHAR (30)    NULL,
    [PostalCode]       VARCHAR (50)    NULL,
    [ActionType]       CHAR (1)        NULL,
    [IsDuplicate]      BIT             NULL
);

