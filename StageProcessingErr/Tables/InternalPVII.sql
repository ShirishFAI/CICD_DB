﻿CREATE TABLE [StageProcessingErr].[InternalPVII] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [ErrorStatusId]    TINYINT        NULL,
    [LegalDescription] NVARCHAR (MAX) NULL,
    [GVSEligible]      NVARCHAR (510) NULL,
    [LoanAmt]          NVARCHAR (510) NULL,
    [LendingValue]     NVARCHAR (510) NULL,
    [GuaranteeValue]   NVARCHAR (510) NULL,
    [UnitNumber]       NVARCHAR (510) NULL,
    [StreetNumber]     NVARCHAR (510) NULL,
    [StreetName]       NVARCHAR (510) NULL,
    [StreetType]       NVARCHAR (510) NULL,
    [StreetDirection]  NVARCHAR (510) NULL,
    [Province]         NVARCHAR (510) NULL,
    [City]             NVARCHAR (510) NULL,
    [AddressId]        NVARCHAR (510) NULL,
    [Country]          NVARCHAR (510) NULL,
    [PostalCode]       NVARCHAR (510) NULL
);

