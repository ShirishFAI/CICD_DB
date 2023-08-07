CREATE TABLE [StageLanding].[AB_Edmonton_City_of_Edmonton_Business_Licenses] (
    [Category]                  NVARCHAR (MAX) NULL,
    [Trade Name]                NVARCHAR (MAX) NULL,
    [Address]                   NVARCHAR (MAX) NULL,
    [Licence Number]            NVARCHAR (MAX) NULL,
    [Licence Status]            NVARCHAR (MAX) NULL,
    [Issue Date]                NVARCHAR (MAX) NULL,
    [Expiry Date]               NVARCHAR (MAX) NULL,
    [Business Improvement Area] NVARCHAR (MAX) NULL,
    [Neighbourhood ID]          NVARCHAR (MAX) NULL,
    [Neighbourhood]             NVARCHAR (MAX) NULL,
    [Ward]                      NVARCHAR (MAX) NULL,
    [Latitude]                  NVARCHAR (MAX) NULL,
    [Longitude]                 NVARCHAR (MAX) NULL,
    [Count]                     NVARCHAR (MAX) NULL,
    [Location]                  NVARCHAR (MAX) NULL,
    [SourceID]                  INT            IDENTITY (1, 1) NOT NULL,
    [Code]                      VARCHAR (200)  NULL
);

