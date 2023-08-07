CREATE TABLE [StageLanding].[MB_Brandon_Assessment_File] (
    [ROLL_NUMBER]             NVARCHAR (MAX) NULL,
    [STREET_ADDRESS]          NVARCHAR (MAX) NULL,
    [PROVINCIAL_SCHOOL]       NVARCHAR (MAX) NULL,
    [BRANDON_SCHOOL_DIVISION] NVARCHAR (MAX) NULL,
    [GENERAL_MUNICIPAL]       NVARCHAR (MAX) NULL,
    [LOCAL_IMPROVEMENTS]      NVARCHAR (MAX) NULL,
    [GROSS_TAX]               NVARCHAR (MAX) NULL,
    [HOG_AMOUNT_CLAIMED]      NVARCHAR (MAX) NULL,
    [NET_TAX]                 NVARCHAR (MAX) NULL,
    [TAX_YEAR]                NVARCHAR (MAX) NULL,
    [SourceID]                INT            IDENTITY (1, 1) NOT NULL,
    [Code]                    VARCHAR (200)  NULL
);

