CREATE TABLE [StageLanding].[AB_GrandePrairie_Development_Permits_Nov_2011_to_Current] (
    [PermitNumber]     NVARCHAR (MAX) NULL,
    [Comments]         NVARCHAR (MAX) NULL,
    [Type]             NVARCHAR (MAX) NULL,
    [attIssueDate]     NVARCHAR (MAX) NULL,
    [ROLLNUMBER]       NVARCHAR (MAX) NULL,
    [FULLADDRESS]      NVARCHAR (MAX) NULL,
    [MUNICIPALITY]     NVARCHAR (MAX) NULL,
    [HAMLET]           NVARCHAR (MAX) NULL,
    [NEIGHBOURHOOD]    NVARCHAR (MAX) NULL,
    [LEGALDESCRIPTION] NVARCHAR (MAX) NULL,
    [PLAN_BLOCK_LOT]   NVARCHAR (MAX) NULL,
    [GISAREA]          NVARCHAR (MAX) NULL,
    [AREAUNITS]        NVARCHAR (MAX) NULL,
    [ESRI_OID]         NVARCHAR (MAX) NULL,
    [GLOBALID]         NVARCHAR (MAX) NULL,
    [SourceID]         INT            IDENTITY (1, 1) NOT NULL,
    [Code]             VARCHAR (200)  NULL
);

