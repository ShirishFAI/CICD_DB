CREATE TABLE [StageProcessingErr].[AB_GrandePrairie_Development_Permits_Nov_2011_to_Current] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [ErrorStatusId]    TINYINT        NULL,
    [PermitNumber]     NVARCHAR (MAX) NULL,
    [Type]             NVARCHAR (MAX) NULL,
    [attIssueDate]     NVARCHAR (MAX) NULL,
    [ROLLNUMBER]       NVARCHAR (MAX) NULL,
    [FULLADDRESS]      NVARCHAR (MAX) NULL,
    [MUNICIPALITY]     NVARCHAR (MAX) NULL,
    [NEIGHBOURHOOD]    NVARCHAR (MAX) NULL,
    [LEGALDESCRIPTION] NVARCHAR (MAX) NULL,
    [AREAUNITS]        NVARCHAR (MAX) NULL
);

