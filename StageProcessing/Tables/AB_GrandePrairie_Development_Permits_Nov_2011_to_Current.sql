CREATE TABLE [StageProcessing].[AB_GrandePrairie_Development_Permits_Nov_2011_to_Current] (
    [SourceID]         INT            NULL,
    [Code]             VARCHAR (200)  NULL,
    [PermitNumber]     NVARCHAR (510) NULL,
    [Type]             NVARCHAR (510) NULL,
    [attIssueDate]     NVARCHAR (510) NULL,
    [ROLLNUMBER]       VARCHAR (200)  NULL,
    [FULLADDRESS]      VARCHAR (500)  NULL,
    [MUNICIPALITY]     VARCHAR (100)  NULL,
    [NEIGHBOURHOOD]    VARCHAR (200)  NULL,
    [LEGALDESCRIPTION] VARCHAR (4000) NULL,
    [AREAUNITS]        NVARCHAR (510) NULL,
    [ActionType]       CHAR (1)       NULL,
    [IsDuplicate]      BIT            NULL
);

