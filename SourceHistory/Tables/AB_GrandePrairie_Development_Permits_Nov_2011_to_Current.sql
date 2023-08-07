CREATE TABLE [SourceHistory].[AB_GrandePrairie_Development_Permits_Nov_2011_to_Current] (
    [Code]             VARCHAR (200)  NULL,
    [PermitNumber]     NVARCHAR (MAX) NULL,
    [Type]             NVARCHAR (MAX) NULL,
    [attIssueDate]     NVARCHAR (MAX) NULL,
    [ROLLNUMBER]       NVARCHAR (MAX) NULL,
    [FULLADDRESS]      NVARCHAR (MAX) NULL,
    [MUNICIPALITY]     NVARCHAR (MAX) NULL,
    [NEIGHBOURHOOD]    NVARCHAR (MAX) NULL,
    [LEGALDESCRIPTION] NVARCHAR (MAX) NULL,
    [AREAUNITS]        NVARCHAR (MAX) NULL,
    [HistEndDate]      DATETIME       NULL,
    [IsDuplicate]      BIT            NULL
);


GO
CREATE CLUSTERED INDEX [CI_SourceHistory_AB_GrandePrairie_Development_Permits_Nov_2011_to_Current_Code]
    ON [SourceHistory].[AB_GrandePrairie_Development_Permits_Nov_2011_to_Current]([Code] ASC) WITH (FILLFACTOR = 80);


GO
CREATE NONCLUSTERED INDEX [NCI_SourceHistory_AB_GrandePrairie_Development_Permits_Nov_2011_to_Current_HistEndDate]
    ON [SourceHistory].[AB_GrandePrairie_Development_Permits_Nov_2011_to_Current]([HistEndDate] ASC) WITH (FILLFACTOR = 80);

