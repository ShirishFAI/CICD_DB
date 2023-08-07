CREATE TABLE [StageProcessing].[MB_Brandon_Property_Info] (
    [SourceID]          INT             NULL,
    [Code]              VARCHAR (200)   NULL,
    [dROLLNMBR]         VARCHAR (200)   NULL,
    [STREET_ADDRESS]    VARCHAR (500)   NULL,
    [dZONINGCD]         VARCHAR (400)   NULL,
    [dNMBRDWELLINGS]    INT             NULL,
    [dYEARBUILT]        INT             NULL,
    [daPROPMEASFRONT_1] VARCHAR (50)    NULL,
    [dPROPMEASLOT]      DECIMAL (17, 2) NULL,
    [GROSSTX]           DECIMAL (17, 2) NULL,
    [TOTAL_VAL]         DECIMAL (17, 2) NULL,
    [ActionType]        CHAR (1)        NULL,
    [IsDuplicate]       BIT             NULL
);

