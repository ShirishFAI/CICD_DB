CREATE TABLE [StageProcessing].[BC_ColumbiaShuwap_Property] (
    [SourceID]    INT             NULL,
    [Code]        VARCHAR (200)   NULL,
    [IDParcel]    NVARCHAR (510)  NULL,
    [PID]         VARCHAR (50)    NULL,
    [Legal_Plan]  VARCHAR (100)   NULL,
    [Land_Distr]  VARCHAR (200)   NULL,
    [Legal_Desc]  VARCHAR (4000)  NULL,
    [Roll]        VARCHAR (200)   NULL,
    [House_Type]  VARCHAR (20)    NULL,
    [Address]     VARCHAR (500)   NULL,
    [Zoning]      VARCHAR (400)   NULL,
    [Last_Sale_]  DECIMAL (17, 2) NULL,
    [Last_Sale1]  DECIMAL (17, 2) NULL,
    [Lot_Dimens]  VARCHAR (50)    NULL,
    [Lot_Area]    DECIMAL (17, 2) NULL,
    [ActionType]  CHAR (1)        NULL,
    [IsDuplicate] BIT             NULL
);

