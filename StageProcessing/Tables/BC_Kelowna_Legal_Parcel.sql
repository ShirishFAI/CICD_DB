CREATE TABLE [StageProcessing].[BC_Kelowna_Legal_Parcel] (
    [SourceID]    INT            NULL,
    [Code]        VARCHAR (200)  NULL,
    [KID]         NVARCHAR (510) NULL,
    [plan_no]     VARCHAR (100)  NULL,
    [str_unit]    VARCHAR (100)  NULL,
    [str_num]     VARCHAR (100)  NULL,
    [str_dir]     VARCHAR (20)   NULL,
    [city]        VARCHAR (200)  NULL,
    [postal_code] VARCHAR (50)   NULL,
    [strname]     VARCHAR (200)  NULL,
    [strtype]     VARCHAR (200)  NULL,
    [ActionType]  CHAR (1)       NULL,
    [IsDuplicate] BIT            NULL
);

