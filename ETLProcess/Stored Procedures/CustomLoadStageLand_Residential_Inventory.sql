
  
  
        
/****************************************************************************************        
-- AUTHOR  : Srinivaas Chakravarthy T        
-- DATE   : 02/14/2022        
-- PURPOSE  : Residential Inventory External Source File - Load to StageLanding.        
-- DEPENDENCIES :         
--        
-- VERSION HISTORY:        
** ----------------------------------------------------------------------------------------        
** 02/14/2022 Shirish Waghmale Original Version        
******************************************************************************************/        
        
CREATE PROCEDURE [ETLProcess].[CustomLoadStageLand_Residential_Inventory]        
 @ExternalFileName VARCHAR(500)         
AS        
BEGIN        
 SET NOCOUNT ON;        
        
  DECLARE @Params NVARCHAR(500)='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100),@ExternalFileName VARCHAR(500),@ExternalDataSourceName VARCHAR(100)';          
  DECLARE @DynamicSQL NVARCHAR(MAX);          
  DECLARE @StageLandSchema VARCHAR(50)='StageLanding.';           
  DECLARE @ErrorSchema VARCHAR(50)='StageProcessErr.';          
  DECLARE @HistorySchema VARCHAR(50)='SourceHistory.';          
  DECLARE @ExternalDataSourceName VARCHAR(100)='DTCDataSetExternal';          
  DECLARE @TableName VARCHAR(100)='Residential_Inventory';          
  DECLARE @CustomLoad_TableName VARCHAR(100)='CustomLoad_ResidentialInventory';          
  DECLARE @ProcessName VARCHAR(100) ;          
  DECLARE @CurrentStatus VARCHAR(100) ;          
  DECLARE @RunId  INT;          
  DECLARE @IsAuditEntryExists INT;          
  DECLARE @Status VARCHAR(100);          
  DECLARE @ActiveFlag BIT;          
  DECLARE @IsAuditProcessEntryExists INT;           
  DECLARE @IsError BIT=0;          
  DECLARE @ErrorProcedure VARCHAR(100);          
  DECLARE @Exception VARCHAR(500);          
  DECLARE @DynamicSQLLarge VARCHAR(8000);          
  DECLARE @ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';          
  DECLARE @IsKeyCount INT;          
  DECLARE @ProcessID INT;          
  DECLARE @DistKeyCnt INT=0;          
  DECLARE @DistRowCnt INT=0;          
        
  SELECT           
   @ErrorProcedure= s.name+'.'+o.name           
  FROM           
   SYS.OBJECTS O           
            
  INNER JOIN SYS.SCHEMAS S           
  ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;          
          
  SELECT      
   @ProcessName=CleansedFileName      
  FROM      
   Stage.ExternalFileslist      
  WHERE      
   FileName=@ExternalFileName;          
            
          
  SET @TableName=@ProcessName;          
          
  SELECT          
   @ActiveFlag = COUNT(1)          
  FROM           
   ETLProcess.ETLProcess          
          
   INNER JOIN ETLProcess.ETLProcessCategory          
   ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId          
  WHERE          
   ETLProcess.ProcessName = @ProcessName          
   AND ETLProcess.ActiveFlag =1          
   AND ETLProcessCategory.ActiveFlag=1          
   AND ISNULL(IsSourceSpecificLoad,0)=1;         
         
      
      
         
 IF ISNULL(@ActiveFlag,0)=1          
 BEGIN           
            
 EXEC ETLProcess.AuditLog          
 @ProcessCategory = @ProcessCategory          
 , @Phase = 'ProcessHistory'          
 , @ProcessName =@ProcessName          
 , @Stage ='Get RunId for Loading External Files'          
 , @Status = 'InProgress'          
 , @CurrentStatus = 'Started' ;          
     
 SELECT            
 @RunId = AuditProcessCategory.RunId          
 FROM          
 ETLProcess.ETLProcessCategory ProcessCategory          
     
 INNER JOIN ETLAudit.ETLProcessCategory AuditProcessCategory          
 ON ProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId            
     
 INNER JOIN ETLProcess.ETLStatus          
 ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus          
 WHERE          
 ETLStatus.Status NOT IN('Completed','Hold')          
 AND ProcessCategory.ProcessCategoryName=@ProcessCategory;          
     
 EXEC ETLProcess.AuditLog          
 @ProcessCategory = @ProcessCategory          
 , @Phase = 'ProcessHistory'          
 , @ProcessName = @ProcessName          
 , @Stage ='Got RunId for Loading External Files'          
 , @Status = 'Completed'          
 , @CurrentStatus = 'Completed'           
 , @Inserts=0;           
     
 SELECT          
 @IsAuditEntryExists= COUNT(1)           
 FROM       
 ETLProcess.ETLProcess               
     
 INNER JOIN  ETLAudit.ETLProcess AuditProcess          
 ON ETLProcess.ProcessId = AuditProcess.ProcessId          
     
 INNER JOIN ETLProcess.ETLStatus          
 ON ETLStatus.StatusId = AuditProcess.CurrentStatus          
 WHERE           
 ETLProcess.ProcessName=@ProcessName            
 AND AuditProcess.RunId=@RunId          
 AND CurrentStage='Landing';          
     
    
 IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0             
 EXEC ETLProcess.AuditLog          
  @ProcessCategory = 'DTC_ExternalSource_ETL'          
 , @Phase = 'Process'         
 , @ProcessName = @ProcessName          
 , @Stage = 'Landing'          
 , @Status = 'InProgress'          
 , @CurrentStatus = 'Started' ;           
     
 SELECT           
 @CurrentStatus = ETLStatus.Status          
 FROM          
 ETLAudit.ETLProcess AuditProcess          
     
 INNER JOIN ETLProcess.ETLProcess          
 ON AuditProcess.ProcessId = ETLProcess.ProcessId          
     
 INNER JOIN ETLProcess.ETLStatus          
 ON ETLStatus.StatusId = AuditProcess.CurrentStatus          
 WHERE          
 RunId=@RunId          
 AND ETLProcess.ProcessName = @ProcessName          
 AND AuditProcess.CurrentStage = 'Landing';          
     
 SELECT           
 @ProcessID=ProcessID          
 FROM          
 ETLProcess.ETLProcess          
 WHERE          
 ProcessName=@ProcessName        
         
           
    IF ISNULL(@CurrentStatus,'') NOT IN('Completed','Hold')          
    BEGIN           
              
    IF OBJECT_ID( @StageLandSchema+@TableName, 'U') IS NOT NULL          
    BEGIN          
          
        BEGIN TRY          
        EXEC ETLProcess.AuditLog          
         @ProcessCategory = @ProcessCategory          
        , @Phase = 'ProcessHistory'          
        , @ProcessName = @ProcessName          
        , @Stage ='Start loading to StageLanding'          
        , @Status = 'InProgress'          
        , @CurrentStatus = 'Started' ;          
            
       IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES T WHERE T.TABLE_NAME = 'CustomLoad_ResidentialInventory' AND T.TABLE_SCHEMA = REPLACE(@StageLandSchema,'.',''))          
       BEGIN          
          
        
  CREATE TABLE StageLanding.CustomLoad_ResidentialInventory        
  (        
   [Area] nvarchar(500)         
  ,  [Jurisdiction] nvarchar(500)         
  ,  [Roll Number] nvarchar(500)         
  ,  [MB Manual Class] nvarchar(500)         
  ,  [Placeholder 1] nvarchar(500)         
  ,  [Placeholder 2] nvarchar(500)         
  ,  [MB Year Built] nvarchar(500)         
  ,  [MB Effective Year] nvarchar(500)         
  ,  [MB Total Finished Area] nvarchar(500)         
  ,  [MB Num Storeys] nvarchar(500)         
  ,  [Num Full Baths] nvarchar(500)         
  ,  [Num 3-Piece Baths] nvarchar(500)         
  ,  [Num 2-Piece Baths] nvarchar(500)         
  ,  [Num Bedrooms] nvarchar(500)         
  ,  [Num Dens] nvarchar(500)         
  ,  [Placeholder 3] nvarchar(500)         
  ,  [Type of Foundation] nvarchar(500)         
  ,  [Num Multi Garage] nvarchar(500)         
  ,  [Num Single Garage] nvarchar(500)         
  ,  [Num Carport] nvarchar(500)         
  ,  [Land Characteristic Code 1] nvarchar(500)         
  ,  [Land Characteristic Code 2] nvarchar(500)         
  ,  [Land Characteristic Code 3] nvarchar(500)         
  ,  [Land Characteristic Code 4] nvarchar(500)         
  ,  [Land Characteristic Code 5] nvarchar(500)         
  ,  [Land Characteristic Code 6] nvarchar(500)         
  ,  [Pool Flag] nvarchar(500)         
  ,  [Other Building Flag] nvarchar(500)         
  ,  [Land Metric Flag] nvarchar(500)         
  ,  [Land Width] nvarchar(500)         
  ,  [Land Depth] nvarchar(500)         
  ,  [Land Sq Measure] nvarchar(500)         
  ,  [Land Area] nvarchar(500)         
  ,  [Placeholder 4] nvarchar(500)         
  ,  [Inc Unit of Measure Value] nvarchar(500)         
  ,  [Inc Unit of Measure Code] nvarchar(500)         
  ,  [Placeholder 5] nvarchar(500)         
  ,  [Inc Floor Num] nvarchar(500)         
  ,  [Inc Effective Year] nvarchar(500)         
  ,  [Plumbing Nil Flag] nvarchar(500)         
  ,  [Basement Finish Area] nvarchar(500)         
  ,  [Basement Total Area] nvarchar(500)         
  ,  [Deck Sq Footage] nvarchar(500)         
  ,  [Deck Sq Footage Covered] nvarchar(500)         
  ,  [Placeholder 6] nvarchar(500)         
  ,  [Fireplace Num 1] nvarchar(500)         
  ,  [Placeholder 7] nvarchar(500)         
  ,  [Fireplace Num 2] nvarchar(500)         
  ,  [Placeholder 8] nvarchar(500)         
  ,  [Fireplace Num 3] nvarchar(500)         
  ,  [Placeholder 9] nvarchar(500)         
  ,  [Fireplace Num 4] nvarchar(500)         
  ,  [Placeholder 10] nvarchar(500)         
  ,  [Fireplace Num 5] nvarchar(500)         
  ,  [First Floor Area] nvarchar(500)         
  ,  [Second Floor Area] nvarchar(500)         
  ,  [Third Floor Area] nvarchar(500)         
  ,  [School District] nvarchar(500)         
  ,  [Zoning] nvarchar(500)         
     
  )        
     
  END;        
     
  SET @DynamicSQL=N'';          
     
   IF @ExternalFileName LIKE '%.txt'          
   BEGIN          
   SET @DynamicSQL = @DynamicSQL+          
    ' BULK INSERT '+@StageLandSchema+@CustomLoad_TableName          
    + ' FROM '''+@ExternalFileName+''''          
    + ' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FIRSTROW = 1,  FIELDTERMINATOR = ''","'', ROWTERMINATOR=''0x0a''); ';          
     
   SET @Params ='@StageLandSchema VARCHAR(50),@CustomLoad_TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)' ;           
   EXECUTE sp_executesql  @DynamicSQL,@Params,@StageLandSchema,@CustomLoad_TableName,@ExternalFileName,@ExternalDataSourceName;          
   END          
       
   ELSE IF @ExternalFileName LIKE '%.csv'          
   BEGIN          
   SET @DynamicSQL = @DynamicSQL+          
    ' BULK INSERT '+@StageLandSchema+@CustomLoad_TableName          
    + ' FROM '''+@ExternalFileName+''''          
    + ' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,  FIELDQUOTE=''"''); ';          
    --+ ' WITH ( DATA_SOURCE = '''+@ExternalDataSourceName+''', FORMAT=''csv'', FIRSTROW = 2,  FIELDTERMINATOR = '','', ROWTERMINATOR=''0x0a''); ';          
      
       
   SET @Params ='@StageLandSchema VARCHAR(50),@CustomLoad_TableName VARCHAR(100),@ExternalFileName VARCHAR(100),@ExternalDataSourceName VARCHAR(100)';          
   EXECUTE sp_executesql  @DynamicSQL,@Params,@StageLandSchema,@CustomLoad_TableName, @ExternalFileName,@ExternalDataSourceName;              
     
   END;          
         
         
        
  DROP TABLE IF EXISTS #ResidentialInventory        
        
        
  SELECT        
  REPLACE([Area] ,'"','') AS [Area]        
  ,  [Jurisdiction] AS JurCode         
  ,  [Roll Number] AS ARN         
  ,  [MB Manual Class]         
  ,  [Placeholder 1]         
  ,  [Placeholder 2]         
  ,  [MB Year Built]         
  ,  [MB Effective Year]         
  ,  [MB Total Finished Area]         
  ,  [MB Num Storeys]         
  ,  CAST(ISNULL(NULLIF([Num Full Baths],''),0) AS INT)+ CAST(ISNULL(NULLIF([Num 2-Piece Baths],''),0) AS INT)+ CAST(ISNULL(NULLIF([Num 3-Piece Baths],''),0) AS INT) AS NumberOfWashroom        
  ,  [Num Bedrooms]         
  ,  [Num Dens]         
  ,  [Placeholder 3]         
  ,  [Type of Foundation]         
  ,  [Num Multi Garage] AS Multi_Garage        
  ,  [Num Single Garage] AS Single_Garage        
  ,  [Num Carport] AS Carport        
  ,  [Land Characteristic Code 1]         
  ,  [Land Characteristic Code 2]         
  ,  [Land Characteristic Code 3]           ,  [Land Characteristic Code 4]         
  ,  [Land Characteristic Code 5]         
  ,  [Land Characteristic Code 6]         
  ,  [Pool Flag]         
  ,  [Other Building Flag]         
  ,  [Land Metric Flag]         
  ,  [Land Width]         
  ,  [Land Depth]         
  ,  [Land Sq Measure]         
  ,  [Land Area]         
  ,  [Placeholder 4]         
  ,  [Inc Unit of Measure Value]         
  ,  [Inc Unit of Measure Code]         
  ,  [Placeholder 5]         
  ,  [Inc Floor Num]         
  ,  [Inc Effective Year]         
  ,  [Plumbing Nil Flag]         
  ,  [Basement Finish Area]         
  ,  [Basement Total Area]         
  ,  [Deck Sq Footage]         
  ,  [Deck Sq Footage Covered]         
  ,  [Placeholder 6]         
  ,  CASE         
  WHEN  [Fireplace Num 1]  IS NOT NULL AND  CAST(ISNULL(NULLIF([Fireplace Num 1],''),0) AS INT) > 0 THEN 'yes'        
     ELSE 'no'         
  END  AS FirePlace        
  ,  [Placeholder 7]         
  ,  [Fireplace Num 2]         
  ,  [Placeholder 8]         
  ,  [Fireplace Num 3]         
  ,  [Placeholder 9]         
  ,  [Fireplace Num 4]         
  ,  [Placeholder 10]         
  ,  [Fireplace Num 5]       
  ,  [First Floor Area]         
  ,  [Second Floor Area]         
  ,  [Third Floor Area]         
  ,  [School District]         
  ,  REPLACE([Zoning] ,'"','') AS [Zoning]        
        
  INTO #ResidentialInventory        
        
  FROM StageLanding.CustomLoad_ResidentialInventory;         
        
  DROP TABLE IF EXISTS #ResidentialInventory_Load;        
        
  SELECT        
  [Area]         
  ,  [JurCode]         
  ,  [ARN]         
  ,  [MB Manual Class]         
  ,  [Placeholder 1]         
  ,  [Placeholder 2]         
  ,  [MB Year Built]         
  ,  [MB Effective Year]         
  ,  [MB Total Finished Area]         
  ,  [MB Num Storeys]         
  ,  [NumberOfWashroom]        
  ,  [Num Bedrooms]         
  ,  [Num Dens]         
  ,  [Placeholder 3]         
  ,  [Type of Foundation]         
  ,  [ParkingTotal]        
  ,  CASE WHEN Rn=1 and [ParkingTotal] =0 THEN NULL  ELSE [ParkingType] END  AS  ParkingType        
  ,  [Land Characteristic Code 1]         
  ,  [Land Characteristic Code 2]         
  ,  [Land Characteristic Code 3]         
  ,  [Land Characteristic Code 4]         
  ,  [Land Characteristic Code 5]         
  ,  [Land Characteristic Code 6]         
  ,  [Pool Flag]         
  ,  [Other Building Flag]         
  ,  [Land Metric Flag]         
  ,  [Land Width]         
  ,  [Land Depth]         
  ,  [Land Sq Measure]         
  ,  [Land Area]         
  ,  [Placeholder 4]         
  ,  [Inc Unit of Measure Value]         
  ,  [Inc Unit of Measure Code]         
  ,  [Placeholder 5]         
  ,  [Inc Floor Num]         
  ,  [Inc Effective Year]         
  ,  [Plumbing Nil Flag]         
  ,  [Basement Finish Area]         
  ,  [Basement Total Area]         
  ,  [Deck Sq Footage]         
  ,  [Deck Sq Footage Covered]         
  ,  [Placeholder 6]         
  ,  [FirePlace]        
  ,  [Placeholder 7]         
  ,  [Fireplace Num 2]         
  ,  [Placeholder 8]         
  ,  [Fireplace Num 3]         
  ,  [Placeholder 9]         
  ,  [Fireplace Num 4]         
  ,  [Placeholder 10]         
  ,  [Fireplace Num 5]         
  ,  [First Floor Area]         
  ,  [Second Floor Area]         
  ,  [Third Floor Area]         
  ,  [School District]         
  ,  [Zoning]         
  INTO #ResidentialInventory_Load         
  FROM         
  (        
   SELECT        
  [Area]         
  ,  [JurCode]         
  ,  [ARN]         
  ,  [MB Manual Class]         
  ,  [Placeholder 1]         
  ,  [Placeholder 2]         
  ,  [MB Year Built]         
  ,  [MB Effective Year]         
  ,  [MB Total Finished Area]         
  ,  [MB Num Storeys]         
  ,  [NumberOfWashroom]        
  ,  [Num Bedrooms]         
  ,  [Num Dens]         
  ,  [Placeholder 3]         
  ,  [Type of Foundation]         
  ,  [ParkingTotal]        
  ,  [ParkingType]        
  ,  [Land Characteristic Code 1]         
  ,  [Land Characteristic Code 2]        
  ,  [Land Characteristic Code 3]         
  ,  [Land Characteristic Code 4]         
  ,  [Land Characteristic Code 5]         
  ,  [Land Characteristic Code 6]         
  ,  [Pool Flag]         
  ,  [Other Building Flag]         
  ,  [Land Metric Flag]         
  ,  [Land Width]         
  ,  [Land Depth]         
  ,  [Land Sq Measure]         
  ,  [Land Area]         
  ,  [Placeholder 4]         
  ,  [Inc Unit of Measure Value]         
  ,  [Inc Unit of Measure Code]         
  ,  [Placeholder 5]         
  ,  [Inc Floor Num]         
  ,  [Inc Effective Year]         
  ,  [Plumbing Nil Flag]         
  ,  [Basement Finish Area]         
  ,  [Basement Total Area]         
  ,  [Deck Sq Footage]         
  ,  [Deck Sq Footage Covered]         
  ,  [Placeholder 6]         
  ,  [FirePlace]        
  ,  [Placeholder 7]         
  ,  [Fireplace Num 2]         
  ,  [Placeholder 8]         
  ,  [Fireplace Num 3]         
  ,  [Placeholder 9]         
  ,  [Fireplace Num 4]         
  ,  [Placeholder 10]         
  ,  [Fireplace Num 5]         
  ,  [First Floor Area]         
  ,  [Second Floor Area]         
  ,  [Third Floor Area]         
  ,  [School District]         
  ,  [Zoning]        
  ,   ROW_NUMBER() OVER(PARTITION BY ARN,JurCode ORDER BY ParkingTotal DESC) as Rn        
  FROM #ResidentialInventory        
        
  UNPIVOT(        
  ParkingTotal        
  FOR ParkingType IN(Multi_Garage,Single_Garage,Carport)        
        
   ) UPivot        
   ) A        
  WHERE        
    (Rn=1 and ParkingTotal =0)        
    OR        
    (Rn > 0 AND ParkingTotal<>0)        
          
   SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';          
   SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@CustomLoad_TableName+' ;';          
   EXECUTE sp_executesql  @DynamicSQL,@Params,@StageLandSchema,@TableName ; 
        
   SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';          
      SET @DynamicSQL='INSERT INTO '+@StageLandSchema+@TableName+' ([Area],[JurCode],[ARN],[MB Manual Class],[Placeholder 1],[Placeholder 2],  
   [MB Year Built],[MB Effective Year],[MB Total Finished Area],[MB Num Storeys],[NumberOfWashroom],[Num Bedrooms],[Num Dens],[Placeholder 3],  
   [Type of Foundation],[ParkingTotal],[ParkingType],[Land Characteristic Code 1],[Land Characteristic Code 2],[Land Characteristic Code 3],  
   [Land Characteristic Code 4],[Land Characteristic Code 5],[Land Characteristic Code 6],[Pool Flag],[Other Building Flag],[Land Metric Flag],  
   [Land Width],[Land Depth],[Land Sq Measure],[Land Area],[Placeholder 4],[Inc Unit of Measure Value],[Inc Unit of Measure Code],[Placeholder 5],  
   [Inc Floor Num],[Inc Effective Year],[Plumbing Nil Flag],[Basement Finish Area],[Basement Total Area],[Deck Sq Footage],  
   [Deck Sq Footage Covered],[Placeholder 6],[FirePlace],[Placeholder 7],[Fireplace Num 2],[Placeholder 8],[Fireplace Num 3],[Placeholder 9],  
   [Fireplace Num 4],[Placeholder 10],[Fireplace Num 5],[First Floor Area],[Second Floor Area],[Third Floor Area],[School District],[Zoning])  
     
   Select * from #ResidentialInventory_Load;'          
                
      EXECUTE sp_executesql  @DynamicSQL,@Params,@StageLandSchema,@TableName ;          
          
      SET @Params ='@StageLandSchema VARCHAR(50),@CustomLoad_TableName VARCHAR(100)';          
      SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@CustomLoad_TableName+' ;';          
      EXECUTE sp_executesql  @DynamicSQL,@Params,@StageLandSchema,@CustomLoad_TableName ;          
                
      EXEC ETLProcess.AuditLog          
       @ProcessCategory = @ProcessCategory          
      , @Phase = 'ProcessHistory'          
      , @ProcessName = @ProcessName          
      , @Stage ='Completed loading to StageLanding'          
      , @Status = 'Completed'          
      , @CurrentStatus = 'Completed'          
      , @Inserts = @@ROWCOUNT;          
          
      EXEC ETLProcess.AuditLog          
       @ProcessCategory = @ProcessCategory          
      , @Phase = 'Process'          
      , @ProcessName = @ProcessName          
      , @Status = 'Completed'          
      , @CurrentStatus = 'Completed'          
      , @Stage = 'Landing';          
          
     END TRY           
          
     BEGIN CATCH          
      UPDATE Stage.ExternalFileslist SET IsError=1 WHERE FileName=@ExternalFileName;          
          
       SET @IsError=1          
       SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)';          
       SET @DynamicSQL='TRUNCATE TABLE '+@StageLandSchema+@TableName+' ;';          
       EXECUTE sp_executesql  @DynamicSQL,@Params,@StageLandSchema,@TableName ;          
          
       EXEC ETLProcess.AuditLog          
        @ProcessCategory = @ProcessCategory          
       , @Phase = 'ProcessHistory'          
       , @ProcessName = @ProcessName          
       , @Stage ='Error loading to StageLanding'          
       , @Status = 'Error'          
       , @CurrentStatus = 'Error'          
       , @Inserts = 0;          
          
       EXEC ETLProcess.AuditLog          
        @ProcessCategory = @ProcessCategory          
       , @Phase = 'Process'          
       , @ProcessName = @ProcessName          
       , @Status = 'Error'          
       , @CurrentStatus = 'Error'          
       , @Stage = 'Landing';           
          
       INSERT INTO ETLProcess.ETLStoredProcedureErrors          
       (          
        ProcessCategory          
       , ProcessName          
       , ErrorNumber          
       , ErrorSeverity          
       , ErrorState          
       , ErrorProcedure          
       , ErrorLine          
       , ErrorMessage          
       , ErrorDate          
       )          
       SELECT            
        @ProcessCategory          
       , @ProcessName          
       , ERROR_NUMBER() AS ErrorNumber            
       , ERROR_SEVERITY() AS ErrorSeverity            
       , ERROR_STATE() AS ErrorState            
       , @ErrorProcedure          
       , ERROR_LINE() AS ErrorLine            
       , ERROR_MESSAGE() AS ErrorMessage          
       , GETDATE()          
                
                 
       EXEC ETLProcess.EmailNotification          
        @ProcessCategory=@ProcessCategory          
       , @ProcessName= @ProcessName          
       , @ProcessStage='Landing'          
       , @ErrorMessage='Failed to Load StageLanding'          
       , @IsError='Yes';          
      END CATCH          
   END          
  END            
  END           
          
 IF @IsError=1          
  THROW 50005, N'An error occurred while loading data to StageLanding', 1;          
END