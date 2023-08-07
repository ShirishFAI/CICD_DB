

CREATE procedure [ETLProcess].[PostEntityLoadPushReprocessRows]            
           
AS          
  SET NOCOUNT ON;  
  DECLARE @ProcessCategory VARCHAR(100)='DTC_StageEntityLoad_ETL';  
  DECLARE @ProcessName VARCHAR(100)  ='PostEntityLoadPushReprocessRows';  
  DECLARE @ProcessStage VARCHAR(100)='Push Reprocess Rows';  
  DECLARE @HistoryStage VARCHAR(200);  
  DECLARE @ErroMessage VARCHAR(100)='Error Push Reprocess Rows';  
  DECLARE @ProcessID INT;  
  DECLARE @IsAuditEntryExists INT;  
  DECLARE @RunId INT;  
  DECLARE @CurrentStatus VARCHAR(100);  
  DECLARE @IsError BIT=0;  
  DECLARE @ErrorProcedure VARCHAR(100);  
  DECLARE @CurrentRunETLProcessCategoryUTC_StartedAt datetime;  
  
  DECLARE @strSQL nvarchar(max) = N'' ;  
  
    DECLARE @InvalidEntityTableList Table(InvalidEntityTableName Varchar(50))  
  
    INSERT INTO @InvalidEntityTableList (InvalidEntityTableName)Values  
     ('dbo.Address_Invalid')  
     ,('dbo.Building_Invalid')  
     ,('dbo.Business_Invalid')  
     ,('dbo.Listing_Invalid')  
     ,('dbo.Parcel_Invalid')  
     ,('dbo.PIN_Invalid')  
     ,('dbo.Property_Invalid')  
     ,('dbo.Sales_Invalid')  
     ,('dbo.Taxation_Invalid')  
     ,('dbo.Valuation_Invalid')
	 ,('dbo.Permit_Invalid')
      
    DECLARE @InvalidEntityTableName varchar(200) ;  --Table cursor columns     
  
    SELECT    
    @RunId = AuditProcessCategory.RunId,  
    @CurrentRunETLProcessCategoryUTC_StartedAt = AuditProcessCategory.UTC_StartedAt  
   FROM  
    ETLProcess.ETLProcessCategory ProcessCategory  
   
    INNER JOIN ETLAudit.ETLProcessCategory AuditProcessCategory  
    ON ProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId    
   
    INNER JOIN ETLProcess.ETLStatus  
    ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus  
   WHERE  
    ETLStatus.Status NOT IN('Completed','Hold')  
    AND ProcessCategory.ProcessCategoryName=@ProcessCategory;  
  
  SELECT   
    @ProcessID = ETLProcess.ProcessId  
   FROM  
    ETLProcess.ETLProcess   
   
    INNER JOIN ETLProcess.ETLProcessCategory  
    ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId  
   WHERE  
    ETLProcess.ActiveFlag=1  
    AND ETLProcessCategory.ActiveFlag=1  
    AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory  
    AND ETLProcess.ProcessName=@ProcessName;  
  
     
            IF ISNULL(@ProcessID,0) > 0 AND ISNULL(@RunId,0) > 0  
    BEGIN  
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
    AND CurrentStage=@ProcessStage  
  
   IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'Process'  
    , @ProcessName = @ProcessName  
    , @Stage = @ProcessStage  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started'              
     
   SELECT   
    @CurrentStatus = ETLStatus.Status  
   FROM  
    ETLAudit.ETLProcess  
  
    INNER JOIN ETLProcess.ETLStatus  
    ON ETLStatus.StatusId = ETLProcess.CurrentStatus  
   WHERE  
    RunId=@RunId  
    AND ETLProcess.ProcessId = @ProcessId  
    AND ETLProcess.CurrentStage = @ProcessStage;  
  
    IF @CurrentStatus NOT IN('Completed','Hold')  
  
    BEGIN  
       
     DECLARE tableCursor CURSOR FOR             
      SELECT            
      InvalidEntityTableName  
     FROM   
      @InvalidEntityTableList  
   
  
       OPEN tableCursor;            
    FETCH NEXT             
    FROM            
       tableCursor INTO @InvalidEntityTableName   ;            
    WHILE @@FETCH_STATUS = 0             
    BEGIN        
     Declare @ColumnList Varchar(Max)  
  
     IF @InvalidEntityTableName='dbo.Address_Invalid'  
      BEGIN  
       SET @ColumnList=(STUFF(( SELECT DISTINCT ', ' + c.COLUMN_NAME  
              FROM  INFORMATION_SCHEMA.COLUMNS c  
              WHERE c.TABLE_SCHEMA+'.'+c.TABLE_NAME=@InvalidEntityTableName  
              AND c.COLUMN_NAME NOT IN('IsPermanentlyInvalid','ReProcess','ID','InvalidRuleId','LastModifiedDateUTC','IsMADSent','IsMADReceived','MasterAddressID','MADReceivedDateUTC','MADSentDateUTC')  
              ORDER BY ', ' + c.COLUMN_NAME ASC  
              FOR XML PATH ('')), 1, 1, ''));  
  
       SET @strSQL=N'';  
       SET @strSQL=@strSQL  
        +' INSERT INTO '+LEFT(@InvalidEntityTableName, LEN(@InvalidEntityTableName)-8)+'('+@ColumnList+',LastModifiedDateUTC, IsMADSent,IsMADReceived,MasterAddressID,MADReceivedDateUTC,MADSentDateUTC )'  
        +' SELECT '+@ColumnList+','+''''+Convert(varchar(30),@CurrentRunETLProcessCategoryUTC_StartedAt,21)+''''+', 0,NULL,NULL,NULL,NULL FROM '+@InvalidEntityTableName  
        +' WHERE ReProcess=1 AND IsPermanentlyInvalid<>1  AND CODE NOT IN(SELECT CODE FROM '+LEFT(@InvalidEntityTableName, LEN(@InvalidEntityTableName)-8)+ ');'  
        +' DELETE FROM '+@InvalidEntityTableName+' WHERE ReProcess=1 AND IsPermanentlyInvalid<>1 ;'  
  
      END  
     ELSE  
      BEGIN  
       SET @ColumnList=(STUFF(( SELECT DISTINCT ', ' + c.COLUMN_NAME  
              FROM  INFORMATION_SCHEMA.COLUMNS c  
              WHERE c.TABLE_SCHEMA+'.'+c.TABLE_NAME=@InvalidEntityTableName  
              AND c.COLUMN_NAME NOT IN('IsPermanentlyInvalid','ReProcess','ID','InvalidRuleId','LastModifiedDateUTC')  
              ORDER BY ', ' + c.COLUMN_NAME ASC  
              FOR XML PATH ('')), 1, 1, ''));  
  
       SET @strSQL=N'';  
       SET @strSQL=@strSQL  
        +' INSERT INTO '+LEFT(@InvalidEntityTableName, LEN(@InvalidEntityTableName)-8)+'('+@ColumnList+',LastModifiedDateUTC'+')'  
        +' SELECT '+@ColumnList+','+''''+Convert(varchar(30),@CurrentRunETLProcessCategoryUTC_StartedAt,21)+''''+' FROM '+@InvalidEntityTableName  
        +' WHERE ReProcess=1 AND IsPermanentlyInvalid<>1  AND CODE NOT IN(SELECT CODE FROM '+LEFT(@InvalidEntityTableName, LEN(@InvalidEntityTableName)-8)+ ');'  
        +' DELETE FROM '+@InvalidEntityTableName+' WHERE ReProcess=1 AND IsPermanentlyInvalid<>1 ;'  
      END  
  
       
  
     BEGIN TRY  
      SET @HistoryStage =  'Started Reprocess Rows For '+@InvalidEntityTableName+N'';  
  
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = @HistoryStage  
      , @Status = 'InProgress'  
      , @CurrentStatus = 'Started';  
  
      SET @HistoryStage =  'Completed Reprocess Rows For '+@InvalidEntityTableName+N'';        
      EXECUTE sp_executesql @statement = @strSQL ;            
  
      EXEC ETLProcess.AuditLog  
        @ProcessCategory = @ProcessCategory  
       , @Phase = 'ProcessHistory'  
       , @ProcessName = @ProcessName  
       , @Stage = @HistoryStage  
       , @Status = 'Completed'  
       , @CurrentStatus = 'Completed'   
       , @inserts = @@ROWCOUNT;  
        
     END TRY  
  
      BEGIN CATCH  
         
       SET @IsError=1  
        SELECT   
         @ErrorProcedure= s.name+'.'+o.name   
        FROM   
         SYS.OBJECTS O   
   
         INNER JOIN SYS.SCHEMAS S   
         ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;  
  
        EXEC ETLProcess.AuditLog  
         @ProcessCategory = @ProcessCategory  
        , @Phase = 'ProcessHistory'  
        , @ProcessName = @ProcessName  
        , @Stage ='Push Reprocess Rows'  
        , @Status = 'Error'  
        , @CurrentStatus = 'Error'   
           
        EXEC ETLProcess.AuditLog  
         @ProcessCategory = @ProcessCategory  
        , @Phase = 'Process'  
        , @ProcessName = @ProcessName  
        , @Status = 'Error'  
        , @CurrentStatus = 'Error'  
        , @Stage = @ProcessStage  
  
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
        ,  @ErrorProcedure    
        , ERROR_LINE() AS ErrorLine    
        , ERROR_MESSAGE() AS ErrorMessage  
        , GETDATE()  
  
        EXEC ETLProcess.EmailNotification  
         @ProcessCategory=@ProcessCategory  
        , @ProcessName= @ProcessName  
        , @ProcessStage=@ProcessStage  
        , @ErrorMessage=@ErroMessage  
        , @IsError='Yes';  
  
  
      END CATCH  
  
        
       IF @IsError=1  
       THROW 50001, @ErroMessage, 1;   
      
       FETCH NEXT             
      FROM            
         tableCursor INTO @InvalidEntityTableName;            
  
           
       END                 
      close tableCursor;            
      deallocate tableCursor;  
  
    END    --IF @CurrentStatus NOT IN('Completed','Hold')  
    EXEC ETLProcess.AuditLog  
         @ProcessCategory = @ProcessCategory  
        , @Phase = 'Process'  
        , @ProcessName = @ProcessName  
        , @Status = 'Completed'  
        , @CurrentStatus = 'Completed'  
        , @Stage = @ProcessStage;             
        
   END --IF ISNULL(@ProcessID,0) > 0 AND ISNULL(@RunId,0) > 0  
            
   --select @strSQL       