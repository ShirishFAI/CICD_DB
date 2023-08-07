

CREATE  PROCEDURE [ETLProcess].[PostEntityLoadUpdateProvinceCodeAndCity]      
AS          
BEGIN  
/***************************************************************************************  
-- AUTHOR  : Rahul Singh  
-- DATE   : 10/25/2020  
-- PURPOSE  : Update ProviceCode and City(For dbo.Address) for external source records when mapped province code /city  
     source  columns are not present.  
-- DEPENDENCIES :   
--  
-- VERSION HISTORY:  
** ----------------------------------------------------------------------------------------  
** 09/25/2020 Rahul Singh Original Version  
** 02/02/2022 Shirish Waghmale: Updating province code for new entity Permit (TFS:466415)
******************************************************************************************/  
 SET NOCOUNT ON;  
 SET ANSI_WARNINGS OFF;  
  
 DECLARE @ProcessCategory VARCHAR(100)='DTC_StageEntityLoad_ETL';  
 DECLARE @ProcessName VARCHAR(100)   ='PostEntityLoadUpdateProvinceCode'  
 DECLARE @ProcessStage VARCHAR(100)='Update Province and City columns';  
 DECLARE @HistoryStage VARCHAR(200);  
 DECLARE @ErroMessage VARCHAR(100)='Error Update Province and City columns';  
 DECLARE @ProcessID INT;  
 DECLARE @IsAuditEntryExists INT;  
 DECLARE @RunId INT;  
 DECLARE @CurrentStatus VARCHAR(100);  
 DECLARE @IsError BIT=0;  
 DECLARE @ErrorProcedure VARCHAR(100);   
  
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
   BEGIN TRY  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.Address'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE a   
     SET a.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.Address a  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=a.Data_Source_ID  
     JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(a.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
     AND LEFT(ep.ProcessName,3)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Address'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
       
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update City started : dbo.Address'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE  
     [Address]  
    SET  
     City = SUBSTRING(ProcDetail.ProcessName, StartPos,CASE WHEN EndPos<0 THEN 1 ELSE EndPos END)  
    FROM   
     dbo.Address [Address]  
  
     INNER JOIN  
     (   
      SELECT   
       Process.ProcessId  
      , Process.ProcessName  
      , CHARINDEX('_',Process.ProcessName)+1 as StartPos  
      , CHARINDEX('_',Process.ProcessName,CHARINDEX('_', Process.ProcessName)+1)  
         -CASE WHEN LEN(CHARINDEX('_',Process.ProcessName)) >0 THEN  
            CHARINDEX('_',Process.ProcessName)+1  
           ELSE  
            0  
         END AS EndPos  
      FROM  
       ETLProcess.ETLProcess Process   
  
       INNER JOIN ETLProcess.ETLProcessCategory ProcessCategory   
       ON ProcessCategory.ProcessCategoryId=Process.ProcessCategoryId  
      WHERE  
       ProcessCategory.ProcessCategoryName='DTC_ExternalSource_ETL'  
     ) ProcDetail  
     
      ON ProcDetail.ProcessId=[Address].Data_Source_ID  
    WHERE   
     ISNULL([Address].City,'')=''   
     AND SUBSTRING(ProcDetail.ProcessName, StartPos,CASE WHEN EndPos<0 THEN 1 ELSE EndPos END)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update City completed : dbo.Address'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
  
  
    --Building  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.Building'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE b   
     SET b.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.Building b  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=b.Data_Source_ID  
     JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(b.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
     AND LEFT(ep.ProcessName,3)<>'ALL'  
      
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Building'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
  
    --Business  
     EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.Business'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE b   
     SET b.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.Business b  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=b.Data_Source_ID  
     JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(b.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
     AND LEFT(ep.ProcessName,3)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Business'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
  
  
    --Listing  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.Listing'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
      
    UPDATE l   
     SET l.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.Listing l  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=l.Data_Source_ID  
     JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(l.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
     AND LEFT(ep.ProcessName,3)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Listing'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
         
      
    --PIN  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.PIN'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
      
    UPDATE p   
     SET p.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.PIN p  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=p.Data_Source_ID  
     JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(p.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
     AND LEFT(ep.ProcessName,3)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.PIN'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
  
    --Property  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.Property'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE p   
     SET p.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.Property p  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=SUBSTRING(p.code, 1, CHARINDEX('_', p.code) -1)  
     JOIN  ETLProcess.ETLProcessCategory pc On pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(p.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL' --DTC_ExternalSource_ETL  
        
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Property'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
       
      
    --dbo.property Jurcode  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update JurCode started : dbo.Property'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE dbo.Property SET JurCode=ProvinceCode WHERE ISNULL(JurCode,'')='' AND ProvinceCode<>'BC'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update JurCode completed : dbo.Property'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
  
    --dbo.Taxation  
      
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province started : dbo.Taxation'  
    , @Status = 'InProgress'  
    , @CurrentStatus = 'Started';  
  
    UPDATE t   
     SET t.ProvinceCode=LEFT(ep.ProcessName,2)  
    FROM   
     dbo.Taxation t  
     JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=t.Data_Source_ID  
     JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
    WHERE ISNULL(t.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
     AND LEFT(ep.ProcessName,3)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Taxation'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT;  
  
     --dbo.Valuation  
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update Province started : dbo.Valuation'  
     , @Status = 'InProgress'  
     , @CurrentStatus = 'Started';  
  
     UPDATE v   
      SET v.ProvinceCode=LEFT(ep.ProcessName,2)  
     FROM   
      dbo.Valuation v  
      JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=v.Data_Source_ID  
      JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
     WHERE ISNULL(v.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
      AND LEFT(ep.ProcessName,3)<>'ALL'  
  
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update Province completed : dbo.Valuation'  
     , @Status = 'Completed'  
     , @CurrentStatus = 'Completed'   
     , @Updates = @@ROWCOUNT;  
  
       
     --dbo.Parcel  
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update Province started : dbo.Parcel'  
     , @Status = 'InProgress'  
     , @CurrentStatus = 'Started';  
  
     UPDATE p   
      SET p.ProvinceCode=LEFT(ep.ProcessName,2)  
     FROM   
      dbo.Parcel p  
      JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=p.Data_Source_ID  
      JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
     WHERE ISNULL(p.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
      AND LEFT(ep.ProcessName,3)<>'ALL'  
       
       
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update Province completed : dbo.Parcel'  
     , @Status = 'Completed'  
     , @CurrentStatus = 'Completed'   
     , @Updates = @@ROWCOUNT;  
           
       
     --dbo.Taxation  
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update JurCode started : dbo.Taxation'  
     , @Status = 'InProgress'  
     , @CurrentStatus = 'Started';  
  
     UPDATE dbo.Taxation SET JurCode=ProvinceCode WHERE ISNULL(JurCode,'')=''  AND ProvinceCode<>'BC'       
       
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update JurCode completed : dbo.Taxation'  
     , @Status = 'Completed'  
     , @CurrentStatus = 'Completed'   
     , @Updates = @@ROWCOUNT;  
           
           
      --dbo.Address           
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode started : dbo.Address'  
      , @Status = 'InProgress'  
      , @CurrentStatus = 'Started';  
  
      UPDATE dbo.Address SET JurCode=ProvinceCode WHERE ISNULL(JurCode,'') ='' AND ProvinceCode<>'BC'  
        
        
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode completed : dbo.Address'  
      , @Status = 'Completed'  
      , @CurrentStatus = 'Completed'   
      , @Updates = @@ROWCOUNT;  
           
        
      --Listing           
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode started : dbo.Listing'  
      , @Status = 'InProgress'  
      , @CurrentStatus = 'Started';  
  
      UPDATE dbo.Listing SET JurCode=ProvinceCode WHERE ISNULL(JurCode,'')='' AND ProvinceCode<>'BC'  
        
        
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode completed : dbo.Listing'  
      , @Status = 'Completed'  
      , @CurrentStatus = 'Completed'   
      , @Updates = @@ROWCOUNT;  
           
  
      --Valuation    
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode started : dbo.Valuation'  
      , @Status = 'InProgress'  
      , @CurrentStatus = 'Started';  
  
      UPDATE dbo.Valuation SET JurCode=ProvinceCode WHERE ISNULL(JurCode,'')='' AND ProvinceCode<>'BC'  
        
        
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode completed : dbo.Valuation'  
      , @Status = 'Completed'  
      , @CurrentStatus = 'Completed'   
      , @Updates = @@ROWCOUNT;  
  
 

	  --Permit  
     EXEC ETLProcess.AuditLog  
      @ProcessCategory = @ProcessCategory  
     , @Phase = 'ProcessHistory'  
     , @ProcessName = @ProcessName  
     , @Stage = 'Update Province started : dbo.Permit'  
     , @Status = 'InProgress'  
     , @CurrentStatus = 'Started';  
       
     UPDATE Permit   
      SET Permit.ProvinceCode=LEFT(ep.ProcessName,2)  
     FROM   
      dbo.Permit Permit  
      JOIN ETLProcess.ETLProcess ep ON ep.ProcessId=Permit.Data_Source_ID  
      JOIN ETLProcess.ETLProcessCategory pc ON pc.ProcessCategoryId=ep.ProcessCategoryId  
     WHERE ISNULL(Permit.ProvinceCode,'')='' AND pc.ProcessCategoryName='DTC_ExternalSource_ETL'  
      AND LEFT(ep.ProcessName,3)<>'ALL'  
  
    EXEC ETLProcess.AuditLog  
     @ProcessCategory = @ProcessCategory  
    , @Phase = 'ProcessHistory'  
    , @ProcessName = @ProcessName  
    , @Stage = 'Update Province completed : dbo.Permit'  
    , @Status = 'Completed'  
    , @CurrentStatus = 'Completed'   
    , @Updates = @@ROWCOUNT; 


	EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode started : dbo.Permit'  
      , @Status = 'InProgress'  
      , @CurrentStatus = 'Started';  
  
      UPDATE dbo.Permit SET JurCode=ProvinceCode WHERE ISNULL(JurCode,'')='' AND ProvinceCode<>'BC'  
        
        
      EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'ProcessHistory'  
      , @ProcessName = @ProcessName  
      , @Stage = 'Update JurCode completed : dbo.Permit'  
      , @Status = 'Completed'  
      , @CurrentStatus = 'Completed'   
      , @Updates = @@ROWCOUNT;  



	  EXEC ETLProcess.AuditLog  
       @ProcessCategory = @ProcessCategory  
      , @Phase = 'Process'  
      , @ProcessName = @ProcessName  
      , @Status = 'Completed'  
      , @CurrentStatus = 'Completed'  
      , @Stage = @ProcessStage; 
           
     END TRY  
       
     BEGIN CATCH            
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
      , @Stage ='Error Update ProvinceCode,City,JurCode'  
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
    END  
   END  
  
     
  
 END