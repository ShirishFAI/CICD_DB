
CREATE   procedure [ETLProcess].[SourceCleanse]          
(          
 @ProcessName nVARCHAR(100)        
 )          
AS        

/****************************************************************************************
-- AUTHOR		]: Rahul Singh
-- DATE			: 09/25/2020
-- PURPOSE		: Cleansing StageProcessing Tables
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Rahul Singh	Original Version
******************************************************************************************/

	SET NOCOUNT ON;   
    
	DECLARE @tableName nvarchar(200) =CONCAT(N'StageProcessing','.', @ProcessName)     ;
    DECLARE @strSQL nvarchar(max) = N'' ; ---Get Columns and Data Type of of input table          
    DECLARE @ColumnList table(ColumnName Varchar(200), DataType Varchar(50), IsPrimaryKey Bit);  
	DECLARE @RunId  INT;
	DECLARE	@IsAuditEntryExists INT;
	DECLARE	@ProcessCategory nVARCHAR(100)=N'DTC_SourceCleansing_ETL';
	DECLARE	@CurrentStatus nVARCHAR(100) ;
	DECLARE @ProcessID INT=(SELECT DISTINCT ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName=@ProcessName);
	DECLARE @CurrentProcedureName nVarchar(100)=''
	SET @CurrentProcedureName=(select s.name+'.'+o.name from sys.objects o join sys.schemas s on s.schema_id=o.schema_id where object_id=@@PROCID)

	DECLARE @sourceRowCount int;
	IF EXISTS(SELECT 1 FROM sys.objects o Join sys.schemas s ON o.schema_id=s.schema_id WHERE s.name+'.'+o.name=@tableName)
	
		BEGIN
		DECLARE @SourceRowCountSQL nvarchar(max);
		SET @SourceRowCountSQL = N'select @sourceRowCount = count(*) from ' + @tablename;
	
		EXEC sp_executesql @SourceRowCountSQL, N'@sourceRowCount int output', @sourceRowCount = @sourceRowCount output;
		END

	IF EXISTS(SELECT 1 From ETLProcess.ETLProcess WHERE ProcessName=@ProcessName AND ActiveFlag=1) AND  @sourceRowCount>0
	BEGIN
	
	

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
			@IsAuditEntryExists= COUNT(1) 
		FROM 	
			ETLProcess.ETLProcess 				

			INNER JOIN 	ETLAudit.ETLProcess AuditProcess
			ON ETLProcess.ProcessId = AuditProcess.ProcessId

			INNER JOIN ETLProcess.ETLStatus
			ON ETLStatus.StatusId = AuditProcess.CurrentStatus
		WHERE 
			ETLProcess.ProcessName=@ProcessName		
			AND AuditProcess.RunId=@RunId
			AND CurrentStage='SourceCleanse';

		
			--IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0
			--BEGIN
			--	EXEC ETLProcess.AuditLog
			--			@ProcessCategory = 'DTC_SourceCleansing_ETL'
			--		,	@Phase = 'Process'
			--		,	@ProcessName = @ProcessName
			--		,	@Stage = 'Apply Cleansing Rules'
			--		,	@Status = 'InProgress'
			--		,	@CurrentStatus = 'Started'	;						
			--END

			
	
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
			AND AuditProcess.CurrentStage = 'SourceCleanse';

	IF ISNULL(@CurrentStatus,'') NOT IN('Completed','Hold')

	BEGIN
          
			Insert Into          
			   @ColumnList(ColumnName, DataType, IsPrimaryKey)           
			   SELECT          
				  c.name 'Column Name',          
				  t.Name 'Data type',          
				  ISNULL(i.is_primary_key, 0) 'Primary Key'           
			   FROM          
				  sys.columns c           
				  INNER JOIN          
					 sys.types t           
					 ON c.user_type_id = t.user_type_id           
				  LEFT OUTER JOIN          
					 sys.index_columns ic           
					 ON ic.object_id = c.object_id           
					 AND ic.column_id = c.column_id           
				  LEFT OUTER JOIN          
					 sys.indexes i           
					 ON ic.object_id = i.object_id           
					 AND ic.index_id = i.index_id           
			   WHERE          
				  c.object_id = OBJECT_ID(@tableName) ;          
            
           
             
			  --select * from @ColumnList     
      
                
			   ----Implementing rules for Replace function for ALL string columns          
       
			   -------Update Table          
			  Declare @sourceColumn nvarchar(200),   --Table cursor columns       
			  @destinationColumn nvarchar(200),    
			  @DataType nvarchar(20);    
    
         
			 Declare @cleansingrule nvarchar(100),     --Rule Cursor columns     
			 @replacevalue nvarchar(100),          
			 @function nvarchar(100),
			 @cleansingruleId Int
			 ;         
     
			 Declare tableCursor CURSOR FOR           
			  Select          
			  ColumnName,          
			  DataType           
			  From          
			  @ColumnList           
			  Where          
			  IsPrimaryKey = 0           
			  And DataType IN           
			  (          
			  'nvarchar',          
			  'nchar',          
			  'varchar',          
			  'char'          
			  )          
			   OPEN tableCursor;          
			FETCH NEXT           
			FROM          
			   tableCursor INTO @sourceColumn,          
			   @DataType ;          
			WHILE @@FETCH_STATUS = 0           
			BEGIN      
      
			 --Rule Cursor Starts    
    
        
                
					DECLARE rulecursor CURSOR FOR    
				   SELECT          
				   cleansingrule,          
				   replacevalue,          
				   [function] ,
				   cleansingruleId
				   FROM          
					[ETLProcess].[ETLSourceCleansingRules]      
        
				   where          
				   [ColumnName] = 'ALL'    
				   AND ActiveFlag=1    

				   ORDER BY CleansingRuleId DESC
           
    
             
				  OPEN rulecursor;          
				FETCH NEXT           
				  FROM          
				  rulecursor INTO @cleansingrule,          
				  @replacevalue ,          
				  @function,@cleansingruleId  ;          
				WHILE @@FETCH_STATUS = 0           
				BEGIN         
					
					SET @strSQL=@strSQL+N' BEGIN TRY EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applying Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Started'' '
					
					 SET @strSQL=@strSQL+N' UPDATE '+@tableName+ N' SET '+N'['+@sourceColumn+']'+N' = '+@function+N' WHERE '+@cleansingrule+N' END TRY BEGIN CATCH Set @isError =1 EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''ERROR Applying Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''Error'',@CurrentStatus = ''Error'' INSERT INTO ETLProcess.ETLStoredProcedureErrors(ProcessCategory,ProcessName,ErrorNumber,ErrorSeverity,ErrorState,ErrorProcedure,ErrorLine,ErrorMessage,ErrorDate) SELECT '+N''''+@ProcessCategory+N''','+N''''+@ProcessName+N''',ERROR_NUMBER(),ERROR_SEVERITY(),ERROR_STATE(),'+N''''+@CurrentProcedureName+N''''+N',ERROR_LINE(),ERROR_MESSAGE(),GETUTCDATE()   END CATCH IF(@iserror =1) THROW 50005, N''An error occurred while Running Source cleansing'', 1;'     
					 SET @strSQL=REPLACE(@strSQL,N'@srcColumn',N'['+@sourceColumn+N']')  
					 SET @strSQL=@strSQL+N' EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applied Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Completed'' ,@updates=@@rowcount ;'
         
				FETCH NEXT           
				FROM          
				rulecursor INTO @cleansingrule,          
				@replacevalue,          
				@function,@cleansingruleId ;          
				END;          
          
				close rulecursor;          
				deallocate rulecursor;          
        
			 --Rule Cursor Ends    
    
			   FETCH NEXT           
			FROM          
			   tableCursor INTO @sourceColumn,          
			   @DataType ;          
			 END               
			close tableCursor;          
			deallocate tableCursor;          
          
			--select @strSQL          
          
			-------Other Entity  mapped columns rules cleansing    
			 Declare tableCursor CURSOR FOR           
			  Select DISTINCT         
			  c.ColumnName,      
			  sm.DestinationColumnName,    
			  c.DataType           
			  From          
			  @ColumnList  c    
			  Join ETLProcess.ETLSourceMapping  sm ON c.ColumnName=sm.SourceColumnName    
			  Join ETLProcess.ETLSourceCleansingRules cr ON cr.ColumnName=sm.DestinationColumnName    
      
			  Where          
			  IsPrimaryKey = 0         
			  AND cr.ActiveFlag=1    
         
			   OPEN tableCursor;          
			FETCH NEXT           
			FROM          
			   tableCursor INTO @sourceColumn,@destinationColumn,          
			   @DataType ;          
			WHILE @@FETCH_STATUS = 0           
			BEGIN      
      
			 --Rule Cursor Starts    
    
        
                
					DECLARE rulecursor CURSOR FOR    
				   SELECT          
				   cleansingrule,          
				   replacevalue,          
				   [function]           
				   FROM          
					[ETLProcess].[ETLSourceCleansingRules]      
        
        
				   WHERE          
				   [ColumnName] = @destinationColumn    
				   AND ActiveFlag=1    
     
				ORDER BY CleansingRuleId DESC  
           
    
             
				  OPEN rulecursor;      
          
				FETCH NEXT           
				  FROM          
				  rulecursor INTO @cleansingrule,          
				  @replacevalue ,          
				  @function  ;          
				WHILE @@FETCH_STATUS = 0           
				BEGIN         
					 SET @strSQL=@strSQL+N'BEGIN TRY EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applying Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Started'' '
					 SET @strSQL=@strSQL+N' UPDATE '+@tableName+ N' SET '+N'['+@sourceColumn+']'+N' = '+@function+N' WHERE '+@cleansingrule+N' END TRY BEGIN CATCH Set @isError =1 EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''ERROR Applying Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''Error'',@CurrentStatus = ''Error'' INSERT INTO ETLProcess.ETLStoredProcedureErrors(ProcessCategory,ProcessName,ErrorNumber,ErrorSeverity,ErrorState,ErrorProcedure,ErrorLine,ErrorMessage,ErrorDate) SELECT '+N''''+@ProcessCategory+N''','+N''''+@ProcessName+N''',ERROR_NUMBER(),ERROR_SEVERITY(),ERROR_STATE(),'+N''''+@CurrentProcedureName+N''''+N',ERROR_LINE(),ERROR_MESSAGE(),GETUTCDATE()   END CATCH IF(@iserror =1) THROW 50005, N''An error occurred while Running Source cleansing'', 1;'     
					 SET @strSQL=REPLACE(@strSQL,N'@srcColumn',N'['+@sourceColumn+N']')  
					 SET @strSQL=@strSQL+N' EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applied Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Completed'' ,@updates=@@rowcount;'

         
				FETCH NEXT           
				FROM          
				rulecursor INTO @cleansingrule,          
				@replacevalue,          
				@function ;          
				END;          
          
				close rulecursor;          
				deallocate rulecursor;          
        
			 --Rule Cursor Ends    
    
			   FETCH NEXT           
			FROM          
			   tableCursor INTO @sourceColumn,@destinationColumn,          
			   @DataType ;          
			 END               
			close tableCursor;          
			deallocate tableCursor;          
          
			--PIN cleansing
			IF EXISTS(Select 1 From ETLProcess.ETLSourceMapping sm 
							JOIN @ColumnList c On c.ColumnName=sm.SourceColumnName 
							Join ETLProcess.ETLSourceCleansingRules cr ON cr.ColumnName=sm.DestinationColumnName    
							Where sm.DestinationColumnName='PIN' And sm.ProcessId=@ProcessID and cr.ActiveFlag=1
						)
			BEGIN

	
				Declare @PINSourceColumn Varchar(200),@ProvinceSourceColumn Varchar(200)

			 Select @PINSourceColumn=c.ColumnName       
			  From ETLProcess.ETLSourceMapping sm       
			  JOIN @ColumnList c On c.ColumnName=sm.SourceColumnName       
			  Where sm.DestinationColumnName='PIN'

			   Select @ProvinceSourceColumn=c.ColumnName       
			  From ETLProcess.ETLSourceMapping sm       
			  JOIN @ColumnList c On c.ColumnName=sm.SourceColumnName       
			  Where sm.DestinationColumnName='ProvinceCode'

			   IF (@PINSourceColumn IS NOT NULL AND @ProvinceSourceColumn IS NOT NULL)
				BEGIN
					SET @strSQL=@strSQL+N'BEGIN TRY EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applying Rule: Source Column'+N'['+@PINSourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Started'' '
					SET @strSQL=@strSQL+N'  UPDATE '+@tableName+ ' SET '+N'['+@PINSourceColumn+N']'+N' = CASE WHEN ('+N'['+@ProvinceSourceColumn+']'+N' IN(''ON'',''ONT'',''Ontario'',''BC'',''B.C.'',''British Columbia'',''SK'',''Saskatawon'',''Saskatchewan'',''SASKATOON'',''PEI'',''Prince Edward Island'',''PE'') AND '+'LEN('+N'['+@PINSourceColumn+N']'+') = 9) OR ('+N'['+@ProvinceSourceColumn+N']'+N' IN(''AB'',''ABLERTA'',''Alberta'',''ALBERTON'') AND '+'LEN('+N'['+@PINSourceColumn+']'+N') = 10) OR ('+N'['+@ProvinceSourceColumn+N']'+' IN(''MB'',''Manitoba'',''Québec'',''Quebec'',''Qubec'',''Qu?bec'',''PQ'',''QC'') AND '+'LEN('+N'['+@PINSourceColumn+N']'+N') = 7) OR ('+N'['+@ProvinceSourceColumn+N']'+N' IN(''NB'',''New Brunswick'',''Nova Scotia'',''NS'') AND '+'LEN('+N'['+@PINSourceColumn+']'+N') = 8) OR ('+N'['+@ProvinceSourceColumn+']'+N' IN(''NL'',''Newfoundland and Labrador'') AND '+N'LEN('+'['+@PINSourceColumn+']'+N') = 6) '+' THEN '+N'['+@PINSourceColumn+']'+N' ELSE NULL END '+N' END TRY BEGIN CATCH Set @isError =1 EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''ERROR Applying Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''Error'',@CurrentStatus = ''Error'' INSERT INTO ETLProcess.ETLStoredProcedureErrors(ProcessCategory,ProcessName,ErrorNumber,ErrorSeverity,ErrorState,ErrorProcedure,ErrorLine,ErrorMessage,ErrorDate) SELECT '+N''''+@ProcessCategory+N''','+N''''+@ProcessName+N''',ERROR_NUMBER(),ERROR_SEVERITY(),ERROR_STATE(),'+N''''+@CurrentProcedureName+N''''+N',ERROR_LINE(),ERROR_MESSAGE(),GETUTCDATE()   END CATCH IF(@iserror =1) THROW 50005, N''An error occurred while Running Source cleansing'', 1;'
					SET @strSQL=@strSQL+N' EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applied Rule: Source Column'+N'['+@PINSourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Completed'' ,@updates=@@rowcount;'
				END

			  IF @ProvinceSourceColumn IS NULL
			   BEGIN 
				Set @ProvinceSourceColumn=(Select  LEFT(p.ProcessName,2) From ETLProcess.ETLProcess p
				JOIN ETLProcess.ETLProcessCategory pc On p.ProcessCategoryId=pc.ProcessCategoryId
				WHERE ProcessName=@ProcessName AND pc.ProcessCategoryName='DTC_ExternalSource_ETL')

				 SET @strSQL=@strSQL+N'BEGIN TRY EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applying Rule: Source Column'+N'['+@PINSourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Started'' '
					SET @strSQL=@strSQL+N' DECLARE @Province Varchar(100); SET @Province='+''''+@ProvinceSourceColumn+''''+';' +'  UPDATE '+@tableName+ ' SET '+N'['+@PINSourceColumn+N']'+N' = CASE WHEN (@Province IN(''ON'',''ONT'',''Ontario'',''BC'',''B.C.'',''British Columbia'',''SK'',''Saskatawon'',''Saskatchewan'',''SASKATOON'',''PEI'',''Prince Edward Island'',''PE'') AND '+'LEN('+N'['+@PINSourceColumn+N']'+') = 9) OR (@Province IN(''AB'',''ABLERTA'',''Alberta'',''ALBERTON'') AND '+'LEN('+N'['+@PINSourceColumn+']'+N') = 10) OR (@Province IN(''MB'',''Manitoba'',''Québec'',''Quebec'',''Qubec'',''Qu?bec'',''PQ'',''QC'') AND '+'LEN('+N'['+@PINSourceColumn+N']'+N') = 7) OR (@Province IN(''NB'',''New Brunswick'',''Nova Scotia'',''NS'') AND '+'LEN('+N'['+@PINSourceColumn+']'+N') = 8) OR (@Province IN(''NL'',''Newfoundland and Labrador'') AND '+N'LEN('+'['+@PINSourceColumn+']'+N') = 6) '+' THEN '+N'['+@PINSourceColumn+']'+N' ELSE NULL END '+N' END TRY BEGIN CATCH Set @isError =1 EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''ERROR Applying Rule: Source Column'+N'['+@sourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''Error'',@CurrentStatus = ''Error'' INSERT INTO ETLProcess.ETLStoredProcedureErrors(ProcessCategory,ProcessName,ErrorNumber,ErrorSeverity,ErrorState,ErrorProcedure,ErrorLine,ErrorMessage,ErrorDate) SELECT '+N''''+@ProcessCategory+N''','+N''''+@ProcessName+N''',ERROR_NUMBER(),ERROR_SEVERITY(),ERROR_STATE(),'+N''''+@CurrentProcedureName+N''''+N',ERROR_LINE(),ERROR_MESSAGE(),GETUTCDATE()   END CATCH IF(@iserror =1) THROW 50005, N''An error occurred while Running Source cleansing'', 1;'
					SET @strSQL=@strSQL+N' EXEC ETLProcess.AuditLog @ProcessCategory = '+N''''+@ProcessCategory+''''+N',@Phase = ''ProcessHistory'',@ProcessName = '+N''''+@ProcessName+''''+N',@Stage =''Applied Rule: Source Column'+N'['+@PINSourceColumn+']'+ N'RuleID= '+Cast(@cleansingruleId as nvarchar(5))+N''',@Status = ''InProgress'',@CurrentStatus = ''Completed'' ,@updates=@@rowcount;'


			   END

			  

			END

			SET @strSQL=N'DECLARE @isError INT '+@strSQL

	--Select @strSQL          
          
			EXECUTE sp_executesql @statement = @strSQL  
			
			--IF ISNULL(@IsAuditEntryExists,0)=0 AND ISNULL(@RunId,0) > 0
			--BEGIN
			--	EXEC ETLProcess.AuditLog
			--			@ProcessCategory = 'DTC_SourceCleansing_ETL'
			--		,	@Phase = 'Process'
			--		,	@ProcessName = @ProcessName
			--		,	@Stage = 'Apply Cleansing Rules'
			--		,	@Status = 'completed'
			--		,	@CurrentStatus = 'Completed'	;						
			--END
	END 
			--
END