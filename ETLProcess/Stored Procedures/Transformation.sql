CREATE procedure [ETLProcess].[Transformation]      
(      
 @ProcessName VARCHAR(100)       
 )      
as  	
-- =============================================            
-- Author:      Rahul Singh            
-- Create Date: 2020-09-09            
-- Description:             
-- Version History    1. Created 2020=09-09    
-- ============================================ 
	SET NOCOUNT ON;   
	
	DECLARE @tableName varchar(200) =CONCAT('StageProcessing','.', @ProcessName)     ;
	DECLARE @ProcessID INT=(SELECT DISTINCT ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName=@ProcessName);

	DECLARE @strSQL nvarchar(max) = N''
	
	DECLARE @HistoryStage VARCHAR(200);

	DECLARE @IsError BIT=0;
	
	DECLARE @ErrorProcedure VARCHAR(100);

	DECLARE @RunId  INT;
	DECLARE	@IsAuditEntryExists INT;
	DECLARE	@ProcessCategory nVARCHAR(100)=N'DTC_SourceTransformation_ETL';
	DECLARE	@CurrentStatus nVARCHAR(100) ;
	DECLARE @CurrentProcedureName nVarchar(100)='';
	DECLARE @ErroMessage VARCHAR(100)='Error Source Transformation';
	
	
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

	--Get all columns of StageProcessing table matching  with [ETLProcess].[ETLSourceMapping]

	Declare @SourceColumnList table(SourceColumnName Varchar(200))     

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
			AND CurrentStage='SourceTransformation';

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
			AND AuditProcess.CurrentStage = 'SourceTransformation';

IF ISNULL(@CurrentStatus,'') NOT IN('Completed','Hold')

BEGIN
      
	Insert Into      
	   @SourceColumnList(SourceColumnName)       
	   SELECT   DISTINCT    
		  c.name 'Column Name'     
      
	   FROM      
		  sys.columns c       
		  INNER JOIN      
			 sys.types t       
			 ON c.user_type_id = t.user_type_id       
			 INNER JOIN ETLProcess.ETLSourceMapping sm ON sm.SourceColumnName=c.name
			INNER JOIN ETLProcess.ETLTransformations tr ON tr.ColumnName=sm.DestinationColumnName AND tr.ActiveFlag=1
		  LEFT OUTER JOIN      
			 sys.index_columns ic       
			 ON ic.object_id = c.object_id       
			 AND ic.column_id = c.column_id       
		  LEFT OUTER JOIN      
			 sys.indexes i       
			 ON ic.object_id = i.object_id       
			 AND ic.index_id = i.index_id       
	   WHERE      
		  c.object_id = OBJECT_ID(@tableName)  
		  AND ISNULL(i.is_primary_key, 0)=0
	  
		-- Select * from @SourceColumnList
		     

		IF EXISTS(
					SELECT 1 FROM 
						ETLProcess.ETLSourceMapping sm 
						JOIN @SourceColumnList l On l.SourceColumnName=sm.SourceColumnName
						JOIN ETLProcess.ETLTransformations t ON t.ColumnName=sm.DestinationColumnName
					WHERE t.ActiveFlag=1
					)
					

		BEGIN


				-------Update Table      
				 Declare @SourceColumnName varchar(200),    
				 @DestinationColumnName varchar(200),
				 @replaceParameter Varchar(50),
				 @isExternalSource varchar(1),
				 @isExternal Varchar(1),
				 @JurCodeSourceColumn varchar(200),
				 @ProvinceCodeSOurceColumn varchar(200),
				 @externalFileName Varchar(200),
				 @FileName Varchar(200)

				 IF EXISTS(Select 1 From ETLProcess.ETLProcess p 
						JOIN ETLProcess.ETLProcessCategory pc ON p.ProcessCategoryId=pc.ProcessCategoryId
						WHERE p.ProcessName=@ProcessName AND pc.ProcessCategoryName='DTC_ExternalSource_ETL')
					BEGIN
						SET @isExternalSource='1'
						SET @externalFileName=@ProcessName

					END
					ELSE
						SET @isExternalSource='0'
				 
				 
 
				Declare tableCursor CURSOR FOR       
				 SELECT 
						DISTINCT
						source.SourceColumnName,
						em.DestinationColumnName
		
						
		 
				FROM 
						@SourceColumnList source
						JOIN  [ETLProcess].[ETLSourceMapping] em ON em.SourceColumnName=source.SourceColumnName
						JOIN  [ETLProcess].[ETLTransformations] t ON t.ColumnName=em.DestinationColumnName
						
				WHERE		
						t.ColumnName<>'JurCode'

		
		
				   open tableCursor;      
				FETCH NEXT       
				FROM      
				   tableCursor INTO @SourceColumnName,      
				   @DestinationColumnName
				WHILE @@FETCH_STATUS = 0       
				BEGIN   
					--CASE statement daisy chaining code here
						 Declare @ReplaceStatement varchar(max) = '',       
					   @replaceCol varchar(max) = '@replaceParameter' ,     
					   @sourceValue varchar(100),      
					  @TranformValue varchar(100),      
					  @function varchar(100)       
            
				   Declare rulecursor cursor For       
					  SELECT      
						 SourceValue,      
						 TransformValue,      
						 [function]       
					  FROM      
						 [ETLProcess].ETLTransformations     
					  where      
						 [function] = 'CASE'    
						 And ColumnName=@DestinationColumnName
						 And  ColumnName<>'JurCode'
						 And SourceValue<>'If ProvinceCode is null'
						 AND ActiveFlag=1
		 

         
				   OPEN rulecursor;      
				 FETCH NEXT       
					  FROM      
						 rulecursor INTO @sourceValue,      
						 @TranformValue,      
						 @function ;      
				 WHILE @@FETCH_STATUS = 0       
				 BEGIN     
				   -- set      
				   -- @ReplaceStatement = @function + '(' + @replacecol + ',' + '''' + @sourceValue + '''' + ',' + ''''+@TranformValue +''''+ ')' ;      
				   -- set      
					--@replacecol = @ReplaceStatement 
						Set @ReplaceStatement=	@ReplaceStatement+' WHEN '''+ @sourceValue +''' THEN '''	+@TranformValue+''''
					FETCH NEXT       
					FROM      
					rulecursor INTO @sourceValue,      
					@TranformValue,      
					@function ;      
				 END;      
      
				 close rulecursor;      
				 deallocate rulecursor;      
				Set      
				   @ReplaceStatement = REPLACE(@ReplaceStatement, '@replaceParameter', '['+@SourceColumnName+']')       
				set      
				   --@strSQL = @strSQL + 'UPDATE ' + @tableName + ' SET ' + '['+@SourceColumnName+']' + '=' +'CASE '+ '['+@SourceColumnName+']' +@ReplaceStatement  + ' END WHERE '+'['+@SourceColumnName+']'+' IN (SELECT SourceValue   FROM ETLProcess.ETLTransformations WHERE ColumnName = '+''''+@DestinationColumnName+''''+')' +';'       
				  --   @strSQL = @strSQL + 'UPDATE ' + @tableName + ' SET ' + '['+@SourceColumnName+']' + '=' +'CASE '+ '['+@SourceColumnName+']' +@ReplaceStatement  + '  END ;'           
					@strSQL = @strSQL + ' '+'['+@SourceColumnName+']' + '=' +'CASE '+ '['+@SourceColumnName+']' +@ReplaceStatement  + '  END ,'           
				Set      
				   @ReplaceStatement = REPLACE(@ReplaceStatement, '['+@SourceColumnName+']', '@replaceParameter') 
				Set 
					@isExternal=@isExternalSource
				Set 
					@FileName=@externalFileName
				   FETCH NEXT       
				FROM      
				   tableCursor INTO @SourceColumnName, 
				   @DestinationColumnName;      
				END      
				; 
					IF RIGHT(@strSQL,1)=',' 
						BEGIN
						SET @strSQL=LEFT(@strSQL, LEN(@strSQL) - 1)
						END
					SET @strSQL= 'UPDATE ' + @tableName + ' SET ' +@strSQL+' ;'
				--
				
				
				IF EXISTS(Select 1 From ETLProcess.ETLSourceMapping sm 
											JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName 
											JOIN ETLProcess.ETLTransformations tr On tr.ColumnName=sm.DestinationColumnName 
											Where sm.DestinationColumnName='ProvinceCode' And sm.ProcessId=@ProcessID and tr.ActiveFlag=1
						  )
				
				BEGIN	
						 SET @ProvinceCodeSOurceColumn=(SELECT DISTINCT c.SourceColumnName       
					  From ETLProcess.ETLSourceMapping sm       
					  JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName       
					  Where sm.DestinationColumnName='ProvinceCode')
					IF ( @ProvinceCodeSOurceColumn IS NOT NULL AND @isExternal=1)
					 BEGIN
		
						SET @strSQL=@strSQL+ 'UPDATE '+ @tableName +' SET '+ '['+@ProvinceCodeSOurceColumn+']' +'='+  +'CASE WHEN ('+'['+@ProvinceCodeSOurceColumn+']' +' IS NULL OR '+'['+@ProvinceCodeSOurceColumn+']' +' = '''''+') THEN LEFT('+''''+@FileName+''''+',2) ELSE '+'['+@ProvinceCodeSOurceColumn+']' +' END '+ ' ;'
					END

						IF EXISTS(Select 1 From ETLProcess.ETLSourceMapping sm 
										JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName 
										JOIN ETLProcess.ETLTransformations tr On tr.ColumnName=sm.DestinationColumnName 
										Where sm.DestinationColumnName='JurCode' AND sm.ProcessId=@ProcessID and tr.ActiveFlag=1
								  )
							BEGIN	
	
	
							 Select @JurCodeSourceColumn=c.SourceColumnName       
							  From ETLProcess.ETLSourceMapping sm       
							  JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName       
							  Where sm.DestinationColumnName='JurCode'

							  -- Select @ProvinceCodeSOurceColumn=c.SourceColumnName       
							  --From ETLProcess.ETLSourceMapping sm       
							  --JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName       
							  --Where sm.DestinationColumnName='ProvinceCode'
							  --SET @ProvinceCodeSOurceColumn='Pcode'
							  --set @JurCodeSourceColumn ='Juris'
								 IF (@JurCodeSourceColumn IS NOT NULL AND @ProvinceCodeSOurceColumn IS NOT NULL)
								 BEGIN
									SET @strSQL=@strSQL+' Declare @isExternalSource Bit; SET @isExternalSource= '+@isExternal+' ;'  
									SET @strSQL=@strSQL+ 'UPDATE '+ @tableName +' SET '+ '['+@JurCodeSourceColumn+']' +'='+  +'CASE WHEN '+'['+@ProvinceCodeSOurceColumn+']' +' = ''BC''' +' THEN '+'['+@JurCodeSourceColumn+']' + ' WHEN  '+'['+@ProvinceCodeSOurceColumn+']' +' <> ''BC''' + 'THEN '+'['+@ProvinceCodeSOurceColumn+'] END '+ ';'
								END
							END



				 END
				
				   SET @ProvinceCodeSOurceColumn=(SELECT DISTINCT c.SourceColumnName       
					  From ETLProcess.ETLSourceMapping sm       
					  JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName       
					  Where sm.DestinationColumnName='ProvinceCode')
				  --SET @ProvinceCodeSOurceColumn='Pcode'
				  --set @JurCodeSourceColumn ='Juris'

				  		IF @ProvinceCodeSOurceColumn IS NULL
						BEGIN 
							Set @ProvinceCodeSOurceColumn=(Select  LEFT(p.ProcessName,2) From ETLProcess.ETLProcess p
							JOIN ETLProcess.ETLProcessCategory pc On p.ProcessCategoryId=pc.ProcessCategoryId
							WHERE ProcessName=@ProcessName AND pc.ProcessCategoryName='DTC_ExternalSource_ETL')


							IF EXISTS(Select 1 From ETLProcess.ETLSourceMapping sm JOIN
													@SourceColumnList c On c.SourceColumnName=sm.SourceColumnName 
													JOIN ETLProcess.ETLTransformations tr On tr.ColumnName=sm.DestinationColumnName 
													Where sm.DestinationColumnName='JurCode' and sm.ProcessId=@ProcessID and tr.ActiveFlag=1
								    )
								BEGIN	
	
	
								 Select @JurCodeSourceColumn=c.SourceColumnName       
								  From ETLProcess.ETLSourceMapping sm       
								  JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName       
								  Where sm.DestinationColumnName='JurCode'

								  -- Select @ProvinceCodeSOurceColumn=c.SourceColumnName       
								  --From ETLProcess.ETLSourceMapping sm       
								  --JOIN @SourceColumnList c On c.SourceColumnName=sm.SourceColumnName       
								  --Where sm.DestinationColumnName='ProvinceCode'
								  --SET @ProvinceCodeSOurceColumn='Pcode'
								  --set @JurCodeSourceColumn ='Juris'
									 IF (@JurCodeSourceColumn IS NOT NULL AND @ProvinceCodeSOurceColumn IS NOT NULL)
									 BEGIN
										SET @strSQL=@strSQL+' Declare @isExternalSource Bit; SET @isExternalSource= '+@isExternal+' ;'  
										SET @strSQL=@strSQL+ 'DECLARE @Province varchar(50); SET @Province='+''''+@ProvinceCodeSOurceColumn+''''+';'+'  UPDATE '+ @tableName +' SET '+ '['+@JurCodeSourceColumn+']' +'='+  +'CASE WHEN @Province = ''BC''' +' THEN '+'['+@JurCodeSourceColumn+']' + ' WHEN  @Province <> ''BC''' + 'THEN @Province END '+ ';'
									END
								END



						END

					 
				--END

				--

				
				close tableCursor;      
				deallocate tableCursor;     
			

		--select @strSQL

			BEGIN TRY
				SET @HistoryStage =  'Started Source Transformation For '+@tableName+N'';

				EXEC ETLProcess.AuditLog
										@ProcessCategory = 'DTC_SourceTransformation_ETL'
									,	@Phase = 'ProcessHistory'
									,	@ProcessName = @tableName
									,	@Stage = @HistoryStage
									,	@Status = 'InProgress'
									,	@CurrentStatus = 'Started';
				
				SET @HistoryStage =  'Completed Source Transformation For '+@tableName+N'';

				EXECUTE sp_executesql @statement = @strSQL 

				EXEC ETLProcess.AuditLog
									@ProcessCategory = 'DTC_SourceTransformation_ETL'
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @tableName
								,	@Stage = @HistoryStage
								,	@Status = 'Completed'
								,	@CurrentStatus = 'Completed'	
								,	@Updates = @@ROWCOUNT;

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
									@ProcessCategory = 'DTC_SourceTransformation_ETL'
								,	@Phase = 'ProcessHistory'
								,	@ProcessName = @tableName
								,	@Stage ='Error Source Transformation'
								,	@Status = 'Error'
								,	@CurrentStatus = 'Error'	
									
								

								INSERT INTO ETLProcess.ETLStoredProcedureErrors
								(
									ProcessCategory
								,	ProcessName
								,	ErrorNumber
								,	ErrorSeverity
								,	ErrorState
								,	ErrorProcedure
								,	ErrorLine
								,	ErrorMessage
								,	ErrorDate
								)
								SELECT  
									'DTC_SourceTransformation_ETL'
								,	@ProcessName
								,	ERROR_NUMBER() AS ErrorNumber  
								,	ERROR_SEVERITY() AS ErrorSeverity  
								,	ERROR_STATE() AS ErrorState  
								,	 @CurrentProcedureName  
								,	ERROR_LINE() AS ErrorLine  
								,	ERROR_MESSAGE() AS ErrorMessage
								,	GETDATE()

								EXEC ETLProcess.EmailNotification
									@ProcessCategory='DTC_SourceTransformation_ETL'
								,	@ProcessName= @tableName
								,	@ProcessStage=@HistoryStage
								,	@ErrorMessage='Error Source Transformation'
								,	@IsError='Yes';
			END CATCH
			IF @IsError=1
			THROW 50001, @ErroMessage, 1;	
		END 

	END
	--
END