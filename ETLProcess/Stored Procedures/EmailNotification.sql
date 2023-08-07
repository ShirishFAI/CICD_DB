CREATE PROC [ETLProcess].[EmailNotification]
	@ProcessCategory VARCHAR(100) = ''
,	@ProcessName VARCHAR(100) = ''
,	@ProcessStage VARCHAR(100) = ''
,	@ErrorMessage VARCHAR(200)=''
,	@IsError VARCHAR(3) = 'Yes'
,	@PipelineName VARCHAR(100)=''
AS
BEGIN	
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Email Notifiation for process	
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 25-Sep-2020	Sanjay Janardhan	Original Version
** 03-Mar-2021	Sanjay Janardhan	Added Additional information for email content
******************************************************************************************/
/*
		EXEC [ETLProcess].[EmailNotification] @IsError='No',@PipelineName='ETLLoad'
		EXEC [ETLProcess].[EmailNotification] @IsError='No',@PipelineName='Profisee_MS_Started'
		EXEC [ETLProcess].[EmailNotification] @IsError='No',@PipelineName='Profisee_MS_Completed'
*/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DROP TABLE IF EXISTS #StageProcessinErrRecords;
	DROP TABLE IF EXISTS #EntityInvalidRecords;
	DROP TABLE IF EXISTS #EmailList;
	
	DECLARE @EmailId VARCHAR(8000);
	DECLARE @TextId VARCHAR(8000);
	DECLARE @SubjectText VARCHAR(200);
	DECLARE @TextMail VARCHAR(1000);
	DECLARE @ErrorBody VARCHAR(8000);
	DECLARE @SysServerName VARCHAR(50);
	DECLARE @ServerName VARCHAR(50);
	DECLARE @EndTime DATETIME;
	DECLARE @xml NVARCHAR(MAX)
	DECLARE @xml2 NVARCHAR(MAX)
	DECLARE @xml3 NVARCHAR(MAX)
	DECLARE @xml4 NVARCHAR(MAX)
	DECLARE @xmlErr NVARCHAR(MAX)
	DECLARE @body NVARCHAR(MAX)
	DECLARE @Query VARCHAR(MAX)
	DECLARE @SQL VARCHAR(MAX)
	DECLARE @JobName VARCHAR(200)
	DECLARE @StepNumber VARCHAR(200)
	DECLARE @StepName VARCHAR(200)
	
	DECLARE @RunDate VARCHAR(200)
	DECLARE @RunTime VARCHAR(200)
	DECLARE @DFStartTime DATETIME
	
	DECLARE @STime VARCHAR(20), @ETime VARCHAR(20), @Minutes INT,@Hours VARCHAR(20),@StgDate VARCHAR(20),@BatchName VARCHAR(100)

	SET @EmailId = '';
	SET @TextId = '';

	CREATE TABLE #EmailList
	(
		EmailId VARCHAR(8000)
	);

	IF @IsError='Yes'
		BEGIN	
			INSERT INTO #EmailList
				SELECT DISTINCT 
					ETLNotifications.EmailId
				FROM 
					ETLProcess.ETLNotifications ETLNotifications
				WHERE
					ETLNotifications.ActiveFlag = 1
					AND ETLNotifications.Category IN('PS','FAI','DEV','ITNOC')

			SELECT @EmailId = @EmailId + EmailId + ';' FROM #EmailList;

			IF RIGHT(@EmailId, 1) = ';'
				SET @EmailId = LEFT(@EmailId, DATALENGTH(@EmailId) - 1);

			IF DATALENGTH(@EmailId) > 0
				BEGIN	
					SET @SubjectText = 'DTC - Data Load Errors ON ' + @@SERVERNAME;
					
					SELECT @ErrorBody = 
						' Failed Date :'+CAST(GETDATE() AS VARCHAR(20))
						+' ProcessCategory : ' + @ProcessCategory 
						+' Process: ' + @ProcessName 
						+' Process Stage : '+@ProcessStage;

					SET @xml = CAST(
					(	SELECT 
							'left'  AS 'td/@align',@ProcessCategory AS 'td',''
						,	'left' AS 'td/@align',@ProcessName AS 'td',''
						,	'left' AS 'td/@align',@ProcessStage AS 'td',''
						,	'left' AS 'td/@align',@ErrorMessage AS 'td',''
						,	'left' AS 'td/@align',CAST(GETDATE() AS VARCHAR(20)) AS 'td'
						FOR XML PATH('tr'), ELEMENTS
					) AS NVARCHAR(MAX)
					)
			
					SET @body ='<html>
						<body>
							<p style = "font-family: Cambria; font-size: 14px;">
								Error Detail:
							</p>
						<style>
							table, th, td 
								{
								border: 1px solid black;
								border-collapse: collapse;
								}
							table th
								{
								background-color: #008080;
								color: white;
								font-weight: bold;
								font-family: Cambria;
								font-size: 15px;
								}
							table td
								{
								font-weight: normal;
								font-size: 15px;
								font-family: Cambria;
								background-color: #eaf2f8;
								padding-right: 2px;
								}
						</style>
						<table border = 1px> 

						<tr>
							<th> ProcessCategory </th> <th> Process </th> <th> Process Stage </th> <th> Error </th> <th> Date </th>
						</tr>'

					SET @body = @body + @xml +'</table>
						</body></html>'

					EXEC msdb.dbo.sp_send_dbmail
						@profile_name = 'AzureManagedInstance_dbmail_profile'
					,	@recipients = @EmailId
					,	@subject = @SubjectText
					--,	@body = @ErrorBody;
					,	@body = @body
					,	@body_format ='HTML'

				END;
			ELSE
				BEGIN
					RAISERROR('EmailID not found', 16, 1);
				END;
		END
	ELSE
		BEGIN
			INSERT INTO #EmailList
				SELECT DISTINCT 
					ETLNotifications.EmailId
				FROM 
					ETLProcess.ETLNotifications ETLNotifications
				WHERE
					ETLNotifications.ActiveFlag = 1
					AND ETLNotifications.Category <>'ITNOC';				

			SELECT @EmailId = @EmailId + EmailId + ';' FROM #EmailList;

			IF RIGHT(@EmailId, 1) = ';'
				SET @EmailId = LEFT(@EmailId, DATALENGTH(@EmailId) - 1);

			IF DATALENGTH(@EmailId) > 0
				BEGIN	
					DECLARE @SqlStatement NVARCHAR(MAX)=N'', @ScehmaName VARCHAR(100)=N''
					DECLARE @StartedAt DATETIME;
					DECLARE @StartedRunID INT;
					DECLARE @CompletedRunID INT;
					DECLARE @CompletedAt DATETIME;
					DECLARE @Duration INT;
					DECLARE @InHours  VARCHAR(20);
					DECLARE @LoadStep VARCHAR(100);
					DECLARE @CheckProcessCategoryName VARCHAR(100);
					DECLARE @CheckStartAt VARCHAR(100);

					IF @PipelineName='ETLLoad'
					BEGIN						
						CREATE TABLE #StageProcessinErrRecords(ProcessName VARCHAR(100),ErrCount VARCHAR(100));
						CREATE TABLE #EntityInvalidRecords(TableName VARCHAR(100),ErrCount VARCHAR(100));

						SET @SubjectText = 'DTC - ETL Load Completed ON ' + @@SERVERNAME;
						SET @LoadStep ='ETL Load';
						
						SELECT
							@StartedAt =A.UTC_StartedAt
						,	@StartedRunID = a.RunId
						FROM
							ETLAudit.ETLProcessCategory A	
						WHERE
							A.RunId=( SELECT
										MAX(A.RunID)
									FROM
										ETLProcess.ETLProcessCategory P

										INNER JOIN ETLAudit.ETLProcessCategory A
										ON P.ProcessCategoryId = A.ProcessCategoryId

										INNER JOIN ETLProcess.ETLStatus 
										ON ETLStatus.StatusId = A.CurrentStatus
									WHERE
										ProcessCategoryName IN('DTC_InternalSource_ETL','DTC_ExternalSource_ETL')
										AND ETLStatus.Status='Completed');
											
						IF @StartedRunID<>1 
						BEGIN
							SELECT 
								@CheckProcessCategoryName = ETLProcessCategory.ProcessCategoryName 
							,	@CheckStartAt = A.UTC_StartedAt
							FROM 
								ETLAudit.ETLProcessCategory A 
	
								INNER JOIN ETLProcess.ETLProcessCategory 
								ON ETLProcessCategory.ProcessCategoryId=A.ProcessCategoryId 
							WHERE 
								RunId = @StartedRunID-1;
								
							IF @CheckProcessCategoryName='DTC_ExternalSource_ETL'
							BEGIN
								SET @StartedRunID = @StartedRunID-1;
								SET @StartedAt=@CheckStartAt;
							END
						END
						
						--Get completed time
						SELECT
							@CompletedAt =A.UTC_CompletedAt
						,	@CompletedRunID = a.RunId
						FROM
							ETLAudit.ETLProcessCategory A	
						WHERE
							A.RunId=( 
									SELECT							
										MAX(A.RunID)
									FROM
										ETLProcess.ETLProcessCategory P

										INNER JOIN ETLAudit.ETLProcessCategory A
										ON P.ProcessCategoryId = A.ProcessCategoryId

										INNER JOIN ETLProcess.ETLStatus 
										ON ETLStatus.StatusId = A.CurrentStatus
									WHERE
										ProcessCategoryName IN('DTC_Profisee_MS_Automation','DTC_Profisee_MS_Incremental_ETL','DTC_ProfiseeDataLoad_ETL')
										AND ETLStatus.Status='Completed'
										AND a.RunId > @StartedRunID
										)

						SET @Duration = DATEDIFF(MI,@StartedAt,@CompletedAt)

						SET @InHours = 
							CASE WHEN @Duration >= 60 THEN  (SELECT CAST((@Duration / 60) AS VARCHAR(2)) + 'h ' +  CASE WHEN (@Duration % 60) > 0 THEN CAST((@Duration % 60) AS VARCHAR(2)) + 'm' ELSE ''   END)
								ELSE  CAST((@Duration % 60) AS VARCHAR(2)) + 'm'
							END											
							
						SET @xml = CAST(
							(	SELECT 
									'left'  AS 'td/@align',@LoadStep AS 'td',''
								,	'left' AS 'td/@align',CONVERT(VARCHAR, @StartedAt,100) AS 'td',''
								,	'left' AS 'td/@align',CONVERT(VARCHAR, @CompletedAt,100)  AS 'td',''
								,	'left' AS 'td/@align',@InHours AS 'td',''								
								FOR XML PATH('tr'), ELEMENTS
							) AS NVARCHAR(MAX)
						)
			
						SET @body ='<html>
								<body>
									<p style = "font-family: Cambria; font-size: 14px;">
										Detail:
									</p>
								<style>
									table, th, td 
										{
										border: 1px solid black;
										border-collapse: collapse;
										}
									table th
										{
										background-color: #008080;
										color: white;
										font-weight: bold;
										font-family: Cambria;
										font-size: 15px;
										}
									table td
										{
										font-weight: normal;
										font-size: 15px;
										font-family: Cambria;
										background-color: #eaf2f8;
										padding-right: 2px;
										}
								</style>
								<table border = 1px> 

								<tr>
									<th> LoadStep </th> <th> Started Time </th> <th> Completed Time </th> <th> Duration </th>
								</tr>'

							--StageProcessingErr records
							SET @SqlStatement=N''
							SET @ScehmaName='StageProcessingErr';
							
							SELECT @SqlStatement = 
								COALESCE(@SqlStatement, N'') + N'SELECT '''+TABLE_NAME+N''' as ProcessName,COUNT(1) as ErrCount FROM '+@ScehmaName+N'.' + QUOTENAME(TABLE_NAME) + N' union ' + CHAR(13)
							FROM INFORMATION_SCHEMA.TABLES
							WHERE TABLE_SCHEMA = @ScehmaName and TABLE_TYPE = 'BASE TABLE'
							ORDER BY TABLE_NAME;
							
							SET @SqlStatement = N' INSERT INTO #StageProcessinErrRecords SELECT * FROM ('+ LEFT(@SqlStatement,LEN(@SqlStatement)-8)+N') Rec WHERE ErrCount > 0';							
							EXECUTE(@SqlStatement);

							IF (SELECT COUNT(1) FROM #StageProcessinErrRecords)=0
							BEGIN
								INSERT INTO #StageProcessinErrRecords
									SELECT '',''
							END
							

							--Invalid Entity Table Count
							SET @SqlStatement=N''
							SET @ScehmaName='dbo'
							
							SELECT @SqlStatement = 
								COALESCE(@SqlStatement, N'') + N'SELECT '''+TABLE_NAME+''' TableName, COUNT(1) ErrCount FROM '+@ScehmaName+'.' + QUOTENAME(TABLE_NAME) + N' NOLOCK WHERE ISNULL(IsPermanentlyInvalid,0)=0 UNION ' + CHAR(13)
							FROM INFORMATION_SCHEMA.TABLES
							WHERE TABLE_SCHEMA = @ScehmaName AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME IN('Address_Invalid','Building_Invalid','Business_Invalid','Listing_Invalid','Parcel_Invalid','PIN_Invalid','Property_Invalid','Sales_Invalid','Taxation_Invalid','Valuation_Invalid')
							
							SET @SqlStatement = N' INSERT INTO #EntityInvalidRecords SELECT * FROM ('+ LEFT(@SqlStatement,LEN(@SqlStatement)-8)+N') Rec ';							
							EXECUTE(@SqlStatement);
							
							IF (SELECT COUNT(1) FROM #EntityInvalidRecords)=0
							BEGIN
								INSERT INTO #EntityInvalidRecords
									SELECT '',''
							END

							SET @xml2 = CAST(
								(	SELECT 
										'left'  AS 'td/@align',ProcessName AS 'td',''
									,	'left' AS 'td/@align',ErrCount AS 'td',''								
									FROM
										#StageProcessinErrRecords
									FOR XML PATH('tr'), ELEMENTS
								) AS NVARCHAR(MAX)
							)

							SET @xml3 = CAST(
								(	SELECT 
										'left'  AS 'td/@align',TableName AS 'td',''
									,	'left' AS 'td/@align',ErrCount AS 'td',''													
									FROM
										#EntityInvalidRecords
									FOR XML PATH('tr'), ELEMENTS
								) AS NVARCHAR(MAX)
							)

							SET @xml4 = CAST(
								(	
									SELECT 
										'left'  AS 'td/@align', A.RunId AS 'td',''
									,	'left'  AS 'td/@align', P.ProcessCategoryName AS 'td',''
									,	'left'  AS 'td/@align', DATEDIFF(MINUTE,A.UTC_StartedAt,A.UTC_CompletedAt) AS 'td',''
									FROM 
										ETLAudit.ETLProcessCategory A
										INNER JOIN ETLProcess.ETLProcessCategory P
										ON P.ProcessCategoryId = A.ProcessCategoryId
									WHERE
										A.RunId >=@StartedRunID
										AND A.RunId <=@CompletedRunID
									ORDER BY RunId ASC							
									FOR XML PATH('tr'), ELEMENTS
								) AS NVARCHAR(MAX)
							)


							SET @body = @body + @xml +'</table>
								<p style = "font-family: Cambria; font-size: 14px;">
									<br>
									Source Level Invalid records at StageProcessingErr:
								</p>

								<table border = 1px> 

								<tr><th> Source System </th> <th> Source Record Count </th></tr>'

							SET @body = @body + @xml2 +'</table>
								<p style = "font-family: Cambria; font-size: 14px;">
									<br>
									Entity Level Invalid:
								</p>

								<table border = 1px> 

								<tr><th> Table Name System </th> <th> Error Record Count </th></tr>'

							SET @body = @body + @xml3 +'</table>
								<p style = "font-family: Cambria; font-size: 14px;">
									<br>
									Category Level Duration:
								</p>

								<table border = 1px> 

								<tr><th> RunID </th> <th> ProcessCategoryName </th> <th> Duraion(In Minutes) </th></tr>'

							SET @body = @body + @xml4+'</table>
								</body></html>'
						END

					ELSE IF @PipelineName='Profisee_MS_Started'
						BEGIN
							SET @SubjectText = 'DTC - Profisee Matching & Survivorship Started ON ' + @@SERVERNAME;

							SET @body ='<html>
								<body>
									<p style = "font-family: Cambria; font-size: 14px;">
										Profisee Matching & Survivorship Started
									</p>
								<style>
									table, th, td 
										{
										border: 1px solid black;
										border-collapse: collapse;
										}
									table th
										{
										background-color: #008080;
										color: white;
										font-weight: bold;
										font-family: Cambria;
										font-size: 15px;
										}
									table td
										{
										font-weight: normal;
										font-size: 15px;
										font-family: Cambria;
										background-color: #eaf2f8;
										padding-right: 2px;
										}
								</style>
								<table border = 1px> '

							SET @body = @body +'</table>
								</body></html>'
						END
					ELSE IF @PipelineName='Profisee_MS_Completed'
						BEGIN
							SET @SubjectText = 'DTC - Profisee Matching & Survivorship Completed ON ' + @@SERVERNAME;							
							SET  @LoadStep ='Profisee M&S';
							
							SELECT
								@StartedAt =A.UTC_StartedAt
							,	@StartedRunID = a.RunId
							,	@CompletedAt =A.UTC_CompletedAt
							FROM
								ETLAudit.ETLProcessCategory A	
							WHERE
								A.RunId=( SELECT
											MAX(A.RunID)
										FROM
											ETLProcess.ETLProcessCategory P

											INNER JOIN ETLAudit.ETLProcessCategory A
											ON P.ProcessCategoryId = A.ProcessCategoryId

											INNER JOIN ETLProcess.ETLStatus 
											ON ETLStatus.StatusId = A.CurrentStatus
										WHERE
											ProcessCategoryName='DTC_Profisee_MS_Automation'
											AND ETLStatus.Status='Completed');
						
	
							SET @Duration = DATEDIFF(MI,@StartedAt,@CompletedAt)

							SET @InHours = 
								CASE WHEN @Duration >= 60 THEN  (SELECT CAST((@Duration / 60) AS VARCHAR(2)) + 'h ' +  CASE WHEN (@Duration % 60) > 0 THEN CAST((@Duration % 60) AS VARCHAR(2)) + 'm' ELSE ''   END)
									ELSE  CAST((@Duration % 60) AS VARCHAR(2)) + 'm'
								END												


							SET @xml = CAST(
							(	SELECT 
									'left'  AS 'td/@align',@LoadStep AS 'td',''
								,	'left' AS 'td/@align',CONVERT(VARCHAR, @StartedAt,100) AS 'td',''
								,	'left' AS 'td/@align',CONVERT(VARCHAR, @CompletedAt,100)  AS 'td',''
								,	'left' AS 'td/@align',@InHours AS 'td',''								
								FOR XML PATH('tr'), ELEMENTS
							) AS NVARCHAR(MAX)
							)

							SET @xml2 = CAST(
							(	SELECT 
									'left'  AS 'td/@align', A.RunId AS 'td',''
								,	'left'  AS 'td/@align', P.ProcessName AS 'td',''
								,	'left'  AS 'td/@align', DATEDIFF(MINUTE,A.UTC_StartedAt,A.UTC_CompletedAt) AS 'td',''
								FROM 
									ETLAudit.ETLProcess A
	
									INNER JOIN ETLProcess.ETLProcess P
									ON P.ProcessId = A.ProcessId
								WHERE
									A.RunId =(SELECT MAX(RunId) FROM ETLAudit.ETLProcessCategory A INNER JOIN ETLProcess.ETLProcessCategory P ON a.ProcessCategoryId = p.ProcessCategoryId WHERE P.ProcessCategoryName='DTC_Profisee_MS_Automation' )
								ORDER BY 
									RunId ASC							
								FOR XML PATH('tr'), ELEMENTS
							) AS NVARCHAR(MAX)
							)
			
							SET @body ='<html>
								<body>
									<p style = "font-family: Cambria; font-size: 14px;">
										Detail:
									</p>
								<style>
									table, th, td 
										{
										border: 1px solid black;
										border-collapse: collapse;
										}
									table th
										{
										background-color: #008080;
										color: white;
										font-weight: bold;
										font-family: Cambria;
										font-size: 15px;
										}
									table td
										{
										font-weight: normal;
										font-size: 15px;
										font-family: Cambria;
										background-color: #eaf2f8;
										padding-right: 2px;
										}
								</style>
								<table border = 1px> 

								<tr>
									<th> LoadStep </th> <th> Started Time </th> <th> Completed Time </th> <th> Duration </th>
								</tr>'

							SET @body = @body + @xml +'</table>
								<p style = "font-family: Cambria; font-size: 14px;">
									<br>
									Entity Level M&S Duration:
								</p>

								<table border = 1px> 

								<tr><th> RunID </th> <th> M&S Entity Name </th> <th> Duraion(In Minutes) </th></tr>
								'

							
							SET @body = @body + @xml2 +'</table>
								</body></html>'
							
						END		
					
					EXEC msdb.dbo.sp_send_dbmail
						@profile_name = 'AzureManagedInstance_dbmail_profile'
					,	@recipients = @EmailId
					,	@subject = @SubjectText					
					,	@body = @body
					,	@body_format ='HTML';
				END
			ELSE
				BEGIN
					RAISERROR('EmailID not found', 16, 1);
				END;
		END
END