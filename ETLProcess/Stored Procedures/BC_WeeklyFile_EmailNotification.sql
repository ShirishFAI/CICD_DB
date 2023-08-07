CREATE PROCEDURE ETLProcess.BC_WeeklyFile_EmailNotification
	@IsError VARCHAR(3) 
,	@FileName VARCHAR(200)
,	@Stage VARCHAR(200)=''
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Shirish Waghmale
-- DATE			: 02/08/2023
-- PURPOSE		: Email Notifiation for BC Weekly File	
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 25-Sep-2020	Shirish Waghmale	Original Version
******************************************************************************************/
	
	DECLARE @EmailId VARCHAR(8000);
	DECLARE @TextId VARCHAR(8000);
	DECLARE @SubjectText VARCHAR(200);
	DECLARE @ErrorBody VARCHAR(8000);
	DECLARE @body NVARCHAR(MAX);

	DROP TABLE IF EXISTS #EmailList;

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
				SET @SubjectText = 'DTC - BC Weekly File Load Errors ON ' + @@SERVERNAME;

				SELECT @ErrorBody = 
						'<html>
						<body>
							<b>***Weekly File Processing Failed***</b><br><br>  <b>Failed Date</b>: '+CAST(GETDATE() AS VARCHAR(20))
						+'<br><b>FileName</b>: ' + @FileName+'</body></html>';

				SET @body = 'Error';

				EXEC msdb.dbo.sp_send_dbmail
						@profile_name = 'AzureManagedInstance_dbmail_profile'
					,	@recipients = @EmailId
					,	@subject = @SubjectText
					,	@body = @ErrorBody
					--,	@body = @body
					,	@body_format ='HTML'

				END

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
					
					IF DATALENGTH(@EmailId) > 0 AND @Stage='Weekly File Processing Started'
							BEGIN
								SET @SubjectText = 'DTC - BC Weekly File Processing Started ON ' + @@SERVERNAME;
								SET @body='<html>
								<body>
								<b>***BC Weekly File Processing Started***</b><br><br> <b>Start Date:</b> '+CAST(GETDATE() AS VARCHAR(20))
								+'<br><b>FileName:</b> ' + @FileName+'</body></html>';

									EXEC msdb.dbo.sp_send_dbmail
										@profile_name = 'AzureManagedInstance_dbmail_profile'
									,	@recipients = @EmailId
									,	@subject = @SubjectText					
									,	@body = @body
									,	@body_format ='HTML';
							
							END

						IF DATALENGTH(@EmailId) > 0 AND @Stage='Weekly File Processing Completed'
						BEGIN
							SET @SubjectText = 'DTC - BC Weekly File Processing Completed ON ' + @@SERVERNAME;
							SET @body='<html>
						<body>
						<b>***BC Weekly File Processing Completed***</b><br><br> <b>End Date:</b> '+CAST(GETDATE() AS VARCHAR(20))
						+'<br><b>FileName:</b> ' + @FileName+'</body></html>';

							EXEC msdb.dbo.sp_send_dbmail
								@profile_name = 'AzureManagedInstance_dbmail_profile'
							,	@recipients = @EmailId
							,	@subject = @SubjectText					
							,	@body = @body
							,	@body_format ='HTML';
						END

					END

	END