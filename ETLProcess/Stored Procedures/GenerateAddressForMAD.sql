CREATE PROCEDURE [ETLProcess].[GenerateAddressForMAD]	
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Generate Addresses for MAD
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @RunDate DATETIME;
	DECLARE @RunDateFormatted VARCHAR(50);
	DECLARE @ProcessName VARCHAR(100)='PushToMAD';
	DECLARE @ProcessCategory VARCHAR(100)='DTC_MasterAddress_ETL';
	DECLARE @Inserted BIGINT;
	DECLARE @Updated BIGINT;
	
	DECLARE @ErrorProcedure VARCHAR(100);

	SELECT 
	@ErrorProcedure= s.name+'.'+o.name 
	FROM 
	SYS.OBJECTS O 
	INNER JOIN SYS.SCHEMAS S 
	ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;

	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'Process'
	,	@ProcessName = @ProcessName
	,	@Stage = 'GenerateAddressForMAD'
	,	@Status = 'InProgress'
	,	@CurrentStatus = 'Started'	;

	BEGIN TRY
		TRUNCATE TABLE Stage.Address;

		SET @RunDate=GETUTCDATE()
		SET @RunDateFormatted=FORMAT(@RunDate, N'yyyyMMddHHmmss')	;
	
		INSERT INTO Stage.MADSourceInstanceDetails
			SELECT
				RunId
			,	'DTC_' +RunId+'_' + @RunDateFormatted
			,	COUNT(1) AddressToProcess
			,	@RunDate
			,	'SentMAD'
			FROM
				(
				SELECT 
					SUBSTRING(Code,CHARINDEX('_',Code)+1,(((LEN(Code))-CHARINDEX('_', REVERSE(Code)))-CHARINDEX('_',Code)))AS RunId
				FROM 
					dbo.Address
				WHERE
					ISNULL(IsMADSent,0)=0
					--AND ID IN (SELECT TOP 1000 id FROM dbo.Address WHERE ISNULL(IsMADSent,0)=0 ORDER BY ID  )
				) SourceInstanceDetails
			GROUP BY
				RunId;
			
		--Get the Address ready to picked by ADF
		INSERT INTO Stage.Address
			SELECT
				'DTC_' + SUBSTRING(Code,CHARINDEX('_',Code)+1,(((LEN(Code))-CHARINDEX('_', REVERSE(Code)))-CHARINDEX('_',Code))) +'_' + @RunDateFormatted as FileName
			,	'DTC'  SourceName
			,	'DataTreeCanada'  SourceDesc
			,	'DTCAddress' SubSourceName
			,	'Address from DTC in CSV' SubSourceDesc
			,	'Address from DTC in CSV' SubSourceType
			,	'DTC_' + SUBSTRING(Code,CHARINDEX('_',Code)+1,(((LEN(Code))-CHARINDEX('_', REVERSE(Code)))-CHARINDEX('_',Code))) +'_' + @RunDateFormatted SourceInstanceDetails
			,	CASE 
					WHEN NULLIF(FullAddress,'') IS NULL THEN CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') )
					--WHEN NULLIF(FullAddress,'') IS NULL THEN TRIM(REPLACE(ISNULL(UnitNumber,'')+' '+ISNULL(StreetNumber,'')+' '+ISNULL(StreetName,''),'  ', ' '))  
					ELSE FullAddress	
				END AS AddressLine
			,	PostalCode
			,	City
			,	ProvinceCode AS Province
			,	Code AS SourceAddressID
			,	Latitude
			,	Longitude
			,	@RunDate
			FROM
				dbo.Address
			WHERE
				ISNULL(IsMADSent,0)=0
				--AND ID IN (SELECT TOP 1000 id FROM dbo.Address WHERE ISNULL(IsMADSent,0)=0 ORDER BY ID  )			

			EXEC ETLProcess.AuditLog
				@ProcessCategory = @ProcessCategory
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = 'GenerateAddressForMAD'
			,	@Status = 'Completed'
			,	@CurrentStatus = 'Completed'	
			,	@Inserts=@@ROWCOUNT;
		END TRY
		
		BEGIN CATCH
			DELETE 
			FROM
				Stage.MADSourceInstanceDetails
			WHERE
				SourceInstanceDetails='DTC_' +RunId+'_' + @RunDateFormatted

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
				@ProcessCategory
			,	@ProcessName
			,	ERROR_NUMBER() AS ErrorNumber  
			,	ERROR_SEVERITY() AS ErrorSeverity  
			,	ERROR_STATE() AS ErrorState  
			,	@ErrorProcedure
			,	ERROR_LINE() AS ErrorLine  
			,	ERROR_MESSAGE() AS ErrorMessage
			,	GETDATE()
			
			EXEC ETLProcess.AuditLog
				@ProcessCategory = @ProcessCategory
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = 'GenerateAddressForMAD'
			,	@Status = 'Error'
			,	@CurrentStatus = 'Error';

			EXEC ETLProcess.EmailNotification
				@ProcessCategory=@ProcessCategory
			,	@ProcessName= @ProcessName
			,	@ProcessStage='GenerateAddressForMAD'
			,	@ErrorMessage='Failed to Generate Address for MAD'
			,	@IsError='Yes';

			THROW 50000, N'Failed to Generate Address for MAD', 1;

		END CATCH
END