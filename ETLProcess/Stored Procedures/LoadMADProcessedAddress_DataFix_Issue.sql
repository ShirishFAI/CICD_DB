
CREATE PROCEDURE [ETLProcess].[LoadMADProcessedAddress_DataFix_Issue]	
	@SourceInstanceDetails  VARCHAR(100)
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Load dbo.MADAddress
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
** 02/19/2020	Sanjay Janardhan	Updated the matched records in MADAddress with the GeoCode if they are missing
** 03/15/2020	Sanjay Janardhan	Added ProvinceCode to update in Address
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @ProcessName VARCHAR(100)='PullFromMAD';
	DECLARE @ProcessCategory VARCHAR(100)='DTC_MasterAddress_ETL';
	DECLARE @Inserted BIGINT;
	DECLARE @Updated BIGINT;
	DECLARE @RunId INT;
	DEClare @ProcessCategoryStartedAt DATETIME;
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @IsError BIT=0;

	DROP TABLE IF EXISTS #Address;
	DROP TABLE IF EXISTS #GetDuplicates;
	DROP TABLE IF EXISTS #DistinctAddressRN;
	DROP TABLE IF EXISTS #DistinctAddress;
	DROP TABLE IF EXISTS #DeDuplicates;
	DROP TABLE IF EXISTS #StageMADAddressWithGeo;

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
	,	@Stage = 'MADAddressToAddress'
	,	@Status = 'InProgress'
	,	@CurrentStatus = 'Started'	;

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
		@ProcessCategoryStartedAt=UTC_StartedAt
	FROM
		ETLAudit.ETLProcessCategory AuditProcessCategory		
	WHERE
		AuditProcessCategory.RunId=@RunId;

	IF @ProcessCategoryStartedAt IS NULL
		SET @ProcessCategoryStartedAt = GETUTCDATE();

	BEGIN TRY
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Get matched dbo.MADAddress with missing GeoCode'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		SELECT
			StageMAD.Latitude as NewLatitude
		,	StageMAD.Longitude as NewLongitude
		,	CAST(StageMAD.MADAddressID AS VARCHAR(50)) as MADAddressID
		INTO
			#StageMADAddressWithGeo
		FROM
			Stage.MADAddress StageMAD

			INNER JOIN dbo.MADAddress MADAddress
			ON StageMAD.MADAddressID = MADAddress.MADAddressID
			AND StageMAD.SourceAddressID <> MADAddress.SourceAddressID
			AND
			(
					StageMAD.Latitude		<>MADAddress.Latitude
				OR	StageMAD.Longitude		<>MADAddress.Longitude			
			);

		SET @Inserted =@@ROWCOUNT;

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed getting matched dbo.MADAddress with missing GeoCode'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Inserts = @Inserted
		,	@Updates = @Updated;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Update matched dbo.MADAddress which are missing GeoCode'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		UPDATE
			MADAddress
		SET
			MADAddress.Latitude = GeoCodes.NewLatitude
		,	MADAddress.Longitude =GeoCodes.NewLongitude
		,	MADAddreSS.LastModifiedDateUTC = @ProcessCategoryStartedAt
		FROM	
			MADAddress

			INNER JOIN #StageMADAddressWithGeo GeoCodes
			ON MADAddress.MADAddressID = GeoCodes.MADAddressID;		

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed updating matched dbo.MADAddress with GeoCode'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Inserts = 0
		,	@Updates = @@ROWCOUNT;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Update matched dbo.Address with LastModifiedDateUTC'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		UPDATE
			Address
		SET
			Address.LastModifiedDateUTC = @ProcessCategoryStartedAt
		FROM	
			dbo.Address

			INNER JOIN #StageMADAddressWithGeo GeoCodes
			ON Address.MasterAddressID = GeoCodes.MADAddressID;
			
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed updating matched dbo.Address with LastModifiedDateUTC'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Inserts = 0
		,	@Updates = @@ROWCOUNT;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Start Loading dbo.MADAddress'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		--Get UPDATES
		UPDATE
			MADAddress
		SET
			MADAddress.MADAddressID			=	MADStage.MADAddressID
		,	MADAddress.UnitNumber			=	MADStage.UnitNumber
		,	MADAddress.StreetNumber			=	MADStage.StreetNumber
		,	MADAddress.StreetName			=	MADStage.StreetName
		,	MADAddress.StreetType			=	MADStage.StreetType
		,	MADAddress.StreetDirection		=	MADStage.StreetDirection
		,	MADAddress.FSA					=	MADStage.FSA
		,	MADAddress.PostalCode			=	MADStage.PostalCode
		,	MADAddress.City					=	MADStage.City
		,	MADAddress.ProvinceCode			=	MADStage.ProvinceCode
		,	MADAddress.Country				=	MADStage.Country
		,	MADAddress.Latitude				=	MADStage.Latitude
		,	MADAddress.Longitude			=	MADStage.Longitude
		,	MADAddress.CityAlternative		=	MADStage.CityAlternative
		,	MADAddress.CityNameFR			=	MADStage.CityNameFR
		,	MADAddress.ProvinceNameFR		=	MADStage.ProvinceNameFR
		,	MADAddress.Filename				=	MADStage.Filename
		,	MADAddress.FullAddress			=	MADStage.FullAddress
		,	MADAddress.UnitCode				=	MADStage.UnitCode
		,	MADAddress.CreatedDate			=	MADStage.CreatedDate
		,	MADAddress.UpdatedDate			=	MADStage.UpdatedDate
		,	MADAddress.Status				=	MADStage.Status
		,	MADAddress.NewAddressID			=	MADStage.NewAddressID
		,	MADAddress.LastModifiedDateUTC	= 	@ProcessCategoryStartedAt
		FROM 
			Stage.MADAddress MADStage

			INNER JOIN dbo.MADAddress MADAddress
			ON MADStage.SourceAddressId = MADAddress.SourceAddressID;

		SET @Updated =@@ROWCOUNT;

		--Get INSERTS
		INSERT INTO dbo.MADAddress
		(	
			SourceAddressID
		,	MADAddressID
		,	UnitNumber
		,	StreetNumber
		,	StreetName
		,	StreetType
		,	StreetDirection
		,	FSA
		,	PostalCode
		,	City
		,	ProvinceCode
		,	Country
		,	Latitude
		,	Longitude
		,	CityAlternative
		,	CityNameFR
		,	ProvinceNameFR
		,	Filename
		,	FullAddress
		,	UnitCode
		,	CreatedDate
		,	UpdatedDate
		,	Status
		,	NewAddressID
		,	DateCreatedUTC
		,	LastModifiedDateUTC
		)
		SELECT
			SourceAddressID
		,	MADAddressID
		,	UnitNumber
		,	StreetNumber
		,	StreetName
		,	StreetType
		,	StreetDirection
		,	FSA
		,	PostalCode
		,	City
		,	ProvinceCode
		,	Country
		,	Latitude
		,	Longitude
		,	CityAlternative
		,	CityNameFR
		,	ProvinceNameFR
		,	Filename
		,	FullAddress
		,	UnitCode
		,	CreatedDate
		,	UpdatedDate
		,	Status
		,	NewAddressID
		,	@ProcessCategoryStartedAt
		,	@ProcessCategoryStartedAt
		FROM
			Stage.MADAddress MADStage
		WHERE
			NOT EXISTS(SELECT 1 FROM dbo.MADAddress WHERE MADAddress.SourceAddressID=MADStage.SourceAddressId);
		
		SET @Inserted =@@ROWCOUNT;

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed Loading dbo.MADAddress'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Inserts = @Inserted
		,	@Updates = @Updated;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Start updating dbo.Address with MADID'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		--Update Actual MAD Found records
		UPDATE 
			[Address]
		SET 
			[Address].MasterAddressID = MADAddress.MADAddressID
		,	[Address].IsMADReceived=1
		,	[Address].MADReceivedDateUTC = MADAddress.LastModifiedDateUTC
		,	[Address].LastModifiedDateUTC = @ProcessCategoryStartedAt
		,	[Address].ProvinceCode = MADAddress.ProvinceCode
		FROM
			dbo.[Address]

			INNER JOIN dbo.MADAddress
			ON [Address].Code = MADAddress.SourceAddressId
		WHERE			
			IsMADReceived IS NULL
			AND MADAddress.MADAddressID IS NOT NULL;

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed Updating dbo.Address with MADID'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Updates = @@ROWCOUNT;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Start updating dbo.Address for similar address'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		--To check if similar address already exists in Address
		;WITH AddressWithoutMAD  AS
		(
			SELECT	
				CASE WHEN NULLIF(FullAddress,'') IS NULL THEN CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') ) ELSE FullAddress	END AS AddressLine
			,	MasterAddressID
			,	ISNULL(City,'') City
			,	ISNULL(PostalCode,'') PostalCode
			,	ISNULL(ProvinceCode,'') ProvinceCode
			,	LastModifiedDateUTC
			FROM
				dbo.Address 
			WHERE
				MasterAddressID IS NULL
		)
		UPDATE AddressWithoutMAD
		SET
			AddressWithoutMAD.MasterAddressID = AddressExisting.MasterAddressID
		,	AddressWithoutMAD.LastModifiedDateUTC = @ProcessCategoryStartedAt		
		FROM
			AddressWithoutMAD

			INNER JOIN
			(	SELECT
					CASE WHEN NULLIF(FullAddress,'') IS NULL THEN CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') ) ELSE FullAddress END AS AddressLine
				,	ISNULL(MasterAddressID,'') MasterAddressID
				,	ISNULL(City,'') City
				,	ISNULL(PostalCode,'') PostalCode
				,	ISNULL(ProvinceCode,'') ProvinceCode
				FROM
					dbo.Address AddressExisting
				WHERE
					MasterAddressID IS NOT NULL
			) AddressExisting
			ON AddressWithoutMAD.AddressLine	= AddressExisting.AddressLine
			AND AddressWithoutMAD.City			= AddressExisting.City
			AND AddressWithoutMAD.PostalCode	= AddressExisting.PostalCode
			AND AddressWithoutMAD.ProvinceCode	= AddressExisting.ProvinceCode;
		
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed Updating dbo.Address for similar address'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Updates = @@ROWCOUNT;

		
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Get addresses which dont have MAD'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;
		
		
		--DeDuplication on IsReliable
		SELECT
			ID
		,	CASE WHEN NULLIF(FullAddress,'') IS NULL THEN CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') ) ELSE FullAddress END AS AddressLine
		,	ISNULL(City,'') City
		,	ISNULL(PostalCode,'')	PostalCode	
		,	ISNULL(ProvinceCode,'')	 ProvinceCode
		,	DateCreatedUTC
		INTO
			#Address
		FROM
			dbo.[Address]

			INNER JOIN ETLProcess.ETLProcess
			ON ETLProcess.ProcessId =  [Address].Data_Source_ID
		WHERE			
			[Address].MasterAddressID IS NULL
			AND ETLProcess.IsAddressReliable=1;

		SELECT   
			MIN(ID) ID
		,	AddressLine
		,	City			
		,	PostalCode		
		,	ProvinceCode	
		,	DateCreatedUTC  AS AddressInsertTS
		INTO 
			#GetDuplicates
		FROM 
			#Address
		GROUP BY 
			AddressLine
		,	City			
		,	PostalCode		
		,	ProvinceCode		
		,   DateCreatedUTC 
		
		SELECT 
			ID
		,	AddressLine
		,	City			
		,	PostalCode		
		,	ProvinceCode
		,	ROW_NUMBER() OVER(PARTITION BY AddressLine, City, PostalCode,ProvinceCode ORDER BY AddressInsertTS) AS RN 
		INTO 
			#DistinctAddressRN
		FROM 
			#GetDuplicates
							

		SELECT
			ID
		,	AddressLine
		,	City			
		,	PostalCode		
		,	ProvinceCode
		INTO 
			#DistinctAddress
		FROM 
			#DistinctAddressRN
		WHERE
			RN = 1

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Obtained addresses which dont have MAD'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Update the Master records'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;
		
		
		--Update identified Master record with Code
		UPDATE
			[Address]
		SET
			[Address].MasterAddressID		= [Address].Code	
		,	[Address].LastModifiedDateUTC	= @ProcessCategoryStartedAt
		FROM 
			#DistinctAddress
		
			INNER JOIN dbo.[Address]
			ON [Address].ID = #DistinctAddress.ID

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed Updating the Master records'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	
		,	@Updates = @@ROWCOUNT;
			
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Update the child records'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		----Get Addresses similar to identified Masters
		SELECT 
			DA.ID AS MasterID
		,	A.ID as DupID
		INTO
			#DeDuplicates	
		FROM 
			#DistinctAddress DA
		
			INNER JOIN #Address A
			ON DA.AddressLine		= A.AddressLine	
			AND	DA.City				= A.City
			AND	DA.PostalCode		= A.PostalCode
			AND	DA.ProvinceCode		= A.ProvinceCode
		WHERE
			DA.ID <> A.ID;
		

		--Update the child Addresses
		UPDATE
			AddressToUpdate
		SET
			AddressToUpdate.MasterAddressID		= AddressMaster.MasterAddressID
		,	AddressToUpdate.LastModifiedDateUTC = @ProcessCategoryStartedAt
		from 
			#DeDuplicates DeDup
		
			INNER JOIN dbo.[Address] As AddressToUpdate
			ON AddressToUpdate.ID = DeDup.DupID
		
			INNER JOIN dbo.[Address] As AddressMaster
			ON AddressMaster.ID = DeDup.MasterID;

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed Updating the Child records'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	
		,	@Updates = @@ROWCOUNT;


		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Start updating dbo.Address for similar address(Second)'
		,	@Status = 'InProgress'
		,	@CurrentStatus = 'Started'	;

		--To check if similar address already exists in Address
		;WITH AddressWithoutMAD  AS
		(
			SELECT
				CASE WHEN NULLIF(FullAddress,'') IS NULL THEN CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') ) ELSE FullAddress END AS AddressLine
			,	MasterAddressID
			,	ISNULL(City,'') City
			,	ISNULL(PostalCode,'') PostalCode
			,	ISNULL(ProvinceCode,'') ProvinceCode
			,	LastModifiedDateUTC
			FROM
				dbo.Address 
			WHERE
				MasterAddressID IS NULL
		)
		UPDATE AddressWithoutMAD
		SET
			AddressWithoutMAD.MasterAddressID		= AddressExisting.MasterAddressID
		,	AddressWithoutMAD.LastModifiedDateUTC	= @ProcessCategoryStartedAt
		FROM
			AddressWithoutMAD

			INNER JOIN
			(	SELECT
					CASE WHEN NULLIF(FullAddress,'') IS NULL THEN CONCAT(NULLIF(UnitNumber,'')+' - ',NULLIF(StreetNumber,'')+' ',NULLIF(StreetName,'')+' ',NULLIF(StreetType,'')+' ',NULLIF(StreetDirection,'') ) ELSE FullAddress END AS AddressLine
				,	ISNULL(MasterAddressID,'') MasterAddressID
				,	ISNULL(City,'') City
				,	ISNULL(PostalCode,'') PostalCode
				,	ISNULL(ProvinceCode,'') ProvinceCode
				FROM
					dbo.Address AddressExisting
				WHERE
					MasterAddressID IS NOT NULL
			) AddressExisting
			ON AddressWithoutMAD.AddressLine	= AddressExisting.AddressLine
			AND AddressWithoutMAD.City			= AddressExisting.City
			AND AddressWithoutMAD.PostalCode	= AddressExisting.PostalCode
			AND AddressWithoutMAD.ProvinceCode	= AddressExisting.ProvinceCode;
		
		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'ProcessHistory'
		,	@ProcessName = @ProcessName
		,	@Stage = 'Completed Updating dbo.Address for similar address(Second)'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'
		,	@Updates = @@ROWCOUNT;

		--Remove SourceInstanceEntry
		DELETE 
		FROM 
			Stage.MADSourceInstanceDetails 
		WHERE 
			SourceInstanceDetails=@SourceInstanceDetails;

		TRUNCATE TABLE Stage.MADAddress;

		EXEC ETLProcess.AuditLog
			@ProcessCategory = @ProcessCategory
		,	@Phase = 'Process'
		,	@ProcessName = @ProcessName
		,	@Stage = 'MADAddressToAddress'
		,	@Status = 'Completed'
		,	@CurrentStatus = 'Completed'	;
	END TRY

	BEGIN CATCH
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
		,	@Stage = 'MADAddressToAddress'
		,	@Status = 'Error'
		,	@CurrentStatus = 'Error'	;

		EXEC ETLProcess.EmailNotification
			@ProcessCategory=@ProcessCategory
		,	@ProcessName= @ProcessName
		,	@ProcessStage='MADAddressToAddress'
		,	@ErrorMessage='Failed to update Address from MADAddress'
		,	@IsError='Yes';

		SET @IsError=1

	END CATCH

	DROP TABLE IF EXISTS #StageMADAddressWithGeo;
	DROP TABLE IF EXISTS #Address;
	DROP TABLE IF EXISTS #GetDuplicates;
	DROP TABLE IF EXISTS #DistinctAddressRN;
	DROP TABLE IF EXISTS #DistinctAddress;
	DROP TABLE IF EXISTS #DeDuplicates;
		
	IF @IsError=1
		THROW 50005, N'An error occurred while loading Address from MADAddress', 1;

END