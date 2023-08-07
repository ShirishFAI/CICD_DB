
CREATE PROCEDURE [ETLProcess].[EntityCleanseUpdateKeyColumns]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Update MasterAddressId, PIN and ARN related columns to required tables
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;

	DECLARE @ProcessCategory VARCHAR(100)='DTC_StageEntityCleansing_ETL';
	DECLARE @ProcessName VARCHAR(100)   ='EntityCleansingUpdateKeyValues'
	DECLARE @ProcessStage VARCHAR(100)='Update Key Columns';
	DECLARE @HistoryStage VARCHAR(200);
	DECLARE @ErroMessage VARCHAR(100)='Error Update Key Columns';
	DECLARE @ProcessID INT;
	DECLARE @IsAuditEntryExists INT;
	DECLARE @RunId INT;
	DECLARE @CurrentStatus VARCHAR(100);
	DECLARE @IsError BIT=0;
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @LastCompletedDT  DATETIME;
	DECLARE @CurrentRundDT  DATETIME;

	SELECT		
		@RunId = AuditProcessCategory.RunId
	,	@CurrentRundDT=AuditProcessCategory.UTC_StartedAt
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
		@LastCompletedDT = ETLProcessCategory.UTC_CompletedAt
	FROM
		ETLAudit.ETLProcessCategory 
	WHERE
		RunId IN
		(
			SELECT		
				MAX(AuditProcessCategory.RunId)
			FROM
				ETLProcess.ETLProcessCategory ProcessCategory
			
				INNER JOIN ETLAudit.ETLProcessCategory AuditProcessCategory
				ON ProcessCategory.ProcessCategoryId = AuditProcessCategory.ProcessCategoryId		
			
				INNER JOIN ETLProcess.ETLStatus
				ON ETLStatus.StatusId = AuditProcessCategory.CurrentStatus
			WHERE
				ProcessCategory.ProcessCategoryName=@ProcessCategory	
				AND ETLStatus.Status='Completed'
		);

	IF @LastCompletedDT IS NULL
		SET @LastCompletedDT='1900-01-01'	

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

			INNER JOIN 	ETLAudit.ETLProcess AuditProcess
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
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Stage = @ProcessStage
			,	@Status = 'InProgress'
			,	@CurrentStatus = 'Started'												
			
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
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = 'Update Key Columns started : dbo.Property'
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started';

				IF @LastCompletedDT='1900-01-01'	
					UPDATE
						Property
					SET
						MasterAddressId		=	[Address].MasterAddressID
					,	ARN					=	Taxation.ARN
					--,	JurCode				=	Taxation.JurCode	
					,	PIN					=	PIN.PIN
					--,	ProvinceCode		=	PIN.ProvinceCode	
					,	LastModifiedDateUTC	=	@CurrentRundDT					
					FROM
						dbo.Property

						LEFT JOIN dbo.Address [Address]
						ON [Address].Code=Property.Code
		
						LEFT JOIN dbo.PIN
						ON PIN.Code = Property.Code		

						LEFT JOIN dbo.Taxation
						ON Taxation.Code = Property.Code						
				ELSE
					UPDATE
						Property
					SET
						MasterAddressId		=	[Address].MasterAddressID
					,	ARN					=	Taxation.ARN
					--,	JurCode				=	Taxation.JurCode	
					,	PIN					=	PIN.PIN
					--,	ProvinceCode		=	PIN.ProvinceCode	
					,	LastModifiedDateUTC	=	@CurrentRundDT					
					FROM
						dbo.Property

						LEFT JOIN dbo.Address [Address]
						ON [Address].Code=Property.Code
		
						LEFT JOIN dbo.PIN
						ON PIN.Code = Property.Code		

						LEFT JOIN dbo.Taxation
						ON Taxation.Code = Property.Code		
					WHERE		
						[Address].LastModifiedDateUTC > @LastCompletedDT 
						OR  PIN.LastModifiedDateUTC > @LastCompletedDT 
						OR  Taxation.LastModifiedDateUTC > @LastCompletedDT ;
					

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = 'Update Key Columns completed : dbo.Property'
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'	
				,	@Updates = @@ROWCOUNT;

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = 'Update Key Columns started : dbo.Permit'
				,	@Status = 'InProgress'
				,	@CurrentStatus = 'Started';

				IF @LastCompletedDT='1900-01-01'	
					UPDATE 
						PERMIT
					SET 
						Permit.MasterAddressId = Property.MasterAddressId
					FROM dbo.Permit Permit
					LEFT JOIN dbo.Property Property ON Permit.Code = Property.Code
						AND Permit.JurCode = Property.JurCode
						AND Permit.ARN = Property.ARN
					WHERE Property.MasterAddressId IS NOT NULL						
				ELSE
					UPDATE 
						PERMIT
					SET 
						Permit.MasterAddressId = Property.MasterAddressId
					FROM dbo.Permit Permit
					LEFT JOIN dbo.Property Property 
					ON Permit.Code = Property.Code
						AND Permit.JurCode = Property.JurCode
						AND Permit.ARN = Property.ARN
					WHERE Property.MasterAddressId IS NOT NULL
					AND   Property.LastModifiedDateUTC > @LastCompletedDT 
					

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'ProcessHistory'
				,	@ProcessName = @ProcessName
				,	@Stage = 'Update Key Columns completed : dbo.Permit'
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'	
				,	@Updates = @@ROWCOUNT;
		
				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns started : dbo.Valuation'
				--,	@Status = 'InProgress'
				--,	@CurrentStatus = 'Started';

				--IF @LastCompletedDT='1900-01-01'	
				--	UPDATE
				--		Valuation
				--	SET
				--		ARN					= Taxation.ARN				
				--	,	MasterAddressID		= [Address].MasterAddressID	
				--	,	PIN					= PIN.PIN				
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	--,	JurCode				= CASE WHEN Taxation.IsValid=1	THEN Taxation.JurCode			ELSE NULL END  
				--	--,	ProvinceCode		= CASE WHEN PIN.IsValid=1		THEN PIN.ProvinceCode			ELSE NULL END    
				--	FROM
				--		dbo.Valuation

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code=Valuation.Code
		
				--		LEFT JOIN dbo.PIN
				--		ON PIN.Code = Valuation.Code
		
				--		LEFT JOIN dbo.Taxation
				--		ON Taxation.Code = Valuation.Code							
				--ELSE
				--	UPDATE
				--		Valuation
				--	SET
				--		ARN					= Taxation.ARN				
				--	,	MasterAddressID		= [Address].MasterAddressID	
				--	,	PIN					= PIN.PIN				
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	--,	JurCode				= CASE WHEN Taxation.IsValid=1	THEN Taxation.JurCode			ELSE NULL END  
				--	--,	ProvinceCode		= CASE WHEN PIN.IsValid=1		THEN PIN.ProvinceCode			ELSE NULL END    
				--	FROM
				--		dbo.Valuation

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code=Valuation.Code
		
				--		LEFT JOIN dbo.PIN
				--		ON PIN.Code = Valuation.Code
		
				--		LEFT JOIN dbo.Taxation
				--		ON Taxation.Code = Valuation.Code		
				--	WHERE
				--		[Address].LastModifiedDateUTC > @LastCompletedDT 
				--		OR Taxation.LastModifiedDateUTC > @LastCompletedDT 
				--		OR PIN.LastModifiedDateUTC > @LastCompletedDT ;

				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns completed : dbo.Valuation'
				--,	@Status = 'Completed'
				--,	@CurrentStatus = 'Completed'	
				--,	@Updates = @@ROWCOUNT;


				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns started : dbo.Building'
				--,	@Status = 'InProgress'
				--,	@CurrentStatus = 'Started';

				--IF @LastCompletedDT='1900-01-01'	
				--	UPDATE
				--		Building
				--	SET
				--		MasterAddressID = [Address].MasterAddressID 
				--	,	PIN				= PIN.PIN 
				--	--,	ProvinceCode	= CASE WHEN PIN.IsValid=1 THEN PIN.ProvinceCode  ELSE NULL END    
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Building

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code = Building.Code		

				--		LEFT JOIN dbo.PIN 
				--		ON [Building].Code = PIN.Code	
				--ELSE
				--	UPDATE
				--		Building
				--	SET
				--		MasterAddressID = [Address].MasterAddressID 
				--	,	PIN				= PIN.PIN 
				--	--,	ProvinceCode	= CASE WHEN PIN.IsValid=1 THEN PIN.ProvinceCode  ELSE NULL END    
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Building

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code = Building.Code		

				--		LEFT JOIN dbo.PIN 
				--		ON Building.Code = PIN.Code		
				--	WHERE
				--		[Address].LastModifiedDateUTC > @LastCompletedDT 
				--		OR PIN.LastModifiedDateUTC > @LastCompletedDT ;

				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns completed : dbo.Building'
				--,	@Status = 'Completed'
				--,	@CurrentStatus = 'Completed'	
				--,	@Updates = @@ROWCOUNT;
				
				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns started : dbo.Parcel'
				--,	@Status = 'InProgress'
				--,	@CurrentStatus = 'Started';

				--IF @LastCompletedDT='1900-01-01'						
				--	UPDATE
				--		Parcel
				--	SET
				--		MasterAddressID = [Address].MasterAddressID
				--	,	PIN				= PIN.PIN					  
				--	--,	ProvinceCode	= CASE WHEN PIN.IsValid=1		THEN PIN.ProvinceCode			ELSE NULL END    
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Parcel

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code = Parcel.Code		

				--		LEFT JOIN dbo.PIN 
				--		ON Parcel.Code = PIN.Code							
				--ELSE
				--	UPDATE
				--		Parcel
				--	SET
				--		MasterAddressID = [Address].MasterAddressID
				--	,	PIN				= PIN.PIN					  
				--	--,	ProvinceCode	= CASE WHEN PIN.IsValid=1		THEN PIN.ProvinceCode			ELSE NULL END    
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Parcel

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code = Parcel.Code		

				--		LEFT JOIN dbo.PIN 
				--		ON Parcel.Code = PIN.Code		
				--	WHERE
				--		[Address].LastModifiedDateUTC > @LastCompletedDT 
				--		OR PIN.LastModifiedDateUTC > @LastCompletedDT ;
					
				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns completed : dbo.Parcel'
				--,	@Status = 'Completed'
				--,	@CurrentStatus = 'Completed'	
				--,	@Updates = @@ROWCOUNT;
				
				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns started : dbo.Business'
				--,	@Status = 'InProgress'
				--,	@CurrentStatus = 'Started';

				--IF @LastCompletedDT='1900-01-01'						
				--	UPDATE
				--		Business
				--	SET
				--		MasterAddressID		= [Address].MasterAddressID
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Business

				--		INNER JOIN dbo.Address [Address]
				--		ON [Address].Code = Business.Code		
				--ELSE

				--	UPDATE
				--		Business
				--	SET
				--		MasterAddressID		= [Address].MasterAddressID
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Business

				--		INNER JOIN dbo.Address [Address]
				--		ON [Address].Code = Business.Code		
				--	WHERE
				--		[Address].LastModifiedDateUTC > @LastCompletedDT ;

				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns completed : dbo.Business'
				--,	@Status = 'Completed'
				--,	@CurrentStatus = 'Completed'	
				--,	@Updates = @@ROWCOUNT;
				

				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns started : dbo.Listing'
				--,	@Status = 'InProgress'
				--,	@CurrentStatus = 'Started';

				--IF @LastCompletedDT='1900-01-01'						
				--	UPDATE
				--		Listing
				--	SET
				--		MasterAddressID		=[Address].MasterAddressID
				--	,	ARN					= Taxation.ARN			
				--	,	PIN					= PIN.PIN				
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Listing

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code = Listing.Code

				--		LEFT JOIN dbo.PIN
				--		ON PIN.Code = Listing.Code
		
				--		LEFT JOIN dbo.Taxation
				--		ON Taxation.Code = Listing.Code		

				--ELSE
				--	UPDATE
				--		Listing
				--	SET
				--		MasterAddressID		=[Address].MasterAddressID
				--	,	ARN					= Taxation.ARN			
				--	,	PIN					= PIN.PIN				
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Listing

				--		LEFT JOIN dbo.Address [Address]
				--		ON [Address].Code = Listing.Code

				--		LEFT JOIN dbo.PIN
				--		ON PIN.Code = Listing.Code
		
				--		LEFT JOIN dbo.Taxation
				--		ON Taxation.Code = Listing.Code		

				--	WHERE
				--		[Address].LastModifiedDateUTC > @LastCompletedDT 
				--		OR PIN.LastModifiedDateUTC > @LastCompletedDT 
				--		OR Taxation.LastModifiedDateUTC > @LastCompletedDT ;

				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns completed : dbo.Listing'
				--,	@Status = 'Completed'
				--,	@CurrentStatus = 'Completed'	
				--,	@Updates = @@ROWCOUNT;
				
				
				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns started : dbo.Sales'
				--,	@Status = 'InProgress'
				--,	@CurrentStatus = 'Started';
				
				--IF @LastCompletedDT='1900-01-01'		
				--	UPDATE
				--		Sales
				--	SET
				--		MasterAddressID		= [Address].MasterAddressID
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Sales

				--		INNER JOIN dbo.Address [Address]
				--		ON [Address].Code = Sales.Code							

				--ELSE
				--	UPDATE
				--		Sales
				--	SET
				--		MasterAddressID		= [Address].MasterAddressID
				--	,	LastModifiedDateUTC = @CurrentRundDT
				--	FROM
				--		dbo.Sales

				--		INNER JOIN dbo.Address [Address]
				--		ON [Address].Code = Sales.Code		
				--	WHERE
				--		[Address].LastModifiedDateUTC > @LastCompletedDT ;

				--EXEC ETLProcess.AuditLog
				--	@ProcessCategory = @ProcessCategory
				--,	@Phase = 'ProcessHistory'
				--,	@ProcessName = @ProcessName
				--,	@Stage = 'Update Key Columns completed : dbo.Sales'
				--,	@Status = 'Completed'
				--,	@CurrentStatus = 'Completed'	
				--,	@Updates = @@ROWCOUNT;

				EXEC ETLProcess.AuditLog
					@ProcessCategory = @ProcessCategory
				,	@Phase = 'Process'
				,	@ProcessName = @ProcessName
				,	@Status = 'Completed'
				,	@CurrentStatus = 'Completed'
				,	@Stage = @ProcessStage;		
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
			,	@Phase = 'ProcessHistory'
			,	@ProcessName = @ProcessName
			,	@Stage =@ErroMessage
			,	@Status = 'Error'
			,	@CurrentStatus = 'Error'	
			
			EXEC ETLProcess.AuditLog
				@ProcessCategory = @ProcessCategory
			,	@Phase = 'Process'
			,	@ProcessName = @ProcessName
			,	@Status = 'Error'
			,	@CurrentStatus = 'Error'
			,	@Stage = @ProcessStage

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
			,	 @ErrorProcedure  
			,	ERROR_LINE() AS ErrorLine  
			,	ERROR_MESSAGE() AS ErrorMessage
			,	GETDATE();

			EXEC ETLProcess.EmailNotification
				@ProcessCategory=@ProcessCategory
			,	@ProcessName= @ProcessName
			,	@ProcessStage=@ProcessStage
			,	@ErrorMessage=@ErroMessage
			,	@IsError='Yes';

			THROW 50001, @ErroMessage, 1;
		END CATCH
	END 
	END
END