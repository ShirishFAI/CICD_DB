

CREATE PROCEDURE [ETLProcess].[BC_Handle_Address_Changed_Records]
AS
BEGIN
	/****************************************************************************************
-- AUTHOR		: Shirish W.
-- DATE			: 11/22/2022
-- PURPOSE		: Update records into DTC entities through BC UPTO Date
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** --------------------------------------------------------------------------------------
** 09/25/2020	Shirish W.	Original Version
******************************************************************************************/

DECLARE @StartDate DATETIME;
DECLARE @BC_UPTD_LastLoadDate DATETIME;
SET @StartDate = GETDATE();
SET @BC_UPTD_LastLoadDate = (Select LoadDate from dbo.BC_UPTO_DATE_LoadDate WHERE processName='BC_UPTO_DATE');

	DECLARE @ProcessName VARCHAR(100) = 'BC Update DTC Through BC UPTD';
	DECLARE @ProcessCategory VARCHAR(100) = 'DTC_ExternalSource_ETL';
	DECLARE @ErrorProcedure VARCHAR(100);
	DECLARE @ProcessID INT;

	SET @ProcessID = (SELECT ProcessId FROM ETLProcess.ETLProcess WHERE ProcessName='BC_ALL_Assessment')

	SELECT @ErrorProcedure = s.name + '.' + o.name
	FROM SYS.OBJECTS O
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID = O.SCHEMA_ID
	WHERE OBJECT_ID = @@PROCID;

	BEGIN TRY
		BEGIN TRAN

		--  Deleting Duplicate Records from BC_UPTO_DATE
		SELECT DISTINCT ID,CODE
		INTO #DuplicateRecords
		FROM
		(
		SELECT *
				,ROW_NUMBER() OVER (
					Partition By code order by code
					) RNK
			FROM dbo.BC_UPTO_DATE
			WHERE CODE IS NOT NULL
		)A
		WHERE RNK>1;

		DELETE FROM dbo.BC_UPTO_DATE
		WHERE ID IN (SELECT ID FROM #DuplicateRecords);

		IF OBJECT_ID(N'tempdb..#Address_Update') IS NOT NULL
			BEGIN
				DROP TABLE #Address_Update
			END

		SELECT *
		INTO #BC_UPTO_DATE
		FROM dbo.BC_UPTO_DATE UPTD WITH(NOLOCK)
		WHERE UPTD.Code IS NOT NULL 
		AND UPTD.LastModifiedDateUTC > @BC_UPTD_LastLoadDate

		SELECT UPTD.*
		INTO #Address_Update
		FROM #BC_UPTO_DATE UPTD
		INNER JOIN DBO.Address ADDR WITH(NOLOCK) ON UPTD.Code = ADDR.Code
		WHERE 
		(
			ADDR.StreetNumber != UPTD.FolioAddresses_FolioAddress_StreetNumber
		OR	ADDR.StreetName != UPTD.FolioAddresses_FolioAddress_StreetName
		OR	ADDR.UnitNumber != UPTD.FolioAddresses_FolioAddress_UnitNumber
		OR	ADDR.StreetType != UPTD.FolioAddresses_FolioAddress_StreetType
		OR	ADDR.StreetDirection != UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
		OR	ADDR.PostalCode != UPTD.FolioAddresses_FolioAddress_PostalZip
		OR	ADDR.City != UPTD.FolioAddresses_FolioAddress_City
		)
		
		UPDATE Address
		SET Address.MasterAddressID = NULL
			,Address.IsMADSent=NULL
			,Address.MADSentDateUTC=NULL
			,Address.MADReceivedDateUTC=NULL
			,Address.IsMADReceived=NULL
			,Address.AreaDescription = UPTD.AssessmentAreaDescription
			,Address.JurCode = UPTD.JurisdictionCode
			,Address.JurDescription = UPTD.JurisdictionDescription
			,Address.LandDistrict = UPTD.LegalDescriptions_LegalDescription_LandDistrict
			,Address.LandDistrictName = UPTD.LegalDescriptions_LegalDescription_LandDistrictDescription
			,Address.Neighbourhood = UPTD.FolioDescription_Neighbourhood_NeighbourhoodCode
			,Address.NeighbourhoodDescription = UPTD.FolioDescription_Neighbourhood_NeighbourhoodDescription
			,Address.ProvinceCode = UPTD.FolioAddresses_FolioAddress_ProvinceState
			,Address.Range = UPTD.LegalDescriptions_LegalDescription_Range
			,Address.Region = UPTD.RegionalDistrict_DistrictDescription
			,Address.SchoolDistrictDescription = UPTD.SchoolDistrict_DistrictDescription
			,Address.Township = UPTD.LegalDescriptions_LegalDescription_Township
			,Address.LastModifiedDateUTC = @StartDate
			,Address.City = UPTD.FolioAddresses_FolioAddress_City
			,Address.PostalCode = UPTD.FolioAddresses_FolioAddress_PostalZip
			,Address.StreetDirection = UPTD.FolioAddresses_FolioAddress_StreetDirectionSuffix
			,Address.StreetName = UPTD.FolioAddresses_FolioAddress_StreetName
			,Address.StreetNumber = UPTD.FolioAddresses_FolioAddress_StreetNumber
			,Address.StreetType = UPTD.FolioAddresses_FolioAddress_StreetType
			,Address.UnitNumber = UPTD.FolioAddresses_FolioAddress_UnitNumber
		FROM dbo.Address Address
		INNER JOIN #Address_Update UPTD ON Address.Code = UPTD.Code

	COMMIT TRAN;

	END TRY

	BEGIN CATCH
		ROLLBACK TRAN

		INSERT INTO ETLProcess.ETLStoredProcedureErrors (
			ProcessCategory
			,ProcessName
			,ErrorNumber
			,ErrorSeverity
			,ErrorState
			,ErrorProcedure
			,ErrorLine
			,ErrorMessage
			,ErrorDate
			)
		SELECT @ProcessCategory
			,@ProcessName
			,ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,@ErrorProcedure
			,ERROR_LINE() AS ErrorLine
			,ERROR_MESSAGE() AS ErrorMessage
			,GETDATE()
	END CATCH

END