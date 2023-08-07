

CREATE PROCEDURE [ETLProcess].[BC_Update_MasterAddressID]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Shirish W.
-- DATE			: 11/22/2022
-- PURPOSE		: Update MasterAddressID in DTC and Profisee entities
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

	DECLARE @ProcessName VARCHAR(100) = 'BC Update MasterAddressID in DTC';
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

				SELECT DISTINCT A.code
				,A.MasterAddressID
				,A.UnitNumber
				,A.StreetNumber
				,A.StreetName
				,A.StreetType
				,A.City
				,A.PostalCode
				,ROW_NUMBER() OVER (
					PARTITION BY a.MasterAddressID ORDER BY CODE
					) AS RNK
			INTO #TEMP
			FROM dbo.Address a
			INNER JOIN MADAddress m WITH (NOLOCK) ON a.Code = m.SourceAddressID
			WHERE m.MADAddressID IS NULL
				AND isnumeric(masteraddressID) = 1
				AND a.Data_Source_ID = 8;
			
			SELECT A.CODE AS CODE
				,B.CODE AS MasterAddressID
			INTO #MADIDTOUpdate
			FROM #TEMP A
			INNER JOIN #TEMP B ON A.MasterAddressID = B.MasterAddressID
				AND B.RNK = 1;
			
			UPDATE address
			SET address.MasterAddressID = t.MasterAddressID
			,address.LastModifiedDateUTC = @StartDate
			FROM DBO.Address ADDRESS
			INNER JOIN #MADIDTOUpdate T ON ADDRESS.code = T.CODE;

			UPDATE taddress
			SET taddress.MasterAddressID = t.MasterAddressID
			,taddress.LastModifiedDateUTC = @StartDate
			,tAddress.[Match Group]=NULL     
			,tAddress.[Match Score]=NULL     
			,tAddress.[Match Status]=NULL    
			,tAddress.[Record Source]=NULL   
			,tAddress.[Match Member]=NULL    
			,tAddress.[Match Strategy]=NULL  
			,tAddress.[Match DateTime]=NULL  
			,tAddress.[Match User]=NULL      
			,tAddress.[Match MultiGroup]=NULL
			,tAddress.[Master]=NULL          
			,tAddress.[Approved Count]=NULL  
			,tAddress.[Proposed Count]=NULL
			FROM Profisee.data.tAddress tADDRESS
			INNER JOIN #MADIDTOUpdate T ON tADDRESS.code = T.CODE;


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