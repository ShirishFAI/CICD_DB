


CREATE PROCEDURE [ETLProcess].[BC_UpdateFolio_Through_Weekly_Profisee]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Raghavendra
-- DATE			: 11/25/2022
-- PURPOSE		: Update BCA Weekly files to Profisee tables
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 11/25/2022	Raghavendra	Original Version
******************************************************************************************/

	DECLARE @ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL';
	DECLARE @ProcessName VARCHAR(100) ; 
	DECLARE @TableName VARCHAR(100)='BC_ALL_Assessment_Weekly'; 
	DECLARE @ErrorSchema varchar(300)='StageProcessErr.';        
    DECLARE @HistorySchema varchar(300)='SourceHistory.'; 
	DECLARE @ErrorProcedure VARCHAR(100); 
	DECLARE @IsError BIT=0;     

	SELECT @ErrorProcedure= s.name+'.'+o.name         
	FROM         
	SYS.OBJECTS O            
	INNER JOIN SYS.SCHEMAS S ON S.SCHEMA_ID=O.SCHEMA_ID WHERE OBJECT_ID=@@PROCID;        
        
	 SET @ProcessName='BC_ALL_Assessment_Weekly';
	 SET @TableName=@ProcessName;

	BEGIN TRY 
			SET NOCOUNT ON;

			declare @modifiedDate datetime; 
			set @modifiedDate=GETUTCDATE();
			
			/*------------------------------------------------------------------------------------
			Update profisee NON-M&S Entity tables
			------------------------------------------------------------------------------------*/

			--Get Latest Modified date for Non M&S Entitites (exclude property as its truncate and load)
			--DECLARE @ModifiedDate datetime;
			--SELECT  @ModifiedDate=GETUTCDATE();


			
			DROP TABLE IF EXISTS #Entity_MaxDate;
			CREATE TABLE #Entity_MaxDate (LastModifiedDateUTC datetime, Entity varchar(500));

			INSERT INTO #Entity_MaxDate
			SELECT MAX(LastModifiedDateUTC) AS LastModifiedDateUTC, 'Business' as Entity FROM Profisee.data.tBusiness UNION ALL
			SELECT MAX(LastModifiedDateUTC) AS LastModifiedDateUTC ,'Listing' as Entity FROM Profisee.data.tListing UNION ALL
			SELECT MAX(LastModifiedDateUTC) AS LastModifiedDateUTC ,'Sales' as Entity FROM Profisee.data.tSales UNION ALL
			SELECT MAX(LastModifiedDateUTC) AS LastModifiedDateUTC ,'Valuation' as Entity FROM Profisee.data.tValuation

			---Update profisee.data.tBuisiness
			UPDATE profisee.data.tBusiness
			SET BusinessCategory = Business.BusinessCategory,
			BusinessCode = Business.BusinessCode,
			BusinessDescription = Business.BusinessDescription,
			BusinessType = Business.BusinessType,
			NaicsCode = Business.NaicsCode,
			NaicsDescription = Business.NaicsDescription,
			ProvinceCode = Business.ProvinceCode,
			Company = Business.Company,
			LastModifiedDateUTC = @ModifiedDate
			FROM dbo.Business Business
			JOIN profisee.data.tBusiness prof on Business.Code=prof.Code
			where Business.LastModifiedDateUTC >(select LastModifiedDateUTC from #Entity_MaxDate where Entity = 'Business');

			--Update profisee.data.tListing
			UPDATE profisee.data.tListing
			SET PIN = Listing.PIN
			,ProvinceCode = Listing.ProvinceCode
			,ARN = Listing.ARN
			,JurCode = Listing.JurCode
			,MLSNumber = Listing.MLSNumber
			,SellerName = Listing.SellerName
			,DateEnd = Listing.DateEnd
			,DateStart = Listing.DateStart
			,DateUpdate = Listing.DateUpdate
			,ListDays = Listing.ListDays
			,ListType = Listing.ListType
			,ListStatus = Listing.ListStatus
			,ListHistory = Listing.ListHistory
			,PriceAsked = Listing.PriceAsked
			,FCTTransactionType = Listing.FCTTransactionType
			,LoanAmt = Listing.LoanAmt
			,LendingValue = Listing.LendingValue
			,GuaranteeValue = Listing.GuaranteeValue
			,OwnershipType = Listing.OwnershipType
			,RentAssignment = Listing.RentAssignment
			,LastModifiedDateUTC = @ModifiedDate
			FROM dbo.Listing Listing
			JOIN profisee.data.tListing prof on Listing.Code=prof.Code
			where Listing.LastModifiedDateUTC >(select LastModifiedDateUTC from #Entity_MaxDate where Entity = 'Listing');



			--Update profisee.data.tValuation
			UPDATE profisee.data.tValuation
			SET PIN = Valuation.PIN,
			ProvinceCode = Valuation.ProvinceCode,
			ARN = Valuation.ARN,
			JurCode = Valuation.JurCode,
			EstimatedValue = Valuation.EstimatedValue,
			HighValue = Valuation.HighValue,
			LowValue = Valuation.LowValue,
			CompleteDate = Valuation.CompleteDate,
			MPACValue = Valuation.MPACValue,
			TERANETValue = Valuation.TERANETValue,
			InsuredValue = Valuation.InsuredValue,
			MPACConfidenceLevel = Valuation.MPACConfidenceLevel,
			MPACPropertyType = Valuation.MPACPropertyType,
			POSDate = Valuation.POSDate,
			MPACLowConfidenceLimit = Valuation.MPACLowConfidenceLimit,
			MPACHighConfidenceLimit = Valuation.MPACHighConfidenceLimit,
			ValuePurchasePrice = Valuation.ValuePurchasePrice,
			AppraisedValue = Valuation.AppraisedValue,
			LastModifiedDateUTC = @ModifiedDate
			FROM dbo.Valuation Valuation
			JOIN profisee.data.tListing prof on Valuation.Code=prof.Code
			where Valuation.LastModifiedDateUTC >(select LastModifiedDateUTC from #Entity_MaxDate where Entity = 'Valuation');

			--Update profisee.data.tSales
			UPDATE profisee.data.tSales
			SET LastSaleDate = Sales.LastSaleDate,
			SaleType = Sales.SaleType,
			PurchasePrice = Sales.PurchasePrice,
			OriginalPurchasePrice = Sales.OriginalPurchasePrice,
			BuyerName = Sales.BuyerName,
			PriceSold = Sales.PriceSold,
			LastSaleAmount = Sales.LastSaleAmount,
			LastSaleYear = Sales.LastSaleYear,
			ClosingDate = Sales.ClosingDate,
			POSDateSales = Sales.POSDateSales,
			StatusID = Sales.StatusID,
			LastModifiedDateUTC = @ModifiedDate
			FROM dbo.Sales Sales
			JOIN profisee.data.tSales prof on Sales.Code=prof.Code
			where Sales.LastModifiedDateUTC >(select LastModifiedDateUTC from #Entity_MaxDate where Entity = 'Sales');

			
			END TRY

			BEGIN CATCH        
		    --UPDATE Stage.ExternalFileslist SET IsError=1 WHERE FileName=@ExternalFileName;        
			--ROLLBACK TRAN;

		   SET @IsError=1        
        
		   EXEC ETLProcess.AuditLog        
			@ProcessCategory = @ProcessCategory        
		   , @Phase = 'ProcessHistory'        
		   , @ProcessName = @ProcessName        
		   , @Stage ='Error BCA_Weekly_Update'        
		   , @Status = 'Error'        
		   , @CurrentStatus = 'Error'        
		   , @Inserts = 0;        
        
		   EXEC ETLProcess.AuditLog        
			@ProcessCategory = @ProcessCategory        
		   , @Phase = 'Process'        
		   , @ProcessName = @ProcessName        
		   , @Status = 'Error'        
		   , @CurrentStatus = 'Error'        
		   , @Stage = 'Landing';         
        
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
		   , @ErrorProcedure        
		   , ERROR_LINE() AS ErrorLine          
		   , ERROR_MESSAGE() AS ErrorMessage        
		   , GETDATE()        
              
               
		   EXEC ETLProcess.EmailNotification        
			@ProcessCategory=@ProcessCategory        
		   , @ProcessName= @ProcessName        
		   , @ProcessStage='Landing'        
		   , @ErrorMessage='Failed to Load BCA Weekly Update Profisee'        
		   , @IsError='Yes';        
      END CATCH 

		IF @IsError=1        
	  THROW 50005, N'An error occurred while updating BCA Weekly Update  Profisee', 1;        
 
END