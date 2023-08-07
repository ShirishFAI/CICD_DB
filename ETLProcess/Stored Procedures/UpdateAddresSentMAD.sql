
CREATE PROCEDURE [ETLProcess].[UpdateAddresSentMAD]	
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Update dbo.Address once address sent to MAD
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @ProcessName VARCHAR(100)='PushToMAD';
	DECLARE @ProcessCategory VARCHAR(100)='DTC_MasterAddress_ETL';
	DECLARE @Updated BIGINT;
	
	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'Process'
	,	@ProcessName = @ProcessName
	,	@Stage = 'UpdateAddresSentMAD'
	,	@Status = 'InProgress'
	,	@CurrentStatus = 'Started'	;

	
	--Mark the records in dbo.Address
	UPDATE
		[Address]
	SET
		[Address].IsMADSent=1
	,	[Address].MADSentDateUTC = StageAddress.RunDate
	FROM
		dbo.[Address] [Address]

		INNER JOIN Stage.[Address]  StageAddress
		ON StageAddress.SourceAddressId = [Address].Code;

	SET @Updated =@@ROWCOUNT;

	TRUNCATE TABLE Stage.Address;

	EXEC ETLProcess.AuditLog
		@ProcessCategory = @ProcessCategory
	,	@Phase = 'Process'
	,	@ProcessName = @ProcessName
	,	@Stage = 'UpdateAddresSentMAD'
	,	@Status = 'Completed'
	,	@CurrentStatus = 'Completed'	
	,	@Updates=@Updated;

END