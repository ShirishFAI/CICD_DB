
CREATE PROCEDURE [ETLProcess].[AzureFunctionProcessingStatusUpdate]
	@FileName VARCHAR(200)
,	@Status	VARCHAR(50)

AS	
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Update status from the Azure Function
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	DECLARE @Check INT;

	SELECT 
		@Check = COUNT(1) 
	FROM  
		[ETLProcess].[AzureFunctionProcessingStatus]
	WHERE
		[FileName] =@FileName;


	IF @Check=0
		INSERT INTO [ETLProcess].[AzureFunctionProcessingStatus] VALUES(@FileName,@Status)
	ELSE
		UPDATE 	
			[ETLProcess].[AzureFunctionProcessingStatus] 
		SET
			[Status]=@Status
		WHERE
			FileName = @FileName;
END