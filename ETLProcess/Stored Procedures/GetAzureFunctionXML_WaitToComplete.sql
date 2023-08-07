
CREATE PROCEDURE [ETLProcess].[GetAzureFunctionXML_WaitToComplete]
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Wait function to complete
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	--UPDATE ETLProcess.AzureFunctionProcessingStatus SET Status='InProgress'
	DECLARE @IsExists INT;	
	DECLARE @Counter INT= 1;
	DECLARE @RetryCount INT=6;
	DECLARE @Success    BIT=0;
			
	WAITFOR DELAY  '01:30:00';
	
	WHILE @Counter <=   @RetryCount AND @Success = 0
	BEGIN
		BEGIN TRY			
			--IF @Counter=@RetryCount
			--	UPDATE ETLProcess.AzureFunctionProcessingStatus SET Status='Completed'
				
			SELECT
				@IsExists = COUNT(1)
			FROM	
				ETLProcess.AzureFunctionProcessingStatus
			WHERE
				FileName='BC_ALL_Assessment.xml'
				AND Status='Completed';

			IF @IsExists > 0
			BEGIN
				SELECT 'Completed';
				SET @Counter = @RetryCount+1;
			END
			ELSE
				THROW 51000, 'Function not yet finished loading.', 1; 	
			
		END TRY

		BEGIN CATCH
			SELECT 'Error'
			WAITFOR DELAY  '00:05:00';
			
			SET @Counter = @Counter + 1;

			IF @Counter=@RetryCount+1
				THROW 51000, 'Failed to load the data.', 1; 		
		END CATCH

	END
	

END