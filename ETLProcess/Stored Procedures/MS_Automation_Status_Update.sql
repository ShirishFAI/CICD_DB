CREATE PROCEDURE [ETLProcess].[MS_Automation_Status_Update]
	@Status VARCHAR(100) = ''
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Update the status
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @CheckIsActive INT;

	IF @Status='Start'
	BEGIN
		SELECT
			@CheckIsActive = COUNT(1)
		FROM
			ETLProcess.ETLProcessCategory

			INNER JOIN ETLProcess.ETLProcess
			ON ETLProcess.ProcessCategoryId = ETLProcessCategory.ProcessCategoryId
		WHERE
			ETLProcessCategory.ProcessCategoryName='DTC_Profisee_MS_Automation'
			AND ETLProcessCategory.ActiveFlag =1
			AND ETLProcess.ActiveFlag=1
	
		IF @CheckIsActive > 0
		BEGIN
			UPDATE ETLProcess.MS_Automation_Status SET MSStatus='Start',LastModifiedDate=GETUTCDATE();
		END
	END

END