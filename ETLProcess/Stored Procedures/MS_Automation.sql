CREATE PROCEDURE [ETLProcess].[MS_Automation]
AS
BEGIN	
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Profisee Automation
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @Status VARCHAR(50);

	SELECT
		@Status = MSStatus
	FROM
		ETLProcess.MS_Automation_Status


	IF @Status ='Start'
	BEGIN
		UPDATE ETLProcess.MS_Automation_Status SET MSStatus='InProgress',LastModifiedDate=GETUTCDATE()
		SELECT 1 AS StartMS
	END
	ELSE
	BEGIN
		SELECT 0 AS StartMS
	END

	
	--select * from ETLProcess.MS_Automation_Status
	--CREATE TABLE ETLProcess.MS_Automation_Status
	--(
	--	MSStatus VARCHAR(50),
	--	LastModifiedDate DATETIME
	--)
	
	--INSERT INTO ETLProcess.MS_Automation_Status
	--	SELECT 0,GETUTCDATE()
	
	--UPDATE ETLProcess.MS_Automation_Status SET MSStatus='Start',LastModifiedDate=GETUTCDATE()
END