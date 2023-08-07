

CREATE PROCEDURE [ETLProcess].[GetAzureFunctionFileLoadStatus]	
	@FileName VARCHAR(100)=''
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Get status of the azure function load
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	DECLARE @ProcessingStatus VARCHAR(100);

	SELECT
		@ProcessingStatus = Status
	FROM
		ETLProcess.AzureFunctionProcessingStatus
	WHERE
		FileName=@FileName

	IF @ProcessingStatus='Completed'
		SELECT 1 AS IsCompleted
	ELSE
		SELECT 1 FROM Failed

END