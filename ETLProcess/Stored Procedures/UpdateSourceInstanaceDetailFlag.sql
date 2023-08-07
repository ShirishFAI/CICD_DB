
CREATE PROCEDURE [ETLProcess].[UpdateSourceInstanaceDetailFlag]	
	@SourceInstanceDetails  VARCHAR(100)
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Delete Source Instance Name
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	--Remove SourceInstanceEntry
	UPDATE 
		Stage.MADSourceInstanceDetails 
	SET
		ProcessStatus='InProgress'
	WHERE 
		SourceInstanceDetails=@SourceInstanceDetails;		

END