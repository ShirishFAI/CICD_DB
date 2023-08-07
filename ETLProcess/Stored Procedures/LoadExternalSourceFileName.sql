CREATE PROCEDURE [ETLProcess].[LoadExternalSourceFileName]
	@FileName VARCHAR(510)
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Validate the Files placed in Blob
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @CleansedFileName VARCHAR(100);
	DECLARE @ProcessName VARCHAR(100);
	DECLARE @FileType VARCHAR(10);

	SET @FileType=RIGHT(@FileName ,4)
	
	IF @FileType IN('.txt','.csv')
	BEGIN
		SET @CleansedFileName =LEFT(@FileName, LEN(@FileName)-4);
		SET @ProcessName = LEFT(@FileName, LEN(@FileName)-6);		
	END

	IF  @FileType IN('.dbf','.xml')
	BEGIN
		SET @CleansedFileName =LEFT(@FileName, LEN(@FileName)-4);
		SET @ProcessName = @CleansedFileName
	END

	INSERT INTO Stage.ExternalFileslist
		SELECT 
			@ProcessName
		,	@CleansedFileName
		,	@FileName
		,	@FileType
		,	0
END