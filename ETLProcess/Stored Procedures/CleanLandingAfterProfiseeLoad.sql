



CREATE PROCEDURE [ETLProcess].[CleanLandingAfterProfiseeLoad]
AS
BEGIN
	SELECT 'Clean Landing Tables'

	
	DECLARE 
		@TableName VARCHAR(100)
	,	@ProcessId INT
	,	@RunId INT
	,	@StageLandSchema VARCHAR(50)='StageLanding.'
	,	@DynamicSQL NVARCHAR(MAX)=NULL
	,	@Params NVARCHAR(1000)

		--SET @TableName ='BC_NorthVancouver_CadLegalDescription_0'
		--SET @ProcessId=43
		--SET @RunId=1
		--SET @DynamicSQL=NULL


		--Add Columns to StagLanding table
		SET @DynamicSQL='ALTER TABLE '+ @StageLandSchema+@TableName+' DROP COLUMN SourceID, Code'
		SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)'
		EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	

		SET @DynamicSQL='TRUNCATE TABLE '+ @StageLandSchema+@TableName
		SET @Params ='@StageLandSchema VARCHAR(50),@TableName VARCHAR(100)'
		EXECUTE sp_executesql 	@DynamicSQL,@Params,@StageLandSchema,@TableName	
END