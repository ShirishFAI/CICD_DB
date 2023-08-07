






CREATE PROCEDURE [ETLProcess].[ValidateExternalSourceFile]
	@ProcessCategory VARCHAR(100)='DTC_ExternalSource_ETL'
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

	DECLARE	@ProcessName VARCHAR(100);
	DECLARE	@ProcessId INT;
	DECLARE	@FileName VARCHAR(100);
	DECLARE	@CleansedFileName VARCHAR(100);
	DECLARE @ExtFileType VARCHAR(10);
	DECLARE	@IsError BIT;
	DECLARE @ErrorText VARCHAR(200);

	DECLARE CursorGetActiveProcesses CURSOR 
	FOR 
       	SELECT					
			ETLProcess.ProcessId
		,	ETLProcess.ProcessName
		,	ExternalFileslist.FileName		
		,	ExternalFileslist.CleansedFileName		
		,	ExternalFileslist.FileType
		FROM
			ETLProcess.ETLProcess

			INNER JOIN ETLProcess.ETLProcessCategory
			ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId

			LEFT JOIN Stage.ExternalFileslist
			ON ETLProcess.ProcessName = ExternalFileslist.ProcessName
		WHERE
			ETLProcess.ActiveFlag=1
			AND ETLProcessCategory.ActiveFlag=1			
				AND  1 = ( 
						CASE WHEN @ProcessCategory='ALL' AND ETLProcessCategory.ProcessCategoryName IN('DTC_InternalSource_ETL','DTC_ExternalSource_ETL')THEN 1
							 WHEN @ProcessCategory='DTC_InternalSource_ETL' AND ETLProcessCategory.ProcessCategoryName='DTC_InternalSource_ETL' THEN 1
							 WHEN @ProcessCategory='DTC_ExternalSource_ETL' AND ETLProcessCategory.ProcessCategoryName='DTC_ExternalSource_ETL' THEN 1
							 ELSE 0
						END
				)	

	OPEN CursorGetActiveProcesses
	FETCH NEXT FROM CursorGetActiveProcesses INTO 	@ProcessID,@ProcessName,@FileName, @CleansedFileName, @ExtFileType
	WHILE @@FETCH_STATUS = 0
	BEGIN 

		--If File Does Not Exists
		IF @FileName=''
		BEGIN
			EXEC ETLProcess.EmailNotification
				@ProcessCategory=@ProcessCategory
			,	@ProcessName= @ProcessName
			,	@ProcessStage='Validataion'
			,	@ErrorMessage='File not found to load'
			,	@IsError='Yes';

			SET @IsError=1;
			SET @ErrorText = 'File not found to load';
		END


		IF ( LEFT(@CleansedFileName,3) NOT IN('NL_','PE_','NS_','NB_','QC_','ON_','MB_','SK_','AB_','BC_','YT_','NT_','NU_') AND LEFT(@CleansedFileName,4)<>'ALL_')
		AND LEN(@FileName) > 0
		BEGIN
			EXEC ETLProcess.EmailNotification
				@ProcessCategory=@ProcessCategory
			,	@ProcessName= @ProcessName
			,	@ProcessStage='Validataion'
			,	@ErrorMessage='Province name specified in the in file name not found'
			,	@IsError='Yes';

			SET @IsError=1;
			SET @ErrorText = 'Province name specified in the file name not found';

			UPDATE
				Stage.MappingFileList
			SET
				IsError=1
			WHERE
				ProcessID=@ProcessID;
		END
				
		IF RIGHT(@CleansedFileName,2) NOT IN('_1','_0')	 AND LEN(@FileName) > 0 AND @ExtFileType IN ('.csv','.txt')
		BEGIN			
			EXEC ETLProcess.EmailNotification
				@ProcessCategory=@ProcessCategory
			,	@ProcessName= @ProcessName
			,	@ProcessStage='Validataion'
			,	@ErrorMessage='Reliable property has not been specifid properly in file name'
			,	@IsError='Yes';

			SET @IsError=1;
			SET @ErrorText = 'Reliable property has not been specifid properly in file name';

			UPDATE
				Stage.MappingFileList
			SET
				IsError=1
			WHERE
				ProcessID=@ProcessID;
		END

		FETCH NEXT FROM CursorGetActiveProcesses INTO 	@ProcessID,@ProcessName,@FileName, @CleansedFileName, @ExtFileType
	END

	CLOSE CursorGetActiveProcesses
	DEALLOCATE CursorGetActiveProcesses	

	--IF @IsError=1
	--	THROW 50001, @ErrorText, 1;
END