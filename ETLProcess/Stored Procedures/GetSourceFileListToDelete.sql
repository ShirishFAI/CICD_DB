
CREATE PROCEDURE [ETLProcess].[GetSourceFileListToDelete]	
	@ProcessCategory VARCHAR(100)=''
,	@FileType VARCHAR(20)=''
AS
BEGIN
/****************************************************************************************
-- AUTHOR		: Sanjay Janardhan
-- DATE			: 09/25/2020
-- PURPOSE		: Get Source file list to delete
-- DEPENDENCIES	: 
--
-- VERSION HISTORY:
** ----------------------------------------------------------------------------------------
** 09/25/2020	Sanjay Janardhan	Original Version
******************************************************************************************/
	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;


	SET @FileType=ISNULL(@FileType,'')

	SELECT					
		ExternalFileslist.FileName	
	FROM
		ETLProcess.ETLProcess
	
		INNER JOIN ETLProcess.ETLProcessCategory
		ON ETLProcessCategory.ProcessCategoryId = ETLProcess.ProcessCategoryId
	
		INNER JOIN	Stage.ExternalFileslist
		ON ETLProcess.ProcessName=ExternalFileslist.ProcessName
	WHERE
		ETLProcess.ActiveFlag=1
		AND ETLProcessCategory.ActiveFlag=1
		AND ETLProcessCategory.ProcessCategoryName=@ProcessCategory
		AND 1=( CASE 					
						WHEN @FileType ='CSV/TXT'
									AND ETLProcess.ProcessName NOT IN(	'BC_ColumbiaShuwap_Address','BC_ColumbiaShuwap_Property','ON_NiagaraFalls_condominiums','PE_ALL_Civic_Add_Coor_SHP_ca_points'
							,'BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp','BC_Victoria_Parcels','BC_WhiteRock_Address','BC_Saanich_Parcels'
							,'ON_Windsor_Land_Parcels_UTM83', 'BC_Nanaimo_PARCELS','BC_ALL_Assessment')
									THEN 1 
						WHEN @FileType ='DBF/XML' 
									AND ETLProcess.ProcessName IN(	'BC_ColumbiaShuwap_Address','BC_ColumbiaShuwap_Property','ON_NiagaraFalls_condominiums','PE_ALL_Civic_Add_Coor_SHP_ca_points'
							,'BC_DistrictofOkanagan_Parcelsparcel_legal_civic_shp','BC_Victoria_Parcels','BC_WhiteRock_Address','BC_Saanich_Parcels'
							,'ON_Windsor_Land_Parcels_UTM83', 'BC_Nanaimo_PARCELS','BC_ALL_Assessment')
									THEN 1 
						ELSE 0
					END
				);

END