/****************************************************************************************************************************************
Business deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Business as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By BusinessCategory,BusinessCode,BusinessDescription,BusinessType,NaicsCode,NaicsDescription,ProvinceCode
	,Company,Data_Source_ID Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Business b
	Where Isduplicate=0
)a
where rnk=1