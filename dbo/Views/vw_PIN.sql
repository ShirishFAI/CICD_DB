/****************************************************************************************************************************************
PIN deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_PIN as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By PIN,OriginalPIN,ProvinceCode,Data_Source_ID Order by LastModifiedDateUTC,ID) As RNK
	from dbo.PIN b
	Where Isduplicate=0
)a
where rnk=1