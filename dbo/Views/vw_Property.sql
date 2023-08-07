/****************************************************************************************************************************************
Property deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Property as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By MasterAddressID,PIN,ProvinceCode,ARN,JurCode,SUBSTRING(Code,1,Charindex('_',Code)-1) Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Property b
	Where Isduplicate=0
)a
where rnk=1