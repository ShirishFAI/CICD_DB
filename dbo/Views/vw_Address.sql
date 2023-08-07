/****************************************************************************************************************************************
Address deduplication 
****************************************************************************************************************************************/
create view dbo.vw_Address as
select * from (
Select *
,ROW_NUMBER() Over(Partition by Data_Source_ID,MasterAddressID order by LastModifiedDateUTC,ID) As RNK
from dbo.Address
Where MasterAddressID IS NOT NULL
--AND Data_Source_ID=@data_source_id
AND Isduplicate=0
	) a
	where rnk=1