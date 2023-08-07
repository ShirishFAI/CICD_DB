/****************************************************************************************************************************************
Valuation deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Valuation as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By PIN,ProvinceCode,ARN,JurCode,EstimatedValue,HighValue,LowValue,CompleteDate,MPACValue,TERANETValue
	,InsuredValue,MPACConfidenceLevel,MPACPropertyType,POSDate,MPACLowConfidenceLimit,MPACHighConfidenceLimit,ValuePurchasePrice,AppraisedValue
	,Data_Source_ID Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Valuation b
	Where Isduplicate=0
)a
where rnk=1