/****************************************************************************************************************************************
Listing deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Listing as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By PIN,ProvinceCode,ARN,JurCode,MLSNumber,SellerName,DateEnd,DateStart,DateUpdate,ListDays,ListType
	,ListStatus,ListHistory,PriceAsked,FCTTransactionType,LoanAmt,LendingValue,GuaranteeValue,OwnershipType,RentAssignment
	,Data_Source_ID Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Listing b
	Where Isduplicate=0
)a
where rnk=1