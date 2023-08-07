/****************************************************************************************************************************************
Sales deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Sales as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By LastSaleDate,SaleType,PurchasePrice,OriginalPurchasePrice,BuyerName,PriceSold,LastSaleAmount
	,LastSaleYear,ClosingDate,POSDateSales,StatusID,Data_Source_ID Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Sales b
	Where Isduplicate=0
)a
where rnk=1