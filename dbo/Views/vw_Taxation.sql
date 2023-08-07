/****************************************************************************************************************************************
Taxation deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Taxation as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By ARN,JurCode,AssessmentYear,AssessmentValue,AnnualTaxAmount,TaxYear,TaxAssessedValue,NetTax,GrossTax
										,AssessmentClass,ProvinceCode,Data_Source_ID Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Taxation b
	Where Isduplicate=0
)a
where rnk=1