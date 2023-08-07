/****************************************************************************************************************************************
Permit deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Permit as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By MasterAddressId,ProvinceCode,JurCode,ARN,AppliedDate,DateOfDecision,IssueDate,MustCommenceDate
	,CompletedDate,CanceledRefusedDate,DatePermitExpires,ValueOfConstruction,PermitClass,PermitDescription,PermitType,PermitFee
	,PermitNumber,PermitStatus,DwellingUnitsCreated,DwellingUnitsDemolished,UnitsNetChange,Data_Source_ID Order by LastModifiedDateUTC,ID)  As RNK
	from dbo.Permit b
	Where Isduplicate=0
)a
where rnk=1