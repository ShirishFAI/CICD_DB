/****************************************************************************************************************************************
Parcel deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Parcel as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By PIN,ProvinceCode,Acreage,LotDepth,LotFrontage,IsNativeLand,IsEnergy,IsVacantLand,IsRenovatedLotNum,MetesAndBounds
	,PrimaryProperty,GVSEligible,LotMeasureUnit,LotSQM,LotSQFT,LotHA,LandSQFT,LotDescription,LotSize,LandType,LandUse,PlanNumber,ZoningDescription
	,ZoningCode,PropertyTypeCode,PropertyUse,Easement,LegalDescription,Sequence,Data_Source_ID,IsPartLot,LegalDescriptionBlock,LegalDescriptionDistrictLot
	,LegalDescriptionExceptPlan,LegalDescriptionLegalSubdivision,LegalDescriptionLegalText,LegalDescriptionLot,LegalDescriptionParcel,LegalDescriptionPart1
	,LegalDescriptionPart2,LegalDescriptionPart3,LegalDescriptionPart4,LegalDescriptionPortion,LegalDescriptionSection,LegalDescriptionStrataLot
	,LegalDescriptionSubBlock,LegalDescriptionSubLot Order by LastModifiedDateUTC,ID) As RNK
	from dbo.Parcel b
	Where Isduplicate=0
)a
where rnk=1