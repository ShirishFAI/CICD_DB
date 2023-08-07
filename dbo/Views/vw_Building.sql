
/****************************************************************************************************************************************
Building deduplication -- 01:10:02
****************************************************************************************************************************************/
create view dbo.vw_Building as
select * from (
	Select *
	,ROW_NUMBER() Over(Partition By PIN,ProvinceCode,BuildingDescription,BuildingFeet,BuildingHeight,BuildingLength,BuildingM2,BuildingMeasureUnit
	,BuildingMetre,TypeOfPermit,BuildingSqft,BuildingStyle,BuildingType,BuildingTypeCode,YearBuilt,NumberOfStories,NumberOfUnits,LivingAreaSQFT
	,HouseTypeCode,CondoLevel,CondominumClause,CondoPlanNumber,CondoUnitNumber,HouseArea,FrontDirection,CondoExposure,NumberOfBedrooms,BedroomPlus
	,BedroomString,NumberOfWashroom,Furnished,DenFront,Description1,Description2,Amenities0,Amenities1,Amenities2,Amenities3,Amenities4,Pool,Level
	,Locker,MaintenanceFee,GarageType,ParkingType,Parking,ParkingGarage,ParkingText,ParkingTotal,UtilitiesIncluded,Water,ConstructionMaterial,ConstructionStatus
	,ExteriorFinish,RoofMaterial,RoofStyle,Sewer,FoundationType,AirConditioning,Fireplace,FireplaceFuel,FireplaceType,Heating,HeatingFuel,BasementType,Basement
	,FinishedBasement,Rooms_0_Desc,Rooms_0_Level,Rooms_0_Size,Rooms_0_Type,Rooms_1_Desc,Rooms_1_Level,Rooms_1_Size,Rooms_1_Type,Rooms_2_Desc
	,Rooms_2_Level,Rooms_2_Size,Rooms_2_Type,Rooms_3_Desc,Rooms_3_Level,Rooms_3_Size,Rooms_3_Type,Rooms_4_Desc,Rooms_4_Level,Rooms_4_Size,Rooms_4_Type,Rooms_5_Desc
	,Rooms_5_Level,Rooms_5_Size,Rooms_5_Type,Rooms_6_Desc,Rooms_6_Level,Rooms_6_Size,Rooms_6_Type,Rooms_7_Desc,Rooms_7_Level,Rooms_7_Size,Rooms_7_Type,Rooms_8_Desc
	,Rooms_8_Level,Rooms_8_Size,Rooms_8_Type,Rooms_9_Desc,Rooms_9_Level,Rooms_9_Size,Rooms_9_Type,Rooms_10_Desc,Rooms_10_Level,Rooms_10_Size,Rooms_10_Type,Rooms_11_Desc
	,Rooms_11_Level,Rooms_11_Size,Rooms_11_Type,IsCondo,AttachedGarage,DetachedGarage,Fuel,Garage,IsMobileHome,IsNewHome,NewHomeEasement,EstateTypeCode,OccupancyTypeCode
	,PropertyCategory,PropertyClassification,Data_Source_ID,Data_Source_Priority,BasementFinishArea,BasementTotalArea,DeckSqFootage,DeckSqFootageCovered
	,Elevators,MezzanineArea,NumDens,OtherBuildingFlag,TotalBalconyArea,TypeofHeating Order by LastModifiedDateUTC,ID)  As RNK
	from dbo.Building b
	Where Isduplicate=0
)a
where rnk=1