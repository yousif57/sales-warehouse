-- Active: 1731745085190@@127.0.0.1@5432@postgres
SELECT * from main;

# sales dim
CREATE TABLE Sales_dim AS
SELECT "Type_of_Sale", "Sale_Condition"
FROM main;

ALTER TABLE Sales_dim
ADD COLUMN "SalesId" SERIAL PRIMARY KEY;

SELECT * from sales_dim;
#----------------------------------------------------------------

# date dim
CREATE TABLE Date_dim AS
SELECT "Year_Built", "Year_Remodeled", "Garage_Year_Built", "Month_Sold", "Year_Sold"
FROM main;

ALTER TABLE Date_dim
ADD COLUMN "DateId" SERIAL PRIMARY KEY;
#-----------------------------------------------------------------

#location dim
CREATE TABLE Location_dim AS
select "Street_Connection_Length_ft","Road_Access","Alley_Access","Property_Flatness",
    "Property_Slope","Neighborhood_Location","Proximity_to_Condition1","Proximity_to_Condition2",
    "Zoning_Classification"
from main;

ALTER TABLE location_dim
ADD COLUMN "LocationId" SERIAL PRIMARY KEY;
#------------------------------------------------------------------

#quality dim
CREATE TABLE Quality_dim AS
Select "Overall_Material_Quality", "Overall_Condition", "Exterior_Quality", "Exterior_Condition", "Basement_Height_Quality",
    "Basement_Condition", "Basement_Exposure", "Basement_Finish_Type1", "Basement_Finish_Type2", "Heating_Quality", 
    "Kitchen_Quality", "Fireplace_Quality", "Garage_Quality", "Garage_Condition", "Pool_Quality", "Fence_Quality"
FROM main;

ALTER TABLE Quality_dim
ADD COLUMN "QualityId" SERIAL PRIMARY KEY;
#----------------------------------------------------------------------

# Construction_dim
CREATE TABLE Construction_dim AS
SELECT "Building_Class", "Property_Shape", "Lot_Configuration", "Building_Type", "House_Style", "Roof_Style", "Roof_Material"
    "Exterior_Covering1", "Exterior_Covering2", "Masonry_Veneer_Type", "Masonry_Veneer_Area_sq_ft", "Foundation_Type", 
    "Home_Functionality"
FROM main;

ALTER TABLE Construction_dim
ADD COLUMN "ConstructionId" SERIAL PRIMARY KEY;
#---------------------------------------------------------------------

# Facilities_dim
CREATE TABLE Facilities_dim AS
SELECT "Heating_Type", "Electrical_System", "Central_Air_Conditioning", "Number_of_Fireplaces", "Pool_Area_sq_ft", 
    "Garage_Area_sq_ft", "Garage_Capacity_Cars", "Garage_Interior_Finish", "Garage_Location_Type", "Driveway_Surface_Type",
    "Screen_Porch_Area_sq_ft", "Three_Season_Porch_Area_sq_ft", "Enclosed_Porch_Area_sq_ft", "Open_Porch_Area_sq_ft", 
    "Wood_Deck_Area_sq_ft", "Utilities_Available", "Miscellaneous_Feature", "Miscellaneous_Value_usd"
    
FROM main;

ALTER TABLE Facilities_dim
ADD COLUMN "FacilitiesId" SERIAL PRIMARY KEY;
#---------------------------------------------------------------------

# property_fact
CREATE TABLE Property_fact AS
SELECT "Lot_Size_sq_ft", "Total_Basement_Area_sq_ft", "Unfinished_Basement_Area_sq_ft", "Basement_Finished_Area1_sq_ft",
    "Basement_Finished_Area2_sq_ft", "Above_Grade_Living_Area_sq_ft", "Low_Quality_Finished_Area_sq_ft", "First_Floor_Area_sq_ft", 
    "Half_Bathrooms_Above_Grade", "Full_Bathrooms_Above_Grade", "Basement_Half_Bathrooms", 
    "Basement_Full_Bathrooms", "Second_Floor_Area_sq_ft", "Bedrooms_Above_Grade", "Kitchens_Above_Grade", 
    "Total_Rooms_Above_Grade"
FROM main;

ALTER TABLE Property_fact
ADD CONSTRAINT "SalesId" Foreign Key ("SalesId") REFERENCES Sales_dim("SalesId"),
ADD CONSTRAINT "DateId" Foreign Key ("DateId") REFERENCES Date_dim("DateId"),
ADD CONSTRAINT "LocationId" Foreign Key ("LocationId") REFERENCES Location_dim("LocationId"),
ADD CONSTRAINT "QualityId" Foreign Key ("QualityId") REFERENCES Quality_dim("QualityId"),
ADD CONSTRAINT "ConstructionId" Foreign Key ("ConstructionId") REFERENCES Construction_dim("ConstructionId"),
ADD CONSTRAINT "FacilitiesId" Foreign Key ("FacilitiesId") REFERENCES Facilities_dim("FacilitiesId");
#---------------------------------------------------------------------

