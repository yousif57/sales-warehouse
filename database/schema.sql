-- Active: 1731745085190@@127.0.0.1@5432@postgres

#Sales Dimension
CREATE TABLE Sales_dim (
    "SalesId" SERIAL PRIMARY KEY,
    "Type_of_Sale" VARCHAR(255),
    "Sale_Condition" VARCHAR(255)
);

INSERT INTO Sales_dim ("Type_of_Sale", "Sale_Condition")
SELECT "Type_of_Sale", "Sale_Condition" FROM main;
#------------------------------------------------------------------

#date dim;
CREATE TABLE Date_dim (
    "DateId" SERIAL PRIMARY KEY,
    "Year_Built" INT,
    "Year_Remodeled" INT,
    "Garage_Year_Built" INT,
    "Month_Sold" INT,
    "Year_Sold" INT
);

INSERT INTO Date_dim ("Year_Built", "Year_Remodeled", "Garage_Year_Built", "Month_Sold", "Year_Sold")
SELECT "Year_Built", "Year_Remodeled", "Garage_Year_Built", "Month_Sold", "Year_Sold"
FROM main;
#------------------------------------------------------------------

#location dim
CREATE TABLE Location_dim (
    "LocationId" SERIAL PRIMARY KEY,
    "Street_Connection_Length_ft" INT,
    "Road_Access" VARCHAR(255),
    "Alley_Access" VARCHAR(255),
    "Property_Flatness" VARCHAR(255),
    "Property_Slope" VARCHAR(255),
    "Neighborhood_Location" VARCHAR(255),
    "Proximity_to_Condition1" VARCHAR(255),
    "Proximity_to_Condition2" VARCHAR(255),
    "Zoning_Classification" VARCHAR(255)
);

INSERT INTO Location_dim (
    "Street_Connection_Length_ft", "Road_Access", "Alley_Access", "Property_Flatness", 
    "Property_Slope", "Neighborhood_Location", "Proximity_to_Condition1", 
    "Proximity_to_Condition2", "Zoning_Classification"
)
SELECT 
    "Street_Connection_Length_ft", "Road_Access", "Alley_Access", "Property_Flatness", 
    "Property_Slope", "Neighborhood_Location", "Proximity_to_Condition1", 
    "Proximity_to_Condition2", "Zoning_Classification"
FROM main;
#------------------------------------------------------------------

#quality dim
CREATE TABLE Quality_dim (
    "QualityId" SERIAL PRIMARY KEY,
    "Overall_Material_Quality" VARCHAR(255),
    "Overall_Condition" VARCHAR(255),
    "Exterior_Quality" VARCHAR(255),
    "Exterior_Condition" VARCHAR(255),
    "Basement_Height_Quality" VARCHAR(255),
    "Basement_Condition" VARCHAR(255),
    "Basement_Exposure" VARCHAR(255),
    "Basement_Finish_Type1" VARCHAR(255),
    "Basement_Finish_Type2" VARCHAR(255),
    "Heating_Quality" VARCHAR(255),
    "Kitchen_Quality" VARCHAR(255),
    "Fireplace_Quality" VARCHAR(255),
    "Garage_Quality" VARCHAR(255),
    "Garage_Condition" VARCHAR(255),
    "Pool_Quality" VARCHAR(255),
    "Fence_Quality" VARCHAR(255)
);

INSERT INTO Quality_dim (
    "Overall_Material_Quality", "Overall_Condition", "Exterior_Quality", "Exterior_Condition", 
    "Basement_Height_Quality", "Basement_Condition", "Basement_Exposure", 
    "Basement_Finish_Type1", "Basement_Finish_Type2", "Heating_Quality", "Kitchen_Quality", 
    "Fireplace_Quality", "Garage_Quality", "Garage_Condition", "Pool_Quality", "Fence_Quality"
)
SELECT 
    "Overall_Material_Quality", "Overall_Condition", "Exterior_Quality", "Exterior_Condition", 
    "Basement_Height_Quality", "Basement_Condition", "Basement_Exposure", 
    "Basement_Finish_Type1", "Basement_Finish_Type2", "Heating_Quality", "Kitchen_Quality", 
    "Fireplace_Quality", "Garage_Quality", "Garage_Condition", "Pool_Quality", "Fence_Quality"
FROM main;
#----------------------------------------------------------------------

# Construction_dim
CREATE TABLE Construction_dim (
    "ConstructionId" SERIAL PRIMARY KEY,
    "Building_Class" VARCHAR(255),
    "Property_Shape" VARCHAR(255),
    "Lot_Configuration" VARCHAR(255),
    "Building_Type" VARCHAR(255),
    "House_Style" VARCHAR(255),
    "Roof_Style" VARCHAR(255),
    "Roof_Material" VARCHAR(255),
    "Exterior_Covering1" VARCHAR(255),
    "Exterior_Covering2" VARCHAR(255),
    "Masonry_Veneer_Type" VARCHAR(255),
    "Masonry_Veneer_Area_sq_ft" INT,
    "Foundation_Type" VARCHAR(255),
    "Home_Functionality" VARCHAR(255)
);

INSERT INTO Construction_dim (
    "Building_Class", "Property_Shape", "Lot_Configuration", "Building_Type", "House_Style", 
    "Roof_Style", "Roof_Material", "Exterior_Covering1", "Exterior_Covering2", 
    "Masonry_Veneer_Type", "Masonry_Veneer_Area_sq_ft", "Foundation_Type", "Home_Functionality"
)
SELECT 
    "Building_Class", "Property_Shape", "Lot_Configuration", "Building_Type", "House_Style", 
    "Roof_Style", "Roof_Material", "Exterior_Covering1", "Exterior_Covering2", 
    "Masonry_Veneer_Type", "Masonry_Veneer_Area_sq_ft", "Foundation_Type", "Home_Functionality"
FROM main;
#---------------------------------------------------------------------

# Facilities_dim
CREATE TABLE Facilities_dim (
    "FacilitiesId" SERIAL PRIMARY KEY,
    "Heating_Type" VARCHAR(255),
    "Electrical_System" VARCHAR(255),
    "Central_Air_Conditioning" VARCHAR(255),
    "Number_of_Fireplaces" INT,
    "Pool_Area_sq_ft" INT,
    "Garage_Area_sq_ft" INT,
    "Garage_Capacity_Cars" INT,
    "Garage_Interior_Finish" VARCHAR(255),
    "Garage_Location_Type" VARCHAR(255),
    "Driveway_Surface_Type" VARCHAR(255),
    "Screen_Porch_Area_sq_ft" INT,
    "Three_Season_Porch_Area_sq_ft" INT,
    "Enclosed_Porch_Area_sq_ft" INT,
    "Open_Porch_Area_sq_ft" INT,
    "Wood_Deck_Area_sq_ft" INT,
    "Utilities_Available" VARCHAR(255),
    "Miscellaneous_Feature" VARCHAR(255),
    "Miscellaneous_Value_usd" NUMERIC
);

INSERT INTO Facilities_dim (
    "Heating_Type", "Electrical_System", "Central_Air_Conditioning", "Number_of_Fireplaces", 
    "Pool_Area_sq_ft", "Garage_Area_sq_ft", "Garage_Capacity_Cars", "Garage_Interior_Finish", 
    "Garage_Location_Type", "Driveway_Surface_Type", "Screen_Porch_Area_sq_ft", 
    "Three_Season_Porch_Area_sq_ft", "Enclosed_Porch_Area_sq_ft", "Open_Porch_Area_sq_ft", 
    "Wood_Deck_Area_sq_ft", "Utilities_Available", "Miscellaneous_Feature", "Miscellaneous_Value_usd"
)
SELECT 
    "Heating_Type", "Electrical_System", "Central_Air_Conditioning", "Number_of_Fireplaces", 
    "Pool_Area_sq_ft", "Garage_Area_sq_ft", "Garage_Capacity_Cars", "Garage_Interior_Finish", 
    "Garage_Location_Type", "Driveway_Surface_Type", "Screen_Porch_Area_sq_ft", 
    "Three_Season_Porch_Area_sq_ft", "Enclosed_Porch_Area_sq_ft", "Open_Porch_Area_sq_ft", 
    "Wood_Deck_Area_sq_ft", "Utilities_Available", "Miscellaneous_Feature", "Miscellaneous_Value_usd"
FROM main;
#---------------------------------------------------------------------

# property_fact
CREATE TABLE Property_fact (
    "PropertyFactId" SERIAL PRIMARY KEY,
    "SalesId" INT,
    "DateId" INT,
    "LocationId" INT,
    "QualityId" INT,
    "ConstructionId" INT,
    "FacilitiesId" INT,
    "Lot_Size_sq_ft" INT,
    "Total_Basement_Area_sq_ft" INT,
    "Unfinished_Basement_Area_sq_ft" INT,
    "Basement_Finished_Area1_sq_ft" INT,
    "Basement_Finished_Area2_sq_ft" INT,
    "Above_Grade_Living_Area_sq_ft" INT,
    "Low_Quality_Finished_Area_sq_ft" INT,
    "First_Floor_Area_sq_ft" INT,
    "Half_Bathrooms_Above_Grade" INT,
    "Full_Bathrooms_Above_Grade" INT,
    "Basement_Half_Bathrooms" INT,
    "Basement_Full_Bathrooms" INT,
    "Second_Floor_Area_sq_ft" INT,
    "Bedrooms_Above_Grade" INT,
    "Kitchens_Above_Grade" INT,
    "Total_Rooms_Above_Grade" INT,
    FOREIGN KEY ("SalesId") REFERENCES Sales_dim("SalesId"),
    FOREIGN KEY ("DateId") REFERENCES Date_dim("DateId"),
    FOREIGN KEY ("LocationId") REFERENCES Location_dim("LocationId"),
    FOREIGN KEY ("QualityId") REFERENCES Quality_dim("QualityId"),
    FOREIGN KEY ("ConstructionId") REFERENCES Construction_dim("ConstructionId"),
    FOREIGN KEY ("FacilitiesId") REFERENCES Facilities_dim("FacilitiesId")
);

INSERT INTO Property_fact (
    "Lot_Size_sq_ft", "Total_Basement_Area_sq_ft", "Unfinished_Basement_Area_sq_ft", 
    "Basement_Finished_Area1_sq_ft", "Basement_Finished_Area2_sq_ft", 
    "Above_Grade_Living_Area_sq_ft", "Low_Quality_Finished_Area_sq_ft", 
    "First_Floor_Area_sq_ft", "Half_Bathrooms_Above_Grade", "Full_Bathrooms_Above_Grade", 
    "Basement_Half_Bathrooms", "Basement_Full_Bathrooms", "Second_Floor_Area_sq_ft", 
    "Bedrooms_Above_Grade", "Kitchens_Above_Grade", "Total_Rooms_Above_Grade"
)
SELECT
    "Lot_Size_sq_ft", "Total_Basement_Area_sq_ft", "Unfinished_Basement_Area_sq_ft", 
    "Basement_Finished_Area1_sq_ft", "Basement_Finished_Area2_sq_ft", 
    "Above_Grade_Living_Area_sq_ft", "Low_Quality_Finished_Area_sq_ft", 
    "First_Floor_Area_sq_ft", "Half_Bathrooms_Above_Grade", "Full_Bathrooms_Above_Grade", 
    "Basement_Half_Bathrooms", "Basement_Full_Bathrooms", "Second_Floor_Area_sq_ft", 
    "Bedrooms_Above_Grade", "Kitchens_Above_Grade", "Total_Rooms_Above_Grade"
FROM main;
#------------------------------------------------------------------