import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text


elements_mappings = {
    'MSSubClass': {
        20: '1-STORY 1946 & NEWER ALL STYLES',
        30: '1-STORY 1945 & OLDER',
        40: '1-STORY W/FINISHED ATTIC ALL AGES',
        45: '1-1/2 STORY - UNFINISHED ALL AGES',
        50: '1-1/2 STORY FINISHED ALL AGES',
        60: '2-STORY 1946 & NEWER',
        70: '2-STORY 1945 & OLDER',
        75: '2-1/2 STORY ALL AGES',
        80: 'SPLIT OR MULTI-LEVEL',
        85: 'SPLIT FOYER',
        90: 'DUPLEX - ALL STYLES AND AGES',
        120: '1-STORY PUD - 1946 & NEWER',
        150: '1-1/2 STORY PUD - ALL AGES',
        160: '2-STORY PUD - 1946 & NEWER',
        180: 'PUD - MULTILEVEL - INCL SPLIT LEV/FOYER',
        190: '2 FAMILY CONVERSION - ALL STYLES AND AGES'
    },
    
    'MSZoning': {
        'A': 'Agriculture',
        'C': 'Commercial',
        'FV': 'Floating Village Residential',
        'I': 'Industrial',
        'RH': 'Residential High Density',
        'RL': 'Residential Low Density',
        'RP': 'Residential Low Density Park',
        'RM': 'Residential Medium Density'
    },
    
    'Street': {
        'Grvl': 'Gravel',
        'Pave': 'Paved'
    },
    
    'Alley': {
        'Grvl': 'Gravel',
        'Pave': 'Paved',
        np.nan: 'No alley access'
    },
    
    'LotShape': {
        'Reg': 'Regular',
        'IR1': 'Slightly irregular',
        'IR2': 'Moderately Irregular',
        'IR3': 'Irregular'
    },
    
    'LandContour': {
        'Lvl': 'Near Flat/Level',
        'Bnk': 'Banked - Quick and significant rise',
        'HLS': 'Hillside - Significant slope',
        'Low': 'Depression'
    },
    
    'Utilities': {
        'AllPub': 'All public Utilities',
        'NoSewr': 'Electricity, Gas, and Water',
        'NoSeWa': 'Electricity and Gas Only',
        'ELO': 'Electricity only'
    },
    
    'LotConfig': {
        'Inside': 'Inside lot',
        'Corner': 'Corner lot',
        'CulDSac': 'Cul-de-sac',
        'FR2': 'Frontage on 2 sides',
        'FR3': 'Frontage on 3 sides'
    },
    
    'LandSlope': {
        'Gtl': 'Gentle slope',
        'Mod': 'Moderate Slope',
        'Sev': 'Severe Slope'
    },
    
    'Neighborhood': {
        'Blmngtn': 'Bloomington Heights',
        'Blueste': 'Bluestem',
        'BrDale': 'Briardale',
        'BrkSide': 'Brookside',
        'ClearCr': 'Clear Creek',
        'CollgCr': 'College Creek',
        'Crawfor': 'Crawford',
        'Edwards': 'Edwards',
        'Gilbert': 'Gilbert',
        'IDOTRR': 'Iowa DOT and Rail Road',
        'MeadowV': 'Meadow Village',
        'Mitchel': 'Mitchell',
        'Names': 'North Ames',
        'NoRidge': 'Northridge',
        'NPkVill': 'Northpark Villa',
        'NridgHt': 'Northridge Heights',
        'NWAmes': 'Northwest Ames',
        'OldTown': 'Old Town',
        'SWISU': 'South & West of Iowa State University',
        'Sawyer': 'Sawyer',
        'SawyerW': 'Sawyer West',
        'Somerst': 'Somerset',
        'StoneBr': 'Stone Brook',
        'Timber': 'Timberland',
        'Veenker': 'Veenker'
    },
    
    'BldgType': {
        '1Fam': 'Single-family Detached',
        '2FmCon': 'Two-family Conversion',
        'Duplx': 'Duplex',
        'TwnhsE': 'Townhouse End Unit',
        'TwnhsI': 'Townhouse Inside Unit'
    },
    
    'HouseStyle': {
        '1Story': 'One story',
        '1.5Fin': 'One and one-half story: 2nd level finished',
        '1.5Unf': 'One and one-half story: 2nd level unfinished',
        '2Story': 'Two story',
        '2.5Fin': 'Two and one-half story: 2nd level finished',
        '2.5Unf': 'Two and one-half story: 2nd level unfinished',
        'SFoyer': 'Split Foyer',
        'SLvl': 'Split Level'
    },
    
    'RoofStyle': {
        'Flat': 'Flat',
        'Gable': 'Gable',
        'Gambrel': 'Gabrel (Barn)',
        'Hip': 'Hip',
        'Mansard': 'Mansard',
        'Shed': 'Shed'
    },
    
    'RoofMatl': {
        'ClyTile': 'Clay or Tile',
        'CompShg': 'Standard (Composite) Shingle',
        'Membran': 'Membrane',
        'Metal': 'Metal',
        'Roll': 'Roll',
        'Tar&Grv': 'Gravel & Tar',
        'WdShake': 'Wood Shakes',
        'WdShngl': 'Wood Shingles'
    },
    
    'ExterQual': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Average/Typical',
        'Fa': 'Fair',
        'Po': 'Poor'
    },
    
    'ExterCond': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Average/Typical',
        'Fa': 'Fair',
        'Po': 'Poor'
    },
    
    'Foundation': {
        'BrkTil': 'Brick & Tile',
        'CBlock': 'Cinder Block',
        'PConc': 'Poured Concrete',
        'Slab': 'Slab',
        'Stone': 'Stone',
        'Wood': 'Wood'
    },
    
    'Heating': {
        'Floor': 'Floor Furnace',
        'GasA': 'Gas forced warm air furnace',
        'GasW': 'Gas hot water or steam heat',
        'Grav': 'Gravity furnace',
        'OthW': 'Hot water or steam heat other than gas',
        'Wall': 'Wall furnace'
    },
    
    'CentralAir': {
        'N': 'No',
        'Y': 'Yes'
    },
    
    'Functional': {
        'Typ': 'Typical Functionality',
        'Min1': 'Minor Deductions 1',
        'Min2': 'Minor Deductions 2',
        'Mod': 'Moderate Deductions',
        'Maj1': 'Major Deductions 1',
        'Maj2': 'Major Deductions 2',
        'Sev': 'Severely Damaged',
        'Sal': 'Salvage only'
    },
    
    'GarageType': {
        '2Types': 'More than one type of garage',
        'Attchd': 'Attached to home',
        'Basment': 'Basement Garage',
        'BuiltIn': 'Built-In',
        'CarPort': 'Car Port',
        'Detchd': 'Detached from home',
        np.nan: 'No Garage'
    },
    
    'PavedDrive': {
        'Y': 'Paved',
        'P': 'Partial Pavement',
        'N': 'Dirt/Gravel'
    },
    
    'PoolQC': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Average/Typical',
        'Fa': 'Fair',
        np.nan: 'No Pool'
    },
    
    'Fence': {
        'GdPrv': 'Good Privacy',
        'MnPrv': 'Minimum Privacy',
        'GdWo': 'Good Wood',
        'MnWw': 'Minimum Wood/Wire',
        np.nan: 'No Fence'
    },
    
    'SaleType': {
        'WD': 'Warranty Deed - Conventional',
        'CWD': 'Warranty Deed - Cash',
        'VWD': 'Warranty Deed - VA Loan',
        'New': 'Home just constructed and sold',
        'COD': 'Court Officer Deed/Estate',
        'Con': 'Contract 15% Down payment regular terms',
        'ConLw': 'Contract Low Down payment and low interest',
        'ConLI': 'Contract Low Interest',
        'ConLD': 'Contract Low Down',
        'Oth': 'Other'
    },
    
    'SaleCondition': {
        'Normal': 'Normal Sale',
        'Abnorml': 'Abnormal Sale - trade, foreclosure, short sale',
        'AdjLand': 'Adjoining Land Purchase',
        'Alloca': 'Allocation - two linked properties with separate deeds',
        'Family': 'Sale between family members',
        'Partial': 'Home was not completed when last assessed'
    },

    'Condition1': {
            'Artery': 'Adjacent to arterial street',
            'Feedr': 'Adjacent to feeder street',
            'Norm': 'Normal',
            'RRNn': 'Within 200\' of North-South Railroad',
            'RRAn': 'Adjacent to North-South Railroad',
            'PosN': 'Near positive off-site feature--park, greenbelt, etc.',
            'PosA': 'Adjacent to positive off-site feature',
            'RRNe': 'Within 200\' of East-West Railroad',
            'RRAe': 'Adjacent to East-West Railroad'
        },
    
    'Condition2': {
        'Artery': 'Adjacent to arterial street',
        'Feedr': 'Adjacent to feeder street',
        'Norm': 'Normal',
        'RRNn': 'Within 200\' of North-South Railroad',
        'RRAn': 'Adjacent to North-South Railroad',
        'PosN': 'Near positive off-site feature--park, greenbelt, etc.',
        'PosA': 'Adjacent to positive off-site feature',
        'RRNe': 'Within 200\' of East-West Railroad',
        'RRAe': 'Adjacent to East-West Railroad'
    },

    'Exterior1st': {
        'AsbShng': 'Asbestos Shingles',
        'AsphShn': 'Asphalt Shingles',
        'BrkComm': 'Brick Common',
        'BrkFace': 'Brick Face',
        'CBlock': 'Cinder Block',
        'CemntBd': 'Cement Board',
        'HdBoard': 'Hard Board',
        'ImStucc': 'Imitation Stucco',
        'MetalSd': 'Metal Siding',
        'Other': 'Other',
        'Plywood': 'Plywood',
        'PreCast': 'PreCast',
        'Stone': 'Stone',
        'Stucco': 'Stucco',
        'VinylSd': 'Vinyl Siding',
        'Wd Sdng': 'Wood Siding',
        'WdShing': 'Wood Shingles'
    },

    'Exterior2nd': {
        'AsbShng': 'Asbestos Shingles',
        'AsphShn': 'Asphalt Shingles',
        'BrkComm': 'Brick Common',
        'BrkFace': 'Brick Face',
        'CBlock': 'Cinder Block',
        'CemntBd': 'Cement Board',
        'HdBoard': 'Hard Board',
        'ImStucc': 'Imitation Stucco',
        'MetalSd': 'Metal Siding',
        'Other': 'Other',
        'Plywood': 'Plywood',
        'PreCast': 'PreCast',
        'Stone': 'Stone',
        'Stucco': 'Stucco',
        'VinylSd': 'Vinyl Siding',
        'Wd Sdng': 'Wood Siding',
        'WdShing': 'Wood Shingles'
    },

    'MasVnrType': {
        'BrkCmn': 'Brick Common',
        'BrkFace': 'Brick Face',
        'CBlock': 'Cinder Block',
        np.nan: 'None',
        'Stone': 'Stone'
    },

    'BsmtQual': {
        'Ex': 'Excellent (100+ inches)',
        'Gd': 'Good (90-99 inches)',
        'TA': 'Typical (80-89 inches)',
        'Fa': 'Fair (70-79 inches)',
        'Po': 'Poor (<70 inches)',
        np.nan: 'No Basement'
    },

    'BsmtCond': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Typical - slight dampness allowed',
        'Fa': 'Fair - dampness or some cracking or settling',
        'Po': 'Poor - Severe cracking, settling, or wetness',
        np.nan: 'No Basement'
    },

    'BsmtExposure': {
        'Gd': 'Good Exposure',
        'Av': 'Average Exposure',
        'Mn': 'Minimum Exposure',
        'No': 'No Exposure',
        np.nan: 'No Basement'
    },

    'BsmtFinType1': {
        'GLQ': 'Good Living Quarters',
        'ALQ': 'Average Living Quarters',
        'BLQ': 'Below Average Living Quarters',
        'Rec': 'Average Rec Room',
        'LwQ': 'Low Quality',
        'Unf': 'Unfinished',
        np.nan: 'No Basement'
    },

    'BsmtFinType2': {
        'GLQ': 'Good Living Quarters',
        'ALQ': 'Average Living Quarters',
        'BLQ': 'Below Average Living Quarters',
        'Rec': 'Average Rec Room',
        'LwQ': 'Low Quality',
        'Unf': 'Unfinished',
        np.nan: 'No Basement'
    },

    'HeatingQC': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Average/Typical',
        'Fa': 'Fair',
        'Po': 'Poor'
    },

    'Electrical': {
        'SBrkr': 'Standard Circuit Breakers & Romex',
        'FuseA': 'Fuse Box over 60 AMP and all Romex wiring (Average)',
        'FuseF': '60 AMP Fuse Box and mostly Romex wiring (Fair)',
        'FuseP': '60 AMP Fuse Box and mostly knob & tube wiring (poor)',
        'Mix': 'Mixed'
    },

    'KitchenQual': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Typical/Average',
        'Fa': 'Fair',
        'Po': 'Poor'
    },

    'FireplaceQu': {
        'Ex': 'Excellent - Exceptional Masonry Fireplace',
        'Gd': 'Good - Masonry Fireplace in main level',
        'TA': 'Average - Prefabricated Fireplace in main living area or Masonry Fireplace in basement',
        'Fa': 'Fair - Prefabricated Fireplace in basement',
        'Po': 'Poor - Ben Franklin Stove',
        np.nan: 'No Fireplace'
    },

    'GarageFinish': {
        'Fin': 'Finished',
        'RFn': 'Rough Finished',
        'Unf': 'Unfinished',
        np.nan: 'No Garage'
    },

    'GarageQual': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Typical/Average',
        'Fa': 'Fair',
        'Po': 'Poor',
        np.nan: 'No Garage'
    },

    'GarageCond': {
        'Ex': 'Excellent',
        'Gd': 'Good',
        'TA': 'Typical/Average',
        'Fa': 'Fair',
        'Po': 'Poor',
        np.nan: 'No Garage'
    },

    'MiscFeature': {
        'Elev': 'Elevator',
        'Gar2': '2nd Garage (if not described in garage section)',
        'Othr': 'Other',
        'Shed': 'Shed (over 100 SF)',
        'TenC': 'Tennis Court',
        np.nan: 'None'
    }
}

features_mapping = {
    "MSSubClass": "Building_Class",
    "MSZoning": "Zoning_Classification",
    "LotFrontage": "Street_Connection_Length_ft",
    "LotArea": "Lot_Size_sq_ft",
    "Street": "Road_Access",
    "Alley": "Alley_Access",
    "LotShape": "Property_Shape",
    "LandContour": "Property_Flatness",
    "Utilities": "Utilities_Available",
    "LotConfig": "Lot_Configuration",
    "LandSlope": "Property_Slope",
    "Neighborhood": "Neighborhood_Location",
    "Condition1": "Proximity_to_Condition1",
    "Condition2": "Proximity_to_Condition2",
    "BldgType": "Building_Type",
    "HouseStyle": "House_Style",
    "OverallQual": "Overall_Material_Quality",
    "OverallCond": "Overall_Condition",
    "YearBuilt": "Year_Built",
    "YearRemodAdd": "Year_Remodeled",
    "RoofStyle": "Roof_Style",
    "RoofMatl": "Roof_Material",
    "Exterior1st": "Exterior_Covering1",
    "Exterior2nd": "Exterior_Covering2",
    "MasVnrType": "Masonry_Veneer_Type",
    "MasVnrArea": "Masonry_Veneer_Area_sq_ft",
    "ExterQual": "Exterior_Quality",
    "ExterCond": "Exterior_Condition",
    "Foundation": "Foundation_Type",
    "BsmtQual": "Basement_Height_Quality",
    "BsmtCond": "Basement_Condition",
    "BsmtExposure": "Basement_Exposure",
    "BsmtFinType1": "Basement_Finish_Type1",
    "BsmtFinSF1": "Basement_Finished_Area1_sq_ft",
    "BsmtFinType2": "Basement_Finish_Type2",
    "BsmtFinSF2": "Basement_Finished_Area2_sq_ft",
    "BsmtUnfSF": "Unfinished_Basement_Area_sq_ft",
    "TotalBsmtSF": "Total_Basement_Area_sq_ft",
    "Heating": "Heating_Type",
    "HeatingQC": "Heating_Quality",
    "CentralAir": "Central_Air_Conditioning",
    "Electrical": "Electrical_System",
    "1stFlrSF": "First_Floor_Area_sq_ft",
    "2ndFlrSF": "Second_Floor_Area_sq_ft",
    "LowQualFinSF": "Low_Quality_Finished_Area_sq_ft",
    "GrLivArea": "Above_Grade_Living_Area_sq_ft",
    "BsmtFullBath": "Basement_Full_Bathrooms",
    "BsmtHalfBath": "Basement_Half_Bathrooms",
    "FullBath": "Full_Bathrooms_Above_Grade",
    "HalfBath": "Half_Bathrooms_Above_Grade",
    "Bedroom": "Bedrooms_Above_Grade",
    "Kitchen": "Kitchens_Above_Grade",
    "KitchenQual": "Kitchen_Quality",
    "TotRmsAbvGrd": "Total_Rooms_Above_Grade",
    "Functional": "Home_Functionality",
    "Fireplaces": "Number_of_Fireplaces",
    "FireplaceQu": "Fireplace_Quality",
    "GarageType": "Garage_Location_Type",
    "GarageYrBlt": "Garage_Year_Built",
    "GarageFinish": "Garage_Interior_Finish",
    "GarageCars": "Garage_Capacity_Cars",
    "GarageArea": "Garage_Area_sq_ft",
    "GarageQual": "Garage_Quality",
    "GarageCond": "Garage_Condition",
    "PavedDrive": "Driveway_Surface_Type",
    "WoodDeckSF": "Wood_Deck_Area_sq_ft",
    "OpenPorchSF": "Open_Porch_Area_sq_ft",
    "EnclosedPorch": "Enclosed_Porch_Area_sq_ft",
    "3SsnPorch": "Three_Season_Porch_Area_sq_ft",
    "ScreenPorch": "Screen_Porch_Area_sq_ft",
    "PoolArea": "Pool_Area_sq_ft",
    "PoolQC": "Pool_Quality",
    "Fence": "Fence_Quality",
    "MiscFeature": "Miscellaneous_Feature",
    "MiscVal": "Miscellaneous_Value_usd",
    "MoSold": "Month_Sold",
    "YrSold": "Year_Sold",
    "SaleType": "Type_of_Sale",
    "SaleCondition": "Sale_Condition"
}


def Extract_transform():

    train = pd.read_csv("database/train.csv").drop(["SalePrice", "Id"], axis=1)
    test = pd.read_csv("database/test.csv")

    df = pd.concat([train, test])

    # remap elements to orginal form & fill in any missing data
    df_final = df.replace(elements_mappings).bfill()
    # rename columns to more readable form
    df_final = df_final.rename(features_mapping, axis=1)

    return df_final


def load(connection_string, df):
    engine = create_engine(connection_string)

    df.to_sql(name="main", con=engine, index=False, if_exists="replace")

    engine.



if __name__ == "__main__":
    connection_string = "postgresql://postgres:root@localhost:5432/postgres"

    