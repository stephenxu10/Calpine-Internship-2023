import pandas as pd
import os
import time
import warnings
import calendar

warnings.simplefilter("ignore")

"""
This Python scripts aims to aggregate Resource-Level information across historical Wind and Solar Power
Monthly reports. 

Currently aggregates wind and solar information into separate files in the Data folder.
"""

# Version 1 aggregates Solar Data, Version 2 aggregates Wind Data.
version = 1

# Global parameters & variables
start_time = time.time()
solar_base = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/04 - Monthly Updates/101 - Misc/01 - General/Wind Forecast Monthly"
output_path = (
    "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Wind and Solar Aggregates/Resource_Level_Solar.csv"
    if version == 1
    else "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Wind and Solar Aggregates/Resource_Level_Wind.csv"
)

file_key = "SolarPowerForecastMonthly_" if version == 1 else "WindPowerForecastMonthly_"

def month_code_to_number(month_code: str) -> int:
    """
    Convert a three-digit month code to its corresponding month number.

    Parameters:
        month_code (str): The three-digit month code (e.g., 'Dec', 'Jan').

    Returns:
        int: The month number (1 for January, 2 for February, etc.).
    """
    month_code_to_number_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    return month_code_to_number_map.get(month_code, -1)

def pull_sheet(sheet_name: str) -> pd.DataFrame():
    """
    Helper method that extracts the Resource-level information from an Excel sheet. Outputs a DataFrame
    that stores all desired data.

    Inputs:
        - sheet_name: The name of the Excel sheet (not the path!)

    Output:
        - The Pandas DataFrame storing the Resource-level information.    
    """
    assert(file_key in sheet_name)
    year = int(sheet_name[len(file_key) + 3: len(file_key) + 7])
    month_days = calendar.monthrange(year, month_code_to_number(sheet_name[len(file_key): len(file_key) + 3]))[1]
    full_path = os.path.join(solar_base, sheet_name)

    raw_df =  pd.read_excel(full_path, sheet_name="Resource to Region", usecols="A:E", skiprows=month_days + 14)
    raw_df = raw_df.reindex(columns=['Operating Day', 'Resource name', 'Region', 'Resource Capacity', 'Out of service date']) 
        
    return raw_df


merge = []
for file_name in os.listdir(solar_base):
    if file_key in file_name and not file_name.startswith("~"):
        resource_df = pull_sheet(file_name)
        merge.append(resource_df)

final_merged = pd.concat(merge, axis=0)
final_merged['Operating Day'] = pd.to_datetime(final_merged['Operating Day'])

final_merged = final_merged.sort_values(by=['Operating Day'])
final_merged.to_csv(output_path, index=False)


end_time = time.time()
execution_time = end_time - start_time
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
