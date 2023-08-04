# type: ignore

import pandas as pd
import os
import time
from typing import *
from datetime import datetime

# Global parameters and variables
path_base = f"//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/02 - Input/Generation/New Entrants/ERCOT Queue"

# Version 1 aggregates all historical data, version 2 aggregates only the last four reports.
version = 2

output_path = (
    "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/GIS Change Report/GIS_Historical_Aggregate.csv"
    if version == 1
    else "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/GIS Change Report/GIS_Last4release_Aggregate.csv"
)
start_time = time.time()

columns_to_read = [
    'INR',
    'Project Name',
    'Interconnecting Entity',
    'POI Location',
    'County',
    'Projected COD',
    'Fuel',
    'Technology',
    'Capacity (MW)',
    'Approved for Energization',
    'Approved for Synchronization',
]

start_year = 2019
end_year = 2050
current_year = datetime.now().year

def skip_rows(path_base: str, input_name: str, sheet: str) -> int:
    """
    A helper function that finds the number of rows to skip on an input GIS Report DataFrame
    in order to reach the actual Project Attributes.

    Inputs:
        - input_name: The input name (not the path) of the file to read
    
    Output:
        - The number of rows to skip. Should be around 20-30 rows.
    """
    # Start by skipping that pesky ERCOT image.
    df_large_gen = pd.read_excel(os.path.join(path_base, input_name), sheet_name=sheet, skiprows=6)
    first_col = df_large_gen.columns[0]
    additional_rows = 1

    # Iterate until the 'Project Attributes' header is encountered. This signifies the beginning of the DataFrame.
    for _, row in df_large_gen.iterrows():
        additional_rows += 1
        if row[first_col] == "Project Attributes":
            break
    
    return 6 + additional_rows

merge = []

if version == 1:
    for year in range(start_year, end_year + 1):
        yearly_base = os.path.join(path_base, str(year))
        if os.path.isdir(yearly_base):
            print(yearly_base)
            if year <= 2021:
                for month_sheet in os.listdir(yearly_base):
                    if month_sheet.endswith(".xlsx") and not month_sheet.startswith("~"):
                        print(month_sheet)
                        month_df = pd.read_excel(os.path.join(yearly_base, month_sheet), sheet_name="Project Details", skiprows = skip_rows(yearly_base, month_sheet, "Project Details"), 
                                                usecols=columns_to_read)
                        month_df = month_df.dropna(subset=['Project Name'])
                        month_df['GIS Report Name'] = month_sheet[month_sheet.index('GIS_Report'):-5]
                        merge.append(month_df)

            else:
                for month_sheet in os.listdir(yearly_base):
                    if not month_sheet.startswith("~"):
                        print(month_sheet)
                        month_df = pd.read_excel(os.path.join(yearly_base, month_sheet), sheet_name="Project Details - Large Gen", skiprows= skip_rows(yearly_base, month_sheet, "Project Details - Large Gen"), 
                                                    usecols=columns_to_read)
                        month_df = month_df.dropna(subset=['INR'])
                        month_df['GIS Report Name'] = month_sheet[month_sheet.index('GIS_Report'):-5]
                        merge.append(month_df)                

elif version == 2:
    yearly_base = os.path.join(path_base, str(current_year))
    files = [f for f in os.listdir(yearly_base) if not f.startswith("~")]

    for month_sheet in files[-4:]:
        print(month_sheet)
        month_df = pd.read_excel(os.path.join(yearly_base, month_sheet), sheet_name="Project Details - Large Gen", skiprows= skip_rows(yearly_base, month_sheet, "Project Details - Large Gen"), 
                            usecols=columns_to_read)
        month_df = month_df.dropna(subset=['INR'])
        month_df['GIS Report Name'] = month_sheet[month_sheet.index('GIS_Report'):-5]
        merge.append(month_df)            

merged_df = pd.concat(merge, axis=0)
merged_df.to_csv(output_path, index=False)


# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
