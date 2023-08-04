import pandas as pd
import os
from datetime import datetime, timedelta
import time
from typing import *

"""
This Python script aims to compare Generator Projects from ERCOT's GIS Reports
for the last two months. In particular, the project finds the difference between
the projected COD (Commericial Operations Date) between corresponding 
projects and outputs that data to a CSV.
"""
# Global parameters and variables.
current_year = datetime.now().year
path_base = f"//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/02 - Input/Generation/New Entrants/ERCOT Queue/{current_year}"
start_time = time.time()

# The name of the sheet within the Excel file that we want to analyze.
sheet = "Project Details - Large Gen"

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

output_file = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/GIS Change Report/GIS_Delta_Previous2release.csv"

def get_last_two(input_path: str) -> Tuple[str, str]:
    """
    Quick helper method to grab the two most recently modified files
    from an input path base.

    Returns a tuple of the full paths to the most and second most recently
    modified files in the input path.
    """
    names = filter(lambda x: not x.startswith("~"), os.listdir(input_path))
    name_times = [(f, os.path.getmtime(os.path.join(input_path, f))) for f in names]
    name_times = sorted(name_times, key=lambda x: x[1], reverse=True)

    return name_times[1][0], name_times[0][0]

def skip_rows(input_name: str) -> int:
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

    # Iterate until the 'Project Attributes' header is encounterd. This signifies the beginning of the DataFrame.
    for _, row in df_large_gen.iterrows():
        additional_rows += 1
        if row[first_col] == "Project Attributes":
            break
    
    return 6 + additional_rows
    

last_month, curr_month = get_last_two(path_base)

df_last = pd.read_excel(os.path.join(path_base, last_month), sheet_name=sheet, skiprows=skip_rows(last_month), usecols=columns_to_read)
df_last = df_last.dropna(subset=['INR'])
df_last['GIS Report Name'] = last_month[last_month.index('GIS_Report'):-5]

df_curr = pd.read_excel(os.path.join(path_base, curr_month), sheet_name=sheet, skiprows=skip_rows(curr_month), usecols=columns_to_read)
df_curr = df_curr.dropna(subset=['INR'])

merged_df = pd.merge(df_last, df_curr, on='INR', suffixes=('', '_curr'), how='outer')
columns_to_drop = [col for col in merged_df.columns if col.endswith('_curr') and col != 'Projected COD_curr']

# merged_df.drop(columns=columns_to_drop, inplace=True)
merged_df.rename(columns={'Projected COD_curr': 'Current Projected COD'}, inplace=True)

"""
Divvy-up the raw merged DF into four categories:
    1) The Projects with non-zero change in Projected COD
    2) The Projects missing in the last month
    3) The Projects missing in the current month
    4) The Projects with unchanged Projected COD
"""


merged_df.to_csv(output_file, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
