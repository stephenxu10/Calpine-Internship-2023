import pandas as pd
import os
from datetime import datetime, timedelta
import time
from typing import *
import warnings

"""
This Python script aims to compare Generator Projects from ERCOT's GIS Reports
for the last two months. In particular, the project finds the difference between
the projected COD (Commericial Operations Date) between corresponding 
projects and outputs that data to a CSV.
"""
warnings.simplefilter("ignore")

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

def date_difference_string(td):
    days_difference = td.days
    if days_difference > 0:
        return f"Delayed {days_difference} days"
    elif days_difference < 0:
        return f"Sooner by {abs(days_difference)} days"
    else:
        return "No change in dates"

def get_last_two(input_path: str) -> Tuple[str, str]:
    """
    Quick helper method to grab the two most recently modified files
    from an input path base.

    Returns a tuple of the full paths to the most and second most recently
    modified files in the input path.
    """
    names = filter(lambda x: not x.startswith("~"), os.listdir(input_path))
    names = [f for f in names]
    return names[-2], names[-1]

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

    # Iterate until the 'Project Attributes' header is encountered. This signifies the beginning of the DataFrame.
    for _, row in df_large_gen.iterrows():
        additional_rows += 1
        if row[first_col] == "Project Attributes":
            break
    
    return 6 + additional_rows

if __name__ == "__main__":   
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
    df_changed = merged_df[merged_df['Projected COD'] != merged_df['Current Projected COD']]
    df_unique_last = merged_df[merged_df['Project Name_curr'].isna()]
    df_unique_curr = merged_df[merged_df['GIS Report Name'].isna()]
    df_same = merged_df[merged_df['Projected COD'] == merged_df['Current Projected COD']]

    df_changed['Change in Projected COD'] = pd.to_datetime(df_changed['Current Projected COD']) - pd.to_datetime(df_changed['Projected COD'])
    df_changed['Change in Projected COD'] = df_changed['Change in Projected COD'].map(date_difference_string)
    df_changed = df_changed.drop(columns=columns_to_drop).dropna(subset=['Projected COD', 'Current Projected COD'])

    df_unique_last['Change in Projected COD'] = "Only in previous month"
    df_unique_last = df_unique_last.drop(columns=columns_to_drop)

    df_unique_curr['Change in Projected COD'] = "New in the current month"
    df_unique_curr['GIS Report Name'] = curr_month[curr_month.index('GIS_Report'):-5]
    df_unique_curr = df_unique_curr.dropna(axis=1)

    for col in df_unique_curr.columns:
        if "_curr" in col:
            df_unique_curr.rename(columns={col: col[:-5]}, inplace=True)


    df_same['Change in Projected COD'] = "No change"
    df_same = df_same.drop(columns=columns_to_drop)


    # Merge together each DataFrame in the desired order and output to CSV
    final_df = pd.concat([df_changed, df_unique_curr, df_unique_last, df_same], axis=0)
    final_df.to_csv(output_file, index=False)

    # Output summary statistics
    end_time = time.time()
    execution_time = (end_time - start_time)
    print("Generation Complete")
    print(f"The script took {execution_time:.2f} seconds to run.")
