from io import StringIO
import zipfile
import json
import requests
from datetime import datetime
import numpy as np
import warnings
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import time
from datetime import timedelta, datetime
import os

warnings.simplefilter("ignore")
convert_date_format = lambda input_date_str: datetime.strptime(input_date_str.strftime('%Y-%m-%d'), "%Y-%m-%d").strftime("%m/%d/%Y")

# Global Variables and Parameters.
start_time = time.time()
year = 2023

# How many days we look back
days_back = 30

zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\130_SSPSF"
json_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/current_" + str(year) + "_web_data.json"
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/Monthly_RT_SF_" + str(year) + ".csv"

with open(json_path, "r") as file:
    web_data = json.load(file)


def compute_avg_SP(mapping: Dict, date: str, const: str, contin: str):
    """
    Helper method to find the average shadow price on a certain date given an input constraint
    name and contingency. Queries an input mapping.

    Inputs:
        mapping: A dictionary of the some format as the pre-processed web data JSON.
        date: A date in the format MM/DD/YYYY
        const: A string representing the constraint name. Abbreviated version taken from the network drive.
        contin: A string representing the contingency.
    
    Output: A float giving the average shadow price in these bounds.
    """
    sp_sum = 0
    count = 0
    full_name = ""

    date_data = mapping[date]

    for hour in date_data:
        for full_constraint_name in date_data[hour]:
            if const in full_constraint_name:
                full_name = full_constraint_name
                for data_list in date_data[hour][full_constraint_name]:
                    if data_list[0] == contin and not np.isnan(data_list[1]):
                        sp_sum += data_list[1]
                        count += 1
    
    if count == 0:
        return -1, full_name
    
    else:
        return sp_sum / count, full_name


yearly_zip_files = os.listdir(zip_base)

merge = []
today = datetime.now()
for zip_file in yearly_zip_files:
    zip_date = datetime.strptime(zip_file[34:36] + "/" + zip_file[36:38] + "/" + str(year), "%m/%d/%Y")

    # Convert all newly added CSVs since the last aggregation
    if zip_date >= today - timedelta(days=days_back):
        with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
            df = pd.read_csv(zip_path.open(zip_path.namelist()[0]))
            grouped_df = df.groupby(['Constraint_Name', 'Settlement_Point', 'Contingency_Name']).mean()['Shift_Factor'].reset_index()
            grouped_df.insert(0, 'Date', zip_date)
            merge.append(grouped_df)

df_merged = pd.concat(merge, axis=0)
df_merged.rename(columns={'Shift_Factor': 'Average_SF'}, inplace=True)
df_merged = df_merged.groupby(['Date', 'Constraint_Name', 'Contingency_Name'])
df_merged = pd.concat([group for _, group in df_merged], ignore_index=True)

print("CP 1")

average_sp = []
full_cons_list = []
prev_row = None
for index, row in df_merged.iterrows():
    curr_date = row['Date']
    curr_cons = row['Constraint_Name']
    curr_contin = row['Contingency_Name']
    if index > 0 and (prev_row['Constraint_Name'] == curr_cons and prev_row['Contingency_Name'] == curr_contin):
        average_sp.append(average_sp[-1])
        full_cons_list.append(full_cons_list[-1])
    else:
        avg, full_cons = compute_avg_SP(web_data, convert_date_format(curr_date), curr_cons, curr_contin)
        average_sp.append(avg)
        full_cons_list.append(full_cons)

    prev_row = row

df_merged['Constraint_Name'] = full_cons_list
df_merged['Average_SP'] = average_sp

df_merged.to_csv(output_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
