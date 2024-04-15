import pandas as pd
import os
import glob
import time
import re

PATH_BASE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/Semi-Annual"
OUTPUT_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Non-Thermal Constraint Aggregates"

# No data appears to be available prior to 2018.
MIN_YEAR = 2018
MAX_YEAR = 2050

relevant_folders = ["A-S6", "B-S5", "C-S4", "D-S3", "E-S2", "F-S1"]
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
start_time = time.time()

def aggregate_year(year: int, half: int) -> pd.DataFrame:
    if half == 1:
        yearly_base = os.path.join(PATH_BASE, f"{year}-1H/Auction")
    else:
        yearly_base = os.path.join(PATH_BASE, f"{year}-2H/Auction")

    if os.path.isdir(yearly_base):
        dataframes = []
        for folder in relevant_folders:
            folder_base = os.path.join(yearly_base, f"{folder}/Network Model")

            if os.path.isdir(folder_base):
                pattern = os.path.join(folder_base, "*ThermalConstraints*.csv")
                csv_files = glob.glob(pattern)

                if csv_files:
                    for file in csv_files:
                        filename = os.path.basename(file)
                        seq_index = filename.find("Seq")
                        
                        parts = filename.split("_")
                        month = parts[-2]
                        year = parts[-1][:-4]

                        df = pd.read_csv(file)
                        df.columns = [col.strip() for col in df.columns]
                        df['Year'] = year
                        df['Month'] = months.index(month) + 1
                        df['Filename'] = filename[seq_index: seq_index + 4]
                
                        dataframes.append(df)
        
        if len(dataframes) == 0:
            return pd.DataFrame()
        else:
            return pd.concat(dataframes, axis=0)

    else:
        return pd.DataFrame()
    

yearly_dfs = []
for year in range(MIN_YEAR, MAX_YEAR):
    for half in range(1, 3):
        yearly_dfs.append(aggregate_year(year, half))

aggregate_df = pd.concat(yearly_dfs)
aggregate_df = aggregate_df[aggregate_df.columns.tolist()[6:] + aggregate_df.columns.tolist()[:6]]
aggregate_df['DeviceType'] = aggregate_df['DeviceType'].str.upper()

pivot_df = pd.pivot_table(aggregate_df, values='Limit', index=['Year', 'Month', 'Name', 'DeviceName', 'DeviceType', 'FlowDirection', 'Factor'], columns=['Filename'], aggfunc="sum")
pivot_df.to_csv(OUTPUT_PATH + "/CRR_NonThermalConstraints_SemiAnnual.csv")

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
