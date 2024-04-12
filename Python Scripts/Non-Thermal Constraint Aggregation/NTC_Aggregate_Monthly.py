import pandas as pd
import os
import glob
import time

PATH_BASE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/Monthly"
OUTPUT_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Non-Thermal Constraint Aggregates"

# No data appears to be available prior to 2018.
MIN_YEAR = 2018
MAX_YEAR = 2050

start_time = time.time()

def convert(month: int) -> str:
    """
    Simple helper method that appends a leading zero to an integer if it is single-digit and casts it to
    a string.

    Inputs:
    - month: An integer in the interval [1, 12].
    """
    if month <= 9:
        return "0" + str(month)
    else: 
        return str(month)

def aggregate_year(year: int) -> pd.DataFrame:
    if year == 2019:
        yearly_base = os.path.join(PATH_BASE, "2019/999-Month")
    else:
        yearly_base = os.path.join(PATH_BASE, str(year))

    if os.path.isdir(yearly_base):
        dataframes = []
        for month in range(1, 13):
            monthly_base = os.path.join(yearly_base, f"{year}-{convert(month)}/Network Model")

            if os.path.isdir(monthly_base):
                pattern = os.path.join(monthly_base, "*Non-ThermalConstraints*.csv")
                csv_files = glob.glob(pattern)

                if csv_files:
                    df = pd.read_csv(csv_files[0])
                    df.columns = [col.strip() for col in df.columns]
                    
                    df['Year'] = year
                    df['Month'] = month

                    dataframes.append(df)
        
        return pd.concat(dataframes, axis=0)

    else:
        return pd.DataFrame()
    

yearly_dfs = []
for year in range(MIN_YEAR, MAX_YEAR):
    yearly_dfs.append(aggregate_year(year))

aggregate_df = pd.concat(yearly_dfs)
aggregate_df = aggregate_df[aggregate_df.columns.tolist()[6:] + aggregate_df.columns.tolist()[:6]]
aggregate_df.to_csv(OUTPUT_PATH + "/CRR_NonThermalConstraints_Monthly.csv", index=False)
    
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
