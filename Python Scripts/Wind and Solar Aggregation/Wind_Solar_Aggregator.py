import pandas as pd
from datetime import datetime
import os
import time
import concurrent.futures
import threading
import warnings
import requests
from io import StringIO

warnings.simplefilter("ignore")

"""
This Python scripts aims to perform some basic aggregation and analysis on ERCOT historical short-term photovoltaic
power forecasts (STPPF). 

Produces three new columns - Solar RT Estimated Capacity Factor, Solar RT Estimated Curtailment Factor,
and Load Solar Differential. Combines all modified DataFrames into a CSV and outputs to a new file.
"""
# Version 1 aggregates Solar Data, Version 2 aggregates Wind Data.
version = 2

file_key = "SolarPowerForecast" if version == 1 else "WindPowerForecast"
output_key = "RT Aggr Solar-Output (MW)" if version == 1 else "RT Aggr Wind-Output (MW)"
date_key = "Operating Day" if version == 1 else "Date"

# Global parameters & variables
start_time = time.time()
solar_base = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/04 - Monthly Updates/101 - Misc/01 - General/Wind Forecast Monthly"
output_path = (
    "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Wind and Solar Aggregates/ERCOT_Solar_Curtailment_WMWG.csv"
    if version == 1
    else "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Wind and Solar Aggregates/ERCOT_Wind_Curtailment_WMWG.csv"
)
credential_path = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"

overall_solar = ['Operating Day', 'System-Wide Capacity', 'CenterEast Capacity', 'FarEast Capacity', 'FarWest Capacity', 'NorthWest Capacity', 'SouthEast Capacity']

nodes = ["HB_NORTH", "HB_WEST", "HB_SOUTH", "HB_HOUSTON"]
node_url_string = ",".join(nodes)

start_year = 2019
end_year = 2023

lock = threading.Lock()
overall_dfs = []

with open(credential_path, "r") as cred:
    auth = tuple(cred.read().split(" "))

def analyze_sheet(sheet_name: str) -> pd.DataFrame:
    """
    Helper method that extracts and analyzes all relevant data from an Excel sheet name as explained
    in the block comment at the beginning of the script. Outputs a DataFrame containing the married contents
    between the HA System-Wide STPPF and Resource to Region pages.

    Inputs:
        - sheet_name: The name of the sheet to be analyzed (not the path).

    Output:
        - A DataFrame containing the desired information.
    """
    full_path = os.path.join(solar_base, sheet_name)

    # Progress Checking
    print(sheet_name)

    if version == 1:
        cols = "A:H"
        HA_sheet = "HA System-Wide STPPF"

    else:
        cols = "A:G"
        HA_sheet = "HA System-Wide STWPF"

    # Read the resource to region sheet
    resource = pd.read_excel(
        full_path, sheet_name="Resource to Region", skiprows=11, nrows=31, usecols=cols
    )
    resource = resource[pd.notna(resource[date_key])]

    if version == 1:
        resource = resource.reindex(columns=overall_solar)

    # Read the Hour Ahead (HA) sheet
    forecast = pd.read_excel(
        full_path, sheet_name=HA_sheet, skiprows=9, usecols="A:L"
    )

    # Merge together the two DataFrames and rearrange the columns
    merged_df = pd.merge(forecast, resource, right_on=date_key, left_on='Operating Day', how="inner")
    merged_df = merged_df.filter(regex='^(?!Unnamed)')

    merged_df = merged_df.rename(
        columns={date_key: "MARKETDAY", "Operating Hour": "HOURENDING"}
    )
    # Create estimated capacity and curtailment factor columns
    merged_df["RT Est. Cap Factor"] = (
        merged_df[output_key] / merged_df["System-Wide Capacity"]
    )
    merged_df["RT Est. Curt Factor"] = (
        merged_df["RT Est. Curtailments"] / merged_df["System-Wide Capacity"]
    )
    if version == 1:
        merged_df["Load-Solar Difference"] = (
            merged_df["Ercot Load (MW)"] - merged_df[output_key]
        )
    
    else:
        merged_df["Load-Wind Difference"] = (
            merged_df["Ercot Load (MW)"] - merged_df[output_key]
        )
        market_dates = merged_df.pop('MARKETDAY')
        merged_df.insert(0, 'MARKETDAY', market_dates)

    return merged_df


# Function to process each file
def process_file(file_name):
    if not file_name.startswith("~"):
        if file_key in file_name:
            df = analyze_sheet(file_name)

            # Lock the access to overall_dfs to ensure thread-safe addition
            with lock:
                overall_dfs.append(df)


historical_solar = os.listdir(solar_base)

# Use ThreadPoolExecutor to parallelize the file processing procedure
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(process_file, historical_solar)

merged = pd.concat(overall_dfs, axis=0)
merged["MARKETDAY"] = merged["MARKETDAY"].apply(lambda x: pd.to_datetime(x))
merged = merged.sort_values(by=["MARKETDAY", "HOURENDING"])

"""
Do some queries to Yes Energy to grab RT and DA local marginal prices for a certain set of Hubs. Query
one year at a time so that an overly large request is not throttled.
"""
id_url = (
    "https://services.yesenergy.com/PS/rest/timeseries/RTLMP.csv?ISO=ERCOT&Name="
    + node_url_string
    + ""
)
id_req = requests.get(id_url, auth=auth)
df_ids = pd.read_csv(StringIO(id_req.text))

# Build the next URL string
agg_rt_ids = ["RTLMP:" + str(i) for i in df_ids.OBJECTID]
url_string_rt = ",".join(agg_rt_ids)

agg_da_ids = ["DALMP:" + str(i) for i in df_ids.OBJECTID]
url_string_da = ",".join(agg_da_ids)

# Combine the two aggregated ID strings
full_items = f"{url_string_da},{url_string_rt}"

queried_dfs = []
for year in range(start_year, end_year + 1):
    start_date = f"01/01/{year}"
    end_date = f"01/01/{year + 1}"
    lmp_url = f"https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate={start_date}&enddate={end_date}&items={full_items}"
    print(lmp_url)

    yearly_lmp = pd.read_csv(StringIO(requests.get(lmp_url, auth=auth).text))
    yearly_lmp = yearly_lmp.drop(columns=["DATETIME", "MONTH", "YEAR"])

    queried_dfs.append(yearly_lmp)

overall_query = pd.concat(queried_dfs, axis=0)

# Convert 'HOURENDING' column in merged to int64
merged["HOURENDING"] = merged["HOURENDING"].astype(int)

# Convert 'MARKETDAY' column in merged to datetime64[ns]
merged["MARKETDAY"] = pd.to_datetime(merged["MARKETDAY"])

# Convert 'MARKETDAY' and 'HOURENDING' columns in overall_query to datetime64[ns] and int64, respectively
overall_query["MARKETDAY"] = pd.to_datetime(overall_query["MARKETDAY"])
overall_query["HOURENDING"] = overall_query["HOURENDING"].astype(int)

merged = pd.merge(
    merged,
    overall_query,
    how="inner",
    left_on=["MARKETDAY", "HOURENDING"],
    right_on=["MARKETDAY", "HOURENDING"],
)

if version == 2:
    merged = merged.drop(columns=['Operating Day'])

merged.to_csv(output_path, index=False)

end_time = time.time()
execution_time = end_time - start_time
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
