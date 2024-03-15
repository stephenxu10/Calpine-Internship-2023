import zipfile
from io import StringIO, BytesIO
import json
from itertools import product
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import time
from datetime import date
from datetime import timedelta, datetime
import os

"""
This Python task aims to summarize the day-ahead market data pulled from the Network. Furthermore,
the task uses the summarized data to create a historical Delta table across an entire calendar year 
for a specific subset of paths. The end result will closely resemble the output of the Real-Time Delta
Table.

Currently, this script does not generate an aggregated CSV like in ./../Real-Time Constraint Aggregation. Rather,
it generates the summary JSON like before. The overall workflow is as follows:
    1) Query Yes Energy to extract the set of Settlement Points that we are interested in.
    2) Query Yes Energy again to create/update the pre-processed yearly DA web data.
    3) Convert and aggregate all unread data from ERCOT API for the last 14 days using the pre-processed mapping.
    4) Post-process the aggregated data into a summary JSON.
    5) Use this summarized JSON to generate the Delta Table and output it to a CSV.

Current output path: \\pzpwtabapp01\Ercot
"""
warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = date.today().year

delta_path = f"\\\\pzpwtabapp01\\Ercot\\Exposure_DAM_Last_3_Years.csv"
credential_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"
mis_path = "//Pzpwuplancli01/Uplan/ERCOT"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/DA/ERCOT?"

# Adjust this to change how many days of historical data we want.
days_back = 730

with open(credential_path, "r") as credentials:
    auth = tuple(credentials.read().split())

call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?"
r = requests.get(call1, auth=auth)
df = pd.read_csv(StringIO(r.text))
unique_nodes = pd.concat([df["SINK"], df['SOURCE']]).unique()

def process_zip_file(zip_path: str, limit: float) -> pd.DataFrame:
    """
    Process a single zip file to extract the DataFrame according to specified logic.
    """
    drop_columns = ["ConstraintID", "FromStation", "FromStationKV", "ToStation", "ToStationKV", "Limit", "DSTFlag"]
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
        with z.open(csv_file) as csv_f:
            df = pd.read_csv(csv_f)
            df = df[df['SettlementPoint'].isin(unique_nodes)]
            df.drop(columns=[col for col in drop_columns if col in df.columns], errors='ignore', inplace=True)
            df = df[abs(df['ShiftFactor']) > limit]
            df['HourEnding'] = df['HourEnding'].str.extract('(\d+):')[0].astype(int)
            df['DeliveryDate'] = pd.to_datetime(df['DeliveryDate'])
            mask = df['HourEnding'] == 24
            df.loc[mask, 'DeliveryDate'] += pd.Timedelta(days=1)
            
            print(zip_path)
            return df

def aggregate_network_files(year: int, limit: float) -> pd.DataFrame:
    yearly_base = os.path.join(mis_path, f"MIS {year}/55_DSF")
    if os.path.exists(yearly_base):
        yearly_zip_files = [os.path.join(yearly_base, file) for file in os.listdir(yearly_base) if file.endswith('.zip')]
        
        # Use ThreadPoolExecutor to process files in parallel
        with ThreadPoolExecutor(max_workers=6) as executor:  # Adjust max_workers based on your environment
            future_to_zip = {executor.submit(process_zip_file, zip_file, limit): zip_file for zip_file in yearly_zip_files}
            
            results = []
            for future in as_completed(future_to_zip):
                zip_file = future_to_zip[future]
                try:
                    data = future.result()
                    results.append(data)
                except Exception as exc:
                    print(f'{zip_file} generated an exception: {exc}')
                    
            if results:
                aggregated_data = pd.concat(results, ignore_index=True)
                aggregated_data.sort_values(by=['DeliveryDate', 'HourEnding'], inplace=True)
                print(f"Finished {year}")
                return aggregated_data
            else:
                return pd.DataFrame()

def grab_yes_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    A helper method that queries Yes Energy to grab the ERCOT hourly constraint data in a certain date range.
    """
    relevant_columns = ["REPORTED_NAME", "DATETIME", "HOURENDING", "CONTINGENCY"]
    all_years_data = pd.DataFrame()
    current_start_date = datetime.strptime(start_date, "%m/%d/%Y")
    end_date_obj = datetime.strptime(end_date, "%m/%d/%Y")

    while current_start_date <= end_date_obj:
        one_year_later = current_start_date + timedelta(days=250)
        current_end_date = min(one_year_later, end_date_obj)

        formatted_start_date = current_start_date.strftime("%m/%d/%Y")
        formatted_end_date = current_end_date.strftime("%m/%d/%Y")

        req_url = f"{yes_energy}startdate={formatted_start_date}&enddate={formatted_end_date}"
        yes_req = requests.get(req_url, auth=auth)
        print(req_url)  

        if yes_req.ok and "No data" not in yes_req.text:
            current_yes_table = pd.read_html(StringIO(yes_req.text))[0]
            all_years_data = pd.concat([all_years_data, current_yes_table])
            all_years_data.info()
        else:
            print(f"Failed to retrieve data for {formatted_start_date} to {formatted_end_date}")
        
        current_start_date = current_end_date + timedelta(days=1)
        
    return all_years_data[relevant_columns]
    
     
lower_bound = (date.today() - timedelta(days=days_back)).strftime('%m/%d/%Y')
today = (date.today() + timedelta(days=1)).strftime('%m/%d/%Y')

#%%
yes_df = grab_yes_data(lower_bound, today)

#%%
network_df = pd.concat([aggregate_network_files(year, 0.001) for year in range(2022, 2025)])
                        
#%%
yes_df['DATETIME'] = pd.to_datetime(yes_df['DATETIME'])
network_df['DeliveryDate'] = pd.to_datetime(network_df['DeliveryDate'])
network_df = network_df[network_df['DeliveryDate'] > lower_bound]

def match_reported_name(row, yes_df):
    constraint_name = row['ConstraintName']
    delivery_date = row['DeliveryDate']
    hour_ending = row['HourEnding']
    contingency_name = row['ContingencyName']
    
    filtered_yes_df = yes_df[(yes_df['DATETIME'].dt.date == delivery_date.date()) &
                             (yes_df['HOURENDING'] == hour_ending) &
                             (yes_df['CONTINGENCY'] == contingency_name)]
    
    for _, yes_row in filtered_yes_df.iterrows():
        if constraint_name in yes_row['REPORTED_NAME']:
            return yes_row['REPORTED_NAME']
    
    return None

network_df['Constraint'] = network_df.apply(match_reported_name, args=(yes_df,), axis=1)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

