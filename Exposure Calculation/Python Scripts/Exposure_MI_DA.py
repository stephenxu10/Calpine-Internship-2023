import requests
import pandas as pd
from io import StringIO
import zipfile
import logging
import numpy as np
import time
import os
from requests.exceptions import HTTPError
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


# Global Variables and Parameters.
start_time = time.time()

CREDENTIALS_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"
OUTPUT_ROOT = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Exposure Calculation/Outputs"
YEAR = 2022
MONTH = 2
THRESHOLD = 0.1
BASE_URL = "https://services.yesenergy.com/PS/rest/ftr/awards/netinventory.csv"
mis_path = "//Pzpwuplancli01/Uplan/ERCOT"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
with open(CREDENTIALS_PATH, "r") as cred_file:
    AUTH_CREDENTIALS = tuple(cred_file.read().split(" "))

def get_data_from_api(url: str, params=None):
    """
    Makes a GET request to the specified URL and return the data as a DataFrame.

    Parameters
    ----------
    url : str
        Base URL to query to.
    params : dict, optional
        Additional parameters to the query URL. The default is None.

    Returns
    -------
    DataFrame
        A DataFrame containing the queried data. Any invalid and/or unsuccessful
        queries are logged to the console, and an empty DataFrame is returned
        as a result.
    """
    try:
        response = requests.get(url, auth=AUTH_CREDENTIALS, params=params)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text))
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.error(f'Other error occurred: {err}')
    return pd.DataFrame()

def grab_net_inventory(year: int, month: int) -> pd.DataFrame:
    """
    Grabs Raw NetInventory data for a certain year and month combination
    """
    query_date = datetime(year, month, 1)
    next_month_date = (query_date + timedelta(days=31)).strftime("%m/%d/%Y")
    
    params = {
            'iso': 'ERCOT',
            'startdate': query_date.strftime("%m/%d/%Y"),
            'enddate': next_month_date,
            'columns': '2,3,5,7,8,9,13,31,32'
        }
    
    return get_data_from_api(BASE_URL, params)

def process_zip_file(zip_path: str, limit: float) -> pd.DataFrame:
    """
    Process a single zip file to extract the DataFrame according to specified logic.
    """
    drop_columns = ["ConstraintID", "Limit", "DSTFlag", "DeliveryDate", "HourEnding"]
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
        with z.open(csv_file) as csv_f:
            df = pd.read_csv(csv_f)
            df.drop(columns=[col for col in drop_columns if col in df.columns], errors='ignore', inplace=True)
            df = df[abs(df['ShiftFactor']) > limit]
            df.fillna(0)

            df.loc[(df['FromStationKV']==df['ToStationKV']) & ((df['FromStationKV'] == 0)), 'ReportedName'] = df['ConstraintName']
            df.loc[(df['FromStationKV']==df['ToStationKV']) & (df['FromStationKV'] != 0), 'ReportedName'] = df['FromStation'].astype(str)+'-'+df['ToStation'].astype(str)+' '+df['FromStationKV'].astype(str)+'KV '+df['ConstraintName']
            df.loc[(df['FromStationKV']!=df['ToStationKV']) & (df['FromStationKV'] != 0), 'ReportedName'] = df['FromStation'].astype(str)+' '+df['FromStationKV'].astype(str)+'KV '+df['ConstraintName']

            condition = (df['FromStationKV'] != df['ToStationKV']) & (df['FromStationKV'] != 0)
            df['ReportedName'] = np.where(
                condition,
                np.where(
                    pd.notna(df['FromStation']) & pd.notna(df['FromStationKV']),
                    df['FromStation'].astype(str) + ' ' + df['FromStationKV'].astype(str) + 'KV ' + df['ConstraintName'],
                    df['ConstraintName']
                ),
                df['ReportedName']  # Keeps the existing value if the condition is False
            )
            
            df.drop(columns=['FromStation', 'FromStationKV', 'ToStation', 'ToStationKV'], inplace=True)
            return df

def aggregate_network_files(year: int, month: int, limit: float) -> pd.DataFrame:
    yearly_base = os.path.join(mis_path, f"MIS {year}/55_DSF")
    formatted_month = f"{month:02d}"
    if os.path.exists(yearly_base):
        yearly_zip_files = [os.path.join(yearly_base, file) for file in os.listdir(yearly_base) if file.endswith('.zip') and str(year) + formatted_month in file]
        
        # Use ThreadPoolExecutor to process files in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
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
                return aggregated_data
            else:
                return pd.DataFrame()
            
def calculate(year, month):
    inventory_df = grab_net_inventory(year, month)
    inventory_df['SETTLEMENTMONTH'] = pd.to_datetime(inventory_df['SETTLEMENTMONTH'])
    inventory_df['Path'] = inventory_df['SOURCENAME'] + "+" + inventory_df['SINKNAME']
    inventory_df = inventory_df[inventory_df['SETTLEMENTMONTH'].dt.month == month]

    shift_df = aggregate_network_files(YEAR, MONTH, THRESHOLD)

    grouped_shift_df = shift_df.groupby(['ReportedName', 'ContingencyName', 'SettlementPoint'])['ShiftFactor'].mean().reset_index()
    src_netinv_df = pd.merge(inventory_df, grouped_shift_df, left_on='SOURCENAME', right_on='SettlementPoint')
    src_netinv_df.drop(columns=['SettlementPoint'], inplace=True)
    src_netinv_df.rename(columns={'ShiftFactor': 'Source SF'}, inplace=True)

    sink_netinv_df = pd.merge(inventory_df, grouped_shift_df, left_on='SINKNAME', right_on='SettlementPoint')
    sink_netinv_df.drop(columns=['SettlementPoint'], inplace=True)
    sink_netinv_df.rename(columns={'ShiftFactor': 'Sink SF'}, inplace=True)

    merge_keys = ['PARTICIPANTSHORTNAME', 'PEAKTYPE', 'SETTLEMENTMONTH', 'FTRPARTICIPANT', 'Path', 
                'SOURCENAME', 'SINKNAME', 'HOLD_REVENUE_PMWH', 'HOLD_COST_PMWH', 'HELD_MWS', 
                'ContingencyName', 'ReportedName']

    final_df = pd.merge(src_netinv_df, sink_netinv_df, on=merge_keys, how='inner')
    final_df = final_df.drop_duplicates()

    # Do the Exposure calculations on the final merged DataFrame
    final_df['Exposure'] = final_df['Source SF'] - final_df['Sink SF']
    final_df['Exposure_MW'] = final_df['Exposure'] * final_df['HELD_MWS']

    print(f"Outputting to CSV for {year} month {month}.")
    output_file_name = f"CRR Exposure_NI_{month:02d}_{year}.csv"
    final_df.to_csv(os.path.join(OUTPUT_ROOT, output_file_name), index=False)


for year in range(2024, 2025):
    for month in range(1, 13):
        calculate(year, month)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")