import zipfile
from io import StringIO, BytesIO
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import concurrent.futures
import time
import os
from datetime import date
from datetime import timedelta, datetime

warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = date.today().year
pd.set_option('display.max_columns', 500)

DAYS_BACK = 30
LIMIT = 0.005
mis_path = "//Pzpwuplancli01/Uplan/ERCOT"
delta_path = f"\\\\pzpwtabapp01\\Ercot\\Exposure_SCED_Last_{DAYS_BACK}_Days.csv"
credential_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/RT/ERCOT?"

with open(credential_path, "r") as credentials:
    auth = tuple(credentials.read().split())

call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?"
r = requests.get(call1, auth=auth)
paths_df = pd.read_csv(StringIO(r.text))
unique_nodes = pd.concat([paths_df["SINK"],paths_df['SOURCE']]).unique()
paths_df["PATH"] = paths_df['SOURCE'].astype(str) + "+" + paths_df['SINK']

def grab_yes_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    A helper method that queries Yes Energy to grab the ERCOT hourly constraint data in a certain date range.
    """
    relevant_columns = ["REPORTED_NAME", "DATETIME", "HOURENDING", "CONTINGENCY", "SHADOWPRICE"]
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
        else:
            print(f"Failed to retrieve data for {formatted_start_date} to {formatted_end_date}")
        
        current_start_date = current_end_date + timedelta(days=1)
    
    all_years_data['DATETIME'] = pd.to_datetime(all_years_data["DATETIME"]).dt.strftime("%m/%d/%Y")
    return all_years_data[relevant_columns]


def grab_ercotapi_data(l_d: str, l_h: str, u_d: str, u_h: str) -> pd.DataFrame:
    """Queries a certain range of data through ERCOT API in chunks and aggregates the results into one DataFrame."""
    
    def split_time_range(start_date: str, start_hour: str, end_date: str, end_hour: str, chunks: int=3):
        """Split the time range into smaller chunks."""
        start_datetime = datetime.strptime(f"{start_date} {start_hour}", "%Y-%m-%d %H")
        end_datetime = datetime.strptime(f"{end_date} {end_hour}", "%Y-%m-%d %H")
        
        # Calculate total duration and split into chunks
        total_duration = end_datetime - start_datetime
        chunk_duration = total_duration / chunks
        
        time_ranges = []
        for i in range(chunks):
            chunk_start = start_datetime + i * chunk_duration
            chunk_end = chunk_start + chunk_duration
            if i == chunks - 1:
                # Ensure the last chunk ends exactly at the end_datetime
                chunk_end = end_datetime
            time_ranges.append((chunk_start, chunk_end))
        
        return time_ranges

    def query_ercot_data(start_datetime, end_datetime):
        """Perform a query to the ERCOT API for the given datetime range."""
        merged = []
        file_type = "csv"
        ercot_url = f"https://ercotapi.app.calpine.com/reports?reportId=16013&marketParticipantId=CRRAH&startTime={start_datetime.strftime('%Y-%m-%dT%H:00:00')}&endTime={end_datetime.strftime('%Y-%m-%dT%H:00:00')}&unzipFiles=false"
        
        print(ercot_url) 
        
        response = requests.get(ercot_url, verify=False)  
        if response.status_code == 200:
            zip_data = zipfile.ZipFile(BytesIO(response.content))
            with zip_data as z:
                for file_name in z.namelist():
                    if file_type in file_name:
                        inner_data = BytesIO(z.read(file_name))
                        with zipfile.ZipFile(inner_data, 'r') as inner_zip:
                            with inner_zip.open(inner_zip.namelist()[0]) as inner_csv:
                                drop_columns = ["Constraint_ID", "Repeated_Hour_Flag"]
                                df_chunk = pd.read_csv(inner_csv)
                                
                                df_chunk = df_chunk.drop(columns=drop_columns)
                                df_chunk = df_chunk[df_chunk['Settlement_Point'].isin(unique_nodes)]
                                df_chunk = df_chunk[abs(df_chunk['Shift_Factor']) > LIMIT]

                                # Add the HourEnding column
                                if len(df_chunk) > 0:
                                    datetime_obj = datetime.strptime(df_chunk.iloc[0, 0], "%m/%d/%Y %H:%M:%S")
                                    next_hour = (datetime_obj + timedelta(hours=1)).strftime("%H")
    
                                    if next_hour == "00":
                                        next_hour = "24"
    
                                    next_hour = next_hour.lstrip('0')
                                    hourEnding = [next_hour] * len(df_chunk)
                                    df_chunk.insert(1, "Hour_Ending", hourEnding)
                                    merged.append(df_chunk)
        else:
            print(response.status_code)
            return pd.DataFrame()
        
        print("Success!")
        return pd.concat(merged, axis=0)
    
    time_ranges = split_time_range(l_d, l_h, u_d, u_h)
    all_data_futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Schedule the fetch_data_range function to be called for each time range
        for start_dt, end_dt in time_ranges:
            future = executor.submit(query_ercot_data, start_dt, end_dt)
            all_data_futures.append(future)
        
        # Wait for all futures to complete and collect their results
        all_data = [future.result() for future in concurrent.futures.as_completed(all_data_futures)]
    
    # Concatenate all DataFrames into a single DataFrame
    return pd.concat(all_data, axis=0)

def process_zip_file(zip_path: str, limit: float) -> pd.DataFrame:
    """
    Process a single zip file to extract the DataFrame according to specified logic.
    """
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = [f for f in z.namelist() if f.endswith('.csv')][0]
        with z.open(csv_file) as csv_f:
            drop_columns = ["Constraint_ID", "Repeated_Hour_Flag"]
            df_chunk = pd.read_csv(csv_f)
            
            df_chunk = df_chunk.drop(columns=drop_columns)
            df_chunk = df_chunk[df_chunk['Settlement_Point'].isin(unique_nodes)]
            df_chunk = df_chunk[abs(df_chunk['Shift_Factor']) > LIMIT]

            # Add the HourEnding column
            if len(df_chunk) > 0:
                datetime_obj = datetime.strptime(df_chunk.iloc[0, 0], "%m/%d/%Y %H:%M:%S")
                next_hour = (datetime_obj + timedelta(hours=1)).strftime("%H")

                if next_hour == "00":
                    next_hour = "24"

                next_hour = next_hour.lstrip('0')
                hourEnding = [next_hour] * len(df_chunk)
                df_chunk.insert(1, "Hour_Ending", hourEnding)
            
            return df_chunk
        
    return pd.DataFrame()

def aggregate_network_files(year: int, limit: float) -> pd.DataFrame:
    yearly_base = os.path.join(mis_path, f"MIS {year}/130_SSPSF")
    if os.path.exists(yearly_base):
        yearly_zip_files = [os.path.join(yearly_base, file) for file in os.listdir(yearly_base) if file.endswith('.zip')]
        
        # Use ThreadPoolExecutor to process files in parallel
        with ThreadPoolExecutor() as executor:  # Adjust max_workers based on your environment
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
                aggregated_data.sort_values(by=['SCED_Time_Stamp', 'Hour_Ending'], inplace=True)
                print(f"Finished {year}")
                return aggregated_data
            else:
                return pd.DataFrame()

def merge_paths_ercot(paths_df, ercot_df) -> pd.DataFrame:
    # Assuming SCED_Time_Stamp is already a datetime in ercot_df; if not, convert it upfront
    ercot_df['SCED_Time_Stamp'] = pd.to_datetime(ercot_df['SCED_Time_Stamp'])
    
    # Deduplicate
    paths_df = paths_df[['SOURCE', 'SINK']].drop_duplicates()
    ercot_df = ercot_df.drop_duplicates(subset=['Settlement_Point', 'SCED_Time_Stamp', 'Shift_Factor', 'Constraint_Name', 'Contingency_Name'])
    merged_df = pd.merge(paths_df, ercot_df, left_on='SOURCE', right_on='Settlement_Point', how='inner', suffixes=('', '_source'))
    merged_df = pd.merge(merged_df, ercot_df, left_on=['SINK', 'SCED_Time_Stamp', 'Contingency_Name'], right_on=['Settlement_Point', 'SCED_Time_Stamp', 'Contingency_Name'], how='inner', suffixes=('', '_sink'))

    # Clean up and select the needed columns only once, also handling duplicates and NaNs efficiently
    merged_df = merged_df.dropna(subset=['Hour_Ending'])
    merged_df = merged_df.drop_duplicates().sort_values(by=['SCED_Time_Stamp', 'Hour_Ending'])

    # Format SCED_Time_Stamp for final output, if necessary
    merged_df['DATETIME'] = merged_df['SCED_Time_Stamp'].dt.strftime("%m/%d/%Y")

    final_columns = ['SOURCE', 'SINK', 'SCED_Time_Stamp', 'DATETIME', 'Shift_Factor', 'Shift_Factor_sink', 'Constraint_Name', 'Contingency_Name', 'Hour_Ending']
    final_df = merged_df[final_columns].rename(columns={'Shift_Factor': 'SOURCE_SF', 'Shift_Factor_sink': 'SINK_SF'})

    return final_df

lower_bound = (date.today() - timedelta(days=DAYS_BACK)).strftime('%m/%d/%Y')
today = (date.today() + timedelta(days=1)).strftime('%m/%d/%Y')

yes_df = grab_yes_data(lower_bound, today)
network_df = aggregate_network_files(2024, LIMIT)
network_df = network_df[network_df['SCED_Time_Stamp'] >= lower_bound]

lower_bound = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')

ercotapi_df = grab_ercotapi_data(lower_bound, "01", today, "01")
ercotapi_df = pd.concat([network_df, ercotapi_df])
ercotapi_df = ercotapi_df.drop_duplicates()

merged_df = merge_paths_ercot(paths_df, ercotapi_df)

# Merge in the Shadow Prices
merged_df['Contingency_Name'] = merged_df['Contingency_Name'].astype(str)
merged_df['DATETIME'] = pd.to_datetime(merged_df['DATETIME']) 
merged_df['Hour_Ending'] = merged_df['Hour_Ending'].astype(int)  

yes_df['CONTINGENCY'] = yes_df['CONTINGENCY'].astype(str)
yes_df['DATETIME'] = pd.to_datetime(yes_df['DATETIME'])
yes_df['HOURENDING'] = yes_df['HOURENDING'].astype(int)

merged_df = pd.merge(merged_df, yes_df, left_on=['Contingency_Name', 'DATETIME', 'Hour_Ending'], right_on=['CONTINGENCY', 'DATETIME', 'HOURENDING'])
filtered_df = merged_df[merged_df.apply(lambda x: x['Constraint_Name'] in x['REPORTED_NAME'], axis=1)]

filtered_df.drop(columns=['Constraint_Name', 'Contingency_Name', 'DATETIME', 'Hour_Ending'], inplace=True)
filtered_df['Path'] = filtered_df['SOURCE'].astype(str) + '+' + filtered_df['SINK'].astype(str)
filtered_df['Date'] = filtered_df['SCED_Time_Stamp'].dt.strftime("%m/%d/%Y")
filtered_df['Interval'] = (filtered_df['SCED_Time_Stamp'].dt.minute // 5) + 1

filtered_df.drop(columns=['SOURCE', 'SINK', 'SCED_Time_Stamp'], inplace=True, errors='ignore')
filtered_df = filtered_df.rename(columns={'REPORTED_NAME': 'Constraint', 'SOURCE_SF': 'Source SF', 'SINK_SF': 'Sink SF', 'CONTINGENCY': 'Contingency', 'SHADOWPRICE': 'ShadowPrice'}, errors='ignore')
filtered_df = filtered_df[['Date', 'HOURENDING',  'Interval', 'Path', 'Constraint', 'Contingency', 'Source SF', 'Sink SF', 'ShadowPrice']]

filtered_df['$ Cong MWH'] = (filtered_df['Source SF'] - filtered_df['Sink SF']) * filtered_df['ShadowPrice']
filtered_df = filtered_df.drop_duplicates()

filtered_df = filtered_df[filtered_df["Path"].isin(paths_df['PATH'])]
filtered_df = filtered_df.dropna(subset=['$ Cong MWH']).drop(columns=['ShadowPrice'])
filtered_df.to_csv(delta_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

