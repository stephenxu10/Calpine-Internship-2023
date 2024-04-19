import zipfile
from io import StringIO, BytesIO
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import concurrent.futures
import time
import os
from datetime import date
from datetime import timedelta, datetime

warnings.simplefilter("ignore")
PATHID = 1073125

# Global Variables and Parameters.
start_time = time.time()
year = date.today().year
pd.set_option('display.max_columns', 500)

YEAR = 2024
DAYS_BACK = 365
LIMIT = 0.005
mis_path = "//Pzpwuplancli01/Uplan/ERCOT"
delta_path = f"//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/10 - Studies/2024/Summer Prep/Extracts/RT Delta/Exposure_SCED_{YEAR}_{PATHID}.csv"
credential_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/RT/ERCOT?"
 
with open(credential_path, "r") as credentials:
    auth = tuple(credentials.read().split())

call1 = f"https://services.yesenergy.com/PS/rest/ftr/portfolio/{PATHID}/paths.csv?"
r = requests.get(call1, auth=auth)
paths_df = pd.read_csv(StringIO(r.text))
unique_nodes = pd.concat([paths_df['SOURCE'], paths_df['SINK']]).unique()

paths_df['PATH'] = paths_df['SOURCE'] + '+' + paths_df['SINK']
paths_df = paths_df[['PATH', 'SOURCE', 'SINK']]
paths_df = paths_df.drop_duplicates()

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

def process_zip_file(zip_path: str, limit) -> pd.DataFrame:
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
        yearly_zip_files = [os.path.join(yearly_base, file) for file in os.listdir(yearly_base) if file.endswith('_csv.zip')]
        
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
    merged_df = pd.merge(merged_df, ercot_df, left_on=['SINK', 'SCED_Time_Stamp', 'Constraint_Name', 'Contingency_Name'], right_on=['Settlement_Point', 'SCED_Time_Stamp', 'Constraint_Name', 'Contingency_Name'], how='inner', suffixes=('', '_sink'))

    # Clean up and select the needed columns only once, also handling duplicates and NaNs efficiently
    merged_df = merged_df.dropna(subset=['Hour_Ending'])
    merged_df = merged_df.drop_duplicates().sort_values(by=['SCED_Time_Stamp', 'Hour_Ending'])

    # Format SCED_Time_Stamp for final output, if necessary
    merged_df['DATETIME'] = merged_df['SCED_Time_Stamp'].dt.strftime("%m/%d/%Y")

    final_columns = ['SOURCE', 'SINK', 'SCED_Time_Stamp', 'DATETIME', 'Shift_Factor', 'Shift_Factor_sink', 'Constraint_Name', 'Contingency_Name', 'Hour_Ending']
    final_df = merged_df[final_columns].rename(columns={'Shift_Factor': 'SOURCE_SF', 'Shift_Factor_sink': 'SINK_SF'})

    return final_df

lower_bound = f"01/01/{YEAR}"
today = f"12/31/{YEAR}"

yes_df = grab_yes_data(lower_bound, today)
network_df = aggregate_network_files(YEAR, LIMIT)
network_df = network_df[network_df['SCED_Time_Stamp'] >= lower_bound]

merged_df = merge_paths_ercot(paths_df, network_df)
print("merged!")

# Merge in the Shadow Prices
merged_df['Contingency_Name'] = merged_df['Contingency_Name'].astype(str)
merged_df['DATETIME'] = pd.to_datetime(merged_df['DATETIME']) 
merged_df['Hour_Ending'] = merged_df['Hour_Ending'].astype(int)  

#%%
yes_df['CONTINGENCY'] = yes_df['CONTINGENCY'].astype(str)
yes_df['DATETIME'] = pd.to_datetime(yes_df['DATETIME'])
yes_df['HOURENDING'] = yes_df['HOURENDING'].fillna(0).astype(int)
yes_df = yes_df.dropna()

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
filtered_df = filtered_df[filtered_df['ShadowPrice'] > 0]
filtered_df = filtered_df.dropna(subset=['$ Cong MWH']).drop(columns=['ShadowPrice'])
filtered_df.to_csv(delta_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

