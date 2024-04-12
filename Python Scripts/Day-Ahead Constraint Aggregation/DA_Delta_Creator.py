import zipfile
from io import StringIO, BytesIO
import json
from itertools import product
import requests
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

json_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated DA Constraint Data/" + str(year) + "_web_data.json"
json_summary = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated DA Constraint Data/processed_" + str(year) + "_summary.json"
delta_path = f"\\\\pzpwtabapp01\\Ercot\\Exposure_DAM_Last_30_Days.csv"
credential_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/DA/ERCOT?"

# Adjust this to change how many days of historical data we want.
days_back = 30

with open(credential_path, "r") as credentials:
    auth = tuple(credentials.read().split())

call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?"
r = requests.get(call1, auth=auth)
df = pd.read_csv(StringIO(r.text))
unique_nodes = pd.concat([df["SINK"], df['SOURCE']]).unique()

def process_mapping(start_date: str, end_date: str) -> Union[
        Dict[str, Dict[str, Dict[str, List[Tuple[str, str]]]]], None]:
    """
    A helper method that queries Yes Energy to grab the ERCOT hourly constraint data in a certain date range.
    Pre-processes the data into a large dictionary in order to speed up future search times.

    Inputs:
        - start_date: A string in the MM/DD/YYYY format giving the inclusive lower bound.
        - end_date: A string in the MM/DD/YYYY format giving the inclusive upper bound.

        Assume that start_date and end_date are within 367 days of each other.

    Output:
        - res, the pre-processed data stored as a nested dictionary. For reference, res[a][b][c] gives a list of
          3-element tuples corresponding to:
            - a: The Date in MM/DD/YYYY format.
            - b: The hour ending, ranging from 1 to 24.
            - c: The full ReportedName.

        Each tuple (x, y) in res[a][b][c] gives
            - x: Contingency
            - y: PeakType
    """

    # Build the request URL.
    req_url = f"{yes_energy}startdate={start_date}&enddate={end_date}"
    yes_req = requests.get(req_url, auth=auth)
    print(req_url)

    # Build the result dictionary if the website was successfully queried.
    yes_table = pd.read_html(StringIO(yes_req.text))[0]
    res = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for _, row in yes_table.iterrows():
        date = row['DATETIME'][:10]
        hour = str(row['HOURENDING'])
        facility_name = row['REPORTED_NAME']

        contingency = row['CONTINGENCY']
        peak_type = row['PEAKTYPE']

        res[date][hour][facility_name].append((contingency, peak_type))

    return res


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
    ercot_url = f"https://ercotapi.app.calpine.com/reports?reportId=13089&marketParticipantId=CRRAH&startTime={start_datetime.strftime('%Y-%m-%dT%H:00:00')}&endTime={end_datetime.strftime('%Y-%m-%dT%H:00:00')}&unzipFiles=false"
    
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
                            df_chunk = pd.read_csv(inner_csv)
                            merged.append(df_chunk)
    else:
        print(response.status_code)
    
    return merged

def grab_latest_data(l_d: str, l_h: str, u_d: str, u_h: str) -> pd.DataFrame:
    """Queries a certain range of data through ERCOT API in chunks and aggregates the results into one DataFrame."""
    time_ranges = split_time_range(l_d, l_h, u_d, u_h)
    all_data = []
    
    for start_dt, end_dt in time_ranges:
        chunk_data = query_ercot_data(start_dt, end_dt)
        all_data.extend(chunk_data)
    
    return all_data


def findDesired(mapping: Dict, row) -> Tuple[str, str]:
    """
    Given a DataFrame row, this helper method searches a mapping for the full constraint
    name and PeakType corresponding to entries in the row.

    Inputs:
        - mapping: A dictionary mapping facilityNames to a tuple of contingency,
                   shadowPrice and facilityType.

    Output:
        - A tuple of full ConstraintName, PeakType if found.
          Blank strings otherwise.
    """

    row_constraint = row['ConstraintName']
    row_contingency = row['ContingencyName'].strip()

    for reportedName in mapping:
        # Parse the reportedName string, since row_constraint can be a substring.
        if row_constraint in reportedName:
            for contingency, peak_type in mapping[reportedName]:
                # Contingencies must match
                if row_contingency == contingency:
                    return reportedName, peak_type

    return "", ""

def process_csv(existing_data: Dict, yes_mapping: Dict, raw_data: pd.DataFrame):
    """
    Processes a raw DataFrame of data and populates an existing summary dictionary with its entries.

    Inputs:
        - existing_data: The existing dictionary of summary data. For reference, existing_dict[a][b][c]
        gives a list of 5-element tuples corresponding to
            - a: The Settlement Point (such as HB_North)
            - b: The date in MM/DD/YYYY format
            - c: The HourEnding.

        Each tuple (v, w, x, y, z) in existing_dict[a][b][c] gives
            - v: The Contingency Name
            - w: The Constraint Name
            - x: The Peak Type
            - y: The Shift Factor
            - z: The 'ShadowShift' - the Shift Factor multiplied by the Shadow Price
        - yes_mapping: The pre-processed mapping from Yes Energy
        - raw_data: A DataFrame storing the data desired to be summarized.

    Output:
        - Nothing, but updates the existing dictionary with new entries in the raw data.
    """
    raw_data = raw_data[raw_data['SettlementPoint'].isin(unique_nodes)]

    raw_data['ShadowPrice'] = pd.to_numeric(raw_data['ShadowPrice'], errors='coerce')
    raw_data['ShiftFactor'] = pd.to_numeric(raw_data['ShiftFactor'], errors='coerce')
    deliveryDate = raw_data.iloc[0, 0]

    for _, row in raw_data.iterrows():
        parsedHour = str(int(row['HourEnding'].split(":")[0]))
        contingency = row['ContingencyName'].strip()
        shadowPrice = row['ShadowPrice']
        shiftFactor = row['ShiftFactor']
        shadowShift = shiftFactor * shadowPrice
        
        if parsedHour == '24':
            nextDate = datetime.strptime(deliveryDate, "%m/%d/%Y") + timedelta(days=1)
            nextDate = nextDate.strftime("%m/%d/%Y")
        
        else:
            nextDate = deliveryDate
            
        if nextDate not in yes_mapping:
            continue
        
        if parsedHour in yes_mapping[deliveryDate]:
            fullConstraint, peak = (findDesired(yes_mapping[nextDate][parsedHour], row)
                                    if parsedHour == '24' 
                                    else findDesired(yes_mapping[deliveryDate][parsedHour], row))

            if row['SettlementPoint'] not in existing_data:
                existing_data[row['SettlementPoint']] = {}

            if deliveryDate not in existing_data[row['SettlementPoint']]:
                existing_data[row['SettlementPoint']][deliveryDate] = {}

            if parsedHour not in existing_data[row['SettlementPoint']][deliveryDate]:
                existing_data[row['SettlementPoint']][deliveryDate][parsedHour] = []

            existing_data[row['SettlementPoint']][deliveryDate][parsedHour].append((contingency, fullConstraint, peak, shiftFactor, shadowShift))
    # Progress-checking print statement
    print("Finished " + deliveryDate)


def accumulate_data(mapping: Dict, source: str, sink: str) -> Union[pd.DataFrame, None]:
    """
    Accumulates data from the given mapping dictionary based on the specified source and sink.

    Args:
        mapping (Dict): Mapping dictionary containing the data.
        source (str): Key representing the source in the mapping.
        sink (str): Key representing the sink in the mapping.

    Returns:
        Union[pd.DataFrame, None]: A DataFrame containing the accumulated data,
                    or None if either source or sink is not found in the mapping.
    """

    # Check if source and sink exist in the mapping
    if source not in mapping or sink not in mapping:
        return None

    # Define column names
    columns = ['Date', 'HourEnding', 'PeakType', 'Constraint', 'Contingency', 'Path', 'Source SF',
               'Sink SF', '$ Cong MWH']

    # Initialize empty lists for each column
    data = [[] for _ in columns]

    # Get the mappings for source and sink
    source_map = mapping[source]
    sink_map = mapping[sink]

    # Iterate over each date and source item in the source mapping
    for date in source_map:
        for hour, source_list in source_map[date].items():
            # Check if the date exists in the sink mapping
            if date in sink_map and hour in sink_map[date]:
                # Get the sink list for the corresponding date
                sink_list = sink_map[date][hour]

                # Iterate over each combination of source and sink items
                for source_item, sink_item in product(source_list, sink_list):
                    contin, constr, peak, sf, ss = source_item
                    sink_contin, sink_constr, sink_peak, sink_sf, sink_ss = sink_item

                    # Check if the values match for contingency, constraint, and peak type
                    if contin == sink_contin and constr == sink_constr and peak == sink_peak:
                        # Append the data to the respective columns
                        data[0].append(date)
                        data[1].append(hour)
                        data[2].append(peak)
                        data[3].append(constr)
                        data[4].append(contin)
                        data[5].append(f"{source}+{sink}")
                        data[6].append(sf)
                        data[7].append(sink_sf)
                        data[8].append(ss - sink_ss)

    # Create a DataFrame from the accumulated data
    res = pd.DataFrame(dict(zip(columns, data)))

    return res

"""
Procedure to store the pre-processed data as JSONs in the grandparent directory's
Data subfolder. Querying a entire year of data from Yes Energy every single time can be costly, so
this improves performance a tad. 

Each JSON data file also contains a "Latest Date Queried" field, which stores
the latest date in MM/DD format queried from the website.
"""
lower_bound = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
today = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')
mapping = {}

# If the JSON file does not already exist, query all available data and create the JSON file
if not os.path.isfile(json_path):
    # Grab all available data from the year.
    mapping = dict(process_mapping("12/31/" + str(year - 1), "12/31/" + str(year)))
    json_data = json.dumps(mapping)

    with open(json_path, "w") as file:
        file.write(json_data)

# Otherwise, if current year, we query all data from the latest date queried to the current date.
elif year == datetime.now().year:
    with open(json_path, "r") as file:
        mapping = json.load(file)

    mapping = dict(process_mapping(lower_bound, "today+1"))

    with open(json_path, "w") as file:
        json.dump(mapping, file)

# Read in the existing, complete JSON if not current year.
else:
    with open(json_path, "r") as file:
        mapping = json.load(file)
"""
Now that we have the mapping, we can begin converting and aggregating the data.
"""
ercot_df = grab_latest_data(lower_bound, "01", today, "01")
existing_sum = {}

for inner_df in ercot_df:
    process_csv(existing_sum, mapping, inner_df)

with open(json_summary, "w") as json_sum:
    json_sum.write(json.dumps(existing_sum))

"""
Create the Delta Table using the newly updated summary dictionary.
"""
call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?"
r = requests.get(call1, auth=auth)
df = pd.read_csv(StringIO(r.text))
df['Path'] = df['SOURCE'] + '+' + df['SINK']
df = df[['Path', 'SOURCE', 'SINK']]
df = df.drop_duplicates()

final_merge = []
for _, row in df.iterrows():
    source = row['SOURCE']
    sink = row['SINK']

    df_path = accumulate_data(existing_sum, source, sink)

    if df_path is not None:
        final_merge.append(df_path)
        print(f"{source} to {sink} completed.")

df_merged = pd.concat(final_merge, axis=0)

# Do some post-processing of the data
df_merged = df_merged.drop_duplicates(
    subset=['Date', 'HourEnding', 'PeakType', 'Constraint', 'Contingency', 'Path'])
df_merged['Date'] = pd.to_datetime(df_merged['Date'], format='%m/%d/%Y')
df_merged = df_merged.sort_values(by=['Date', 'HourEnding'])
df_merged.to_csv(delta_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

