import json
from itertools import product
import requests
import pandas as pd
from datetime import date, timedelta
import time
from io import StringIO
from typing import Dict, Union

"""
This Python task aims to utilize the summary of the real-time constraint data of 2023 to generate
a table that contains the progression of Delta data for a certain set of paths. 

Be sure to run RT_Constraint_Aggregator.py first before running this script in order to 
collect the most-recent data.

The outputted table is located in the Data subfolder or at \\pzpwtabapp01\Ercot\Exposure_SCED_2023.csv
"""

# Global Variables and Parameters
start_time = time.time()
year = date.today().year

json_processed = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/processed_" + str(year) + "_summary.json"
output_path = f"\\\\pzpwtabapp01\\Ercot\\Exposure_SCED_{year}.csv"
last_30_path = "\\\\pzpwtabapp01\\Ercot\\Exposure_SCED_Last_30_Days.csv"

credential_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"

# Grab the set of all paths that we are interested in - first grab credentials
with open(credential_path, "r") as credentials:
    auth = tuple(credentials.read().split())

call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?"
r = requests.get(call1, auth=auth)
df = pd.read_csv(StringIO(r.text))
df['Path'] = df['SOURCE'] + '+' + df['SINK']
df = df[['Path', 'SOURCE', 'SINK']]
df = df.drop_duplicates()


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
    columns = ['Date', 'HourEnding', 'Interval', 'PeakType', 'Constraint', 'Contingency', 'Path', 'Source SF',
               'Sink SF', '$ Cong MWH']

    # Initialize empty lists for each column
    data = [[] for _ in columns]

    # Get the mappings for source and sink
    source_map = mapping[source]
    sink_map = mapping[sink]

    # Iterate over each date and source item in the source mapping
    for date, source_list in source_map.items():
        # Check if the date exists in the sink mapping
        if date in sink_map:

            # Get the sink list for the corresponding date
            sink_list = sink_map[date]

            # Iterate over each combination of source and sink items
            for source_item, sink_item in product(source_list, sink_list):
                contin, constr, peak, sf, ss = source_item
                sink_contin, sink_constr, sink_peak, sink_sf, sink_ss = sink_item

                # Check if the values match for contingency, constraint, and peak type
                if contin == sink_contin and constr == sink_constr and peak == sink_peak:
                    if abs(sf) > 0.001 and abs(sink_sf) > 0.001:
                        # Append the data to the respective columns
                        data[0].append(date[:10])
                        data[1].append(int(date[11:13]) + 1)
                        data[2].append(int(date[14:16]) // 5 + 1)
                        data[3].append(peak)
                        data[4].append(constr)
                        data[5].append(contin)
                        data[6].append(f"{source}+{sink}")
                        data[7].append(sf)
                        data[8].append(sink_sf)
                        data[9].append(ss - sink_ss)

    # Create a DataFrame from the accumulated data
    res = pd.DataFrame(dict(zip(columns, data)))

    return res


# Load the mapping file that contains a JSON of summarized data for the year
with open(json_processed, "r") as map_file:
    mapping = json.load(map_file)

# Grab and merge together the data
final_merge = []
for _, row in df.iterrows():
    source = row['SOURCE']
    sink = row['SINK']

    df_path = accumulate_data(mapping, source, sink)

    if df_path is not None:
        final_merge.append(df_path)
        print(f"{source} to {sink} completed.")

df_merged = pd.concat(final_merge, axis=0)

# Do some post-processing of the data
df_merged = df_merged.drop_duplicates(
    subset=['Date', 'HourEnding', 'Interval', 'PeakType', 'Constraint', 'Contingency', 'Path'])
df_merged = df_merged.sort_values(by=['Date', 'HourEnding', 'Interval'])
df_merged = df_merged[df_merged['PeakType'] != ""]
df_merged['Date'] = pd.to_datetime(df_merged["Date"], format="%m/%d/%Y")

# Output a file for data in the last thirty days
today = pd.to_datetime(date.today())  # Convert today to datetime object
lower_bound = today - timedelta(days=30)

# From January 1st to January 30th
if lower_bound.year != today.year:
    last_year = f"\\\\pzpwtabapp01\\Ercot\\Exposure_SCED_{lower_bound.year}.csv"
    
    try:
        df_last = pd.read_csv(last_year)
        df_last['Date'] = pd.to_datetime(df_last["Date"], format="%m/%d/%Y")
        df_last = df_last[df_last["Date"] >= lower_bound]
        df_merged_last_30 = pd.concat([df_last, df_merged])

        df_merged_last_30 = df_merged_last_30.sort_values(by="Date")
        df_merged_last_30 = df_merged_last_30.drop_duplicates()
    except IOError:
        print("Error in reading last year's file")

# From February and Beyond
else:
    df_merged_last_30 = df_merged[df_merged["Date"] >= lower_bound]

try:
    df_merged_last_30.to_csv(last_30_path, index=False)
except IOError:
    print("Error in writing to last 30 days' file")

df_merged.to_csv(output_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")