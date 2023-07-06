import json
from itertools import product
import requests
import pandas as pd
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
year = 2023

json_processed = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/processed_" + str(year) + "_summary.json"
output_path = (
        "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/Exposure_SCED_" + str(year) + ".csv"
) if year != 2023 else "\\\\pzpwtabapp01\\Ercot\\Exposure_SCED_2023.csv"

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
                    if abs(sf) > 0.01 and abs(sink_sf) > 0.01:
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
df_merged.to_csv(output_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")