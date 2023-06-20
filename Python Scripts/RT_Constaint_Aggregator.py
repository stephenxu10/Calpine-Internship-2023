from io import StringIO
import zipfile
import requests
import warnings
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import time
import os

"""
This Python script aims to aggregate the real-time ERCOT market constraints across an entire year. Additionally,
it grabs additional data from yesenergy and matches it to each entry in the raw data. Furthermore, we are only interested
in ERCOT Calpine settlement points, stored in:

https://services.yesenergy.com/PS/rest/collection/node/2697330

The code should work regardless of which directory it is placed in.
"""
warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = 2023

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/RT/ERCOT?"
zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\130_SSPSF"
output_path = "./Data/Aggregated RT Constraint Data/RT_Summary_" + str(year) + ".csv"
auth = ('transmission.yesapi@calpine.com', 'texasave717')
nodes = set()


# Extract the set of all nodes that we are interested in
nodes_req = requests.get("https://services.yesenergy.com/PS/rest/collection/node/2697330", auth=auth)

if nodes_req.status_code == 200:
    node_names = pd.read_html(StringIO(nodes_req.text))
    nodes = set(node_names[0]['PNODENAME'].unique())

else:
    print("Request to obtain node values failed. :(")


"""
A helper method that queries Yes Energy to grab the ERCOT hourly constraint data in a certain date range.
Pre-processes the data into a large dictionary in order to speed up future search times.

Inputs:
    - start_date: A string in the MM/DD/YYYY format giving the inclusive lower bound.
    - end_date: A string in the MM/DD/YYYY format giving the inclusive upper bound.
    
    Assume that start_date and end_date are within six months of each other.
    
Output:
    - res, the pre-processed data stored as a nested dictionary. For reference, res[a][b][c] gives a list of 
      3-element tuples corresponding to:
        - a: The Date in MM/DD/YYYY format.
        - b: The hour ending, ranging from 1 to 24.
        - c: The full FacilityName.
    
    Each tuple (x, y, z) in res[a][b][c] gives
        - x: Contingency
        - y: ShadowPrice
        - z: FacilityType
"""
def process_mapping(start_date: str, end_date: str) -> Union[Dict[str, Dict[int, Dict[str, List[Tuple[str, str, str]]]]], None]:
    # Build the request URL.
    req_url = f"{yes_energy}startdate={start_date}&enddate={end_date}"
    yes_req = requests.get(req_url, auth=auth)

    # Build the result dictionary if the website was successfully queried.
    if yes_req.status_code == 200:
        yes_table = pd.read_html(StringIO(yes_req.text))[0]
        res = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for _, row in yes_table.iterrows():
            date = row['DATETIME'][:10]
            hour = row['HOURENDING']
            facility_name = row['FACILITYNAME']

            contingency = row['CONTINGENCY']
            shadowPrice = row['SHADOWPRICE']
            facility_type = row['FACILITYTYPE']

            res[date][hour][facility_name].append((contingency, shadowPrice, facility_type))

        return res
    else:
        print(req_url)
        print("Invalid request made to Yes Energy. Perhaps you have made too many requests.")
        return


"""
Given a DataFrame of raw data taken from the drive, this helper method converts it by doing the following:
    1) Filter out the rows to only include Calpine ERCOT nodes.
    2) Add a column for the HourEnding
    3) Match each filtered row to the pre-processed data to accumulate the ShadowPrice and FacilityType

Inputs:
    - df: The original, raw DataFrame.

Output:
    - df_converted: The converted DataFrame after the above operations have been performed.
"""
def convert_csv(df: pd.DataFrame) -> pd.DataFrame:
    pass


# Grab the list of RT Hourly Zip Files for the year.
merge = []
yearly_zip_files = os.listdir(zip_base)
for zip_file in yearly_zip_files:
    with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
        df_csv = pd.read_csv(zip_path.open(zip_path.namelist()[0]))
        # merge.append(convert_csv(df_csv))
        print(df_csv.head())
        break

# merged_df = pd.DataFrame(pd.concat(merge, axis=0))
# merged_df.to_csv(output_path, index=False)

"""
mapping = process_mapping("today-1", "today")
for name in mapping:
    print("")
    for hour in mapping[name]:
        print(mapping[name][hour])
"""