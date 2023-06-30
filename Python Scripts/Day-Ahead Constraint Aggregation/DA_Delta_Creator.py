from io import StringIO
import json
import requests
import warnings
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import time
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
    3) Convert and aggregate all unread data from the CA drive using the pre-processed mapping.
    4) Post-process the aggregated data into a summary JSON.
    5) Use this summarized JSON to generate the Delta Table and output it to a CSV.
"""
warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = 2023

zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\55_DSF"
json_path = "./../../Data/Aggregated DA Constraint Data/" + str(year) + "_web_data.json"
json_summary = "./../../Data/Aggregated DA Constraint Data/processed_" + str(year) + "_summary.json"
delta_path = "./../../Data/Aggregated DA Constraint Data/Exposure_DAM_" + str(year) + ".csv"
credential_path = "./../../credentials.txt"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/DA/ERCOT?"
days_back = 2

with open(credential_path, "r") as credentials:
    auth = tuple(credentials.read().split())

# Extract the set of all nodes that we are interested in
nodes_req = requests.get("https://services.yesenergy.com/PS/rest/collection/node/2697330", auth=auth)

if nodes_req.status_code == 200:
    node_names = pd.read_html(StringIO(nodes_req.text))
    nodes = set(node_names[0]['PNODENAME'].unique())

else:
    print("Request to obtain node values failed.")

def process_mapping(start_date: str, end_date: str) -> Union[
        Dict[str, Dict[str, Dict[str, List[Tuple[str, str, str, str]]]]], None]:
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

        Each tuple (x, y, z) in res[a][b][c] gives
            - x: Contingency
            - y: FacilityType
            - z: PeakType
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
        facility_type = row['FACILITYTYPE']
        peak_type = row['PEAKTYPE']

        res[date][hour][facility_name].append((contingency, facility_type, peak_type))

    return res


"""
Procedure to store the pre-processed data as JSONs in the parent directory's
Data subfolder. Querying a entire year of data from Yes Energy every single time can be costly, so
this improves performance a tad. 

Each JSON data file also contains a "Latest Date Queried" field, which stores
the latest date in MM/DD format queried from the website.
"""
mapping = {}
latest_date = ""

# If the JSON file does not already exist, query all available data and create the JSON file
if not os.path.isfile(json_path):
    # Grab all available data from the year.
    mapping = dict(process_mapping("12/31/" + str(year - 1), "12/31/" + str(year)))

    # If the year parameter is not the current year, we've grabbed everything
    if year != datetime.now().year:
        mapping["Latest Date Queried"] = "12/31"

    # Otherwise, if we are working with current data, the latest date queried is today.
    else:
        mapping["Latest Date Queried"] = datetime.now().strftime("%m/%d")

    json_data = json.dumps(mapping)

    with open(json_path, "w") as file:
        file.write(json_data)

# Otherwise, if current year, we query all data from the latest date queried to the current date.
elif year == datetime.now().year:
    with open(json_path, "r") as file:
        mapping = json.load(file)

    latest_date = mapping["Latest Date Queried"]
    lower_bound = datetime.strptime(latest_date + "/" + str(year), "%m/%d/%Y") - timedelta(days=days_back)
    new_data = dict(process_mapping(str(lower_bound.date()), "today"))
    mapping.update(new_data)
    mapping["Latest Date Queried"] = datetime.now().strftime("%m/%d")

    with open(json_path, "w") as file:
        json.dump(mapping, file)

# Read in the existing, complete JSON if not current year.
else:
    with open(json_path, "r") as file:
        mapping = json.load(file)
