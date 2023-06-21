from io import StringIO
import zipfile
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
This Python script aims to aggregate the real-time ERCOT market constraints across an entire year. Additionally,
it grabs additional data from yesenergy and matches it to each entry in the raw data. Furthermore, we are only interested
in ERCOT Calpine settlement points, stored in:

https://services.yesenergy.com/PS/rest/collection/node/2697330
"""
warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = 2023

zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\130_SSPSF"
json_path = "./../Data/Aggregated RT Constraint Data/current_" + str(year) + "_web_data.json"
output_path = "./../Data/Aggregated RT Constraint Data/RT_Summary_" + str(year) + ".csv"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/RT/ERCOT?"
auth = ('transmission.yesapi@calpine.com', 'texasave717')

# Extract the set of all nodes that we are interested in
nodes_req = requests.get("https://services.yesenergy.com/PS/rest/collection/node/2697330", auth=auth)

if nodes_req.status_code == 200:
    node_names = pd.read_html(StringIO(nodes_req.text))
    nodes = set(node_names[0]['PNODENAME'].unique())

else:
    print("Request to obtain node values failed.")

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
        - c: The full FacilityName.
    
    Each tuple (x, y, z) in res[a][b][c] gives
        - x: Contingency
        - y: ShadowPrice
        - z: FacilityType
"""
def process_mapping(start_date: str, end_date: str) -> Union[Dict[str, Dict[str, Dict[str, List[Tuple[str, str, str]]]]], None]:
    # Build the request URL.
    req_url = f"{yes_energy}startdate={start_date}&enddate={end_date}"
    yes_req = requests.get(req_url, auth=auth)

    # Build the result dictionary if the website was successfully queried.
    yes_table = pd.read_html(StringIO(yes_req.text))[0]
    res = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for _, row in yes_table.iterrows():
        date = row['DATETIME'][:10]
        hour = str(row['HOURENDING'])
        facility_name = row['FACILITYNAME']

        contingency = row['CONTINGENCY']
        shadowPrice = row['SHADOWPRICE']
        facility_type = row['FACILITYTYPE']

        res[date][hour][facility_name].append((contingency, shadowPrice, facility_type))

    return res


"""
Given a DataFrame row, this helper method searches a mapping for the shadowPrice
and facilityType corresponding to certain entries in the row.

Inputs:
    - mapping: A dictionary mapping facilityNames to a tuple of contingency,
               shadowPrice and facilityType.
               
Output:
    - A tuple of shadowPrice and facilityType, if found. Blank strings otherwise.
"""
def findDesired(mapping: Dict, row) -> Tuple[str, str]:
    row_constraint = row['Constraint_Name']
    row_contingency = row['Contingency_Name']

    for facilityName in mapping:
        # Parse the facilityName string, since row_constraint can be a substring.
        if row_constraint in facilityName:
            for contingency, shadow, fac_type in mapping[facilityName]:
                # Contingencies must match
                if row_contingency == contingency:
                    return shadow, fac_type

    return "", ""

"""
Given a DataFrame of raw data taken from the drive, this helper method converts it by doing the following:
    1) Filter out the rows to only include Calpine ERCOT nodes.
    2) Add a column for the HourEnding
    3) Match each filtered row to the pre-processed data to accumulate the ShadowPrice and FacilityType

Inputs:
    - df: The original, raw DataFrame. Assume the DataFrame only contains data from one day & hour-ending pair.
    - mapping: The pre-processed mapping containing all relevant data

Output:
    - df_converted: The converted DataFrame after the above operations have been performed.
"""
def convert_csv(df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
    # Filter out the original DataFrame to only include desired settlement point nodes
    filtered_df = df[df['Settlement_Point'].isin(nodes)]
    
    # Parse the date and determine the next hour
    datetime_obj = datetime.strptime(filtered_df.iloc[0, 0], "%m/%d/%Y %H:%M:%S")
    next_hour = (datetime_obj + timedelta(hours=1)).strftime("%H")
    df_date = (datetime_obj + timedelta(hours=1)).strftime("%m/%d/%Y")
    
    if next_hour == "00":
        next_hour = "24"

    next_hour = next_hour.lstrip('0')
    hourEnding = [next_hour] * len(filtered_df)

    # Build the shadowPrice and facilityType columns
    shadowPrices = []
    facilityTypes = []
    
    if next_hour not in mapping[df_date]:
        shadowPrices = [""] * len(filtered_df)
        facilityTypes = [""] * len(filtered_df)
    
    else:
        facilityMapping = mapping[df_date][next_hour]
        for _, row in filtered_df.iterrows():
            shadow, facType = findDesired(facilityMapping, row)
            shadowPrices.append(shadow)
            facilityTypes.append(facType)
        
    # Add in the new columns and return the resulting DataFrame
    filtered_df.insert(1, 'Hour_Ending', hourEnding)
    filtered_df['Shadow_Price'] = shadowPrices
    filtered_df['Facility_Type'] = facilityTypes
    
    # Progress-checking print statement
    print(df_date + " " + next_hour)
    return filtered_df


"""
Procedure to store the pre-processed data as JSONs in the parent directory's
Data subfolder. Querying Yes Energy every single time can be costly, so
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
    new_data = dict(process_mapping(latest_date + "/" + str(year), "today"))
    mapping.update(new_data)
    mapping["Latest Date Queried"] = datetime.now().strftime("%m/%d")
    
    with open(json_path, "w") as file:
        json.dump(mapping, file)


# Grab the list of RT Hourly Zip Files for the year.
merge = []
yearly_zip_files = os.listdir(zip_base)

# If the output CSV does not exist, convert all CSVs within the requested year and write to output_path
if not os.path.isfile(output_path):
    for zip_file in yearly_zip_files:
        with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
            df_csv = pd.read_csv(zip_path.open(zip_path.namelist()[0]))
            merge.append(convert_csv(df_csv, mapping))

    merged_df = pd.DataFrame(pd.concat(merge, axis=0))
    merged_df.to_csv(output_path, index=False)

# Otherwise, if the output CSV does exist, only update if requested year is the current year
elif year == datetime.now().year:
    for zip_file in yearly_zip_files:
        zip_date = datetime.strptime(zip_file[34:36] + "/" + zip_file[36:38] + "/" + str(year), "%m/%d/%Y")
        
        # Convert all newly added CSVs since the last aggregation
        if zip_date > datetime.strptime(latest_date + "/" + str(year), "%m/%d/%Y"):
            with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
                df_csv = pd.read_csv(zip_path.open(zip_path.namelist()[0]))
                merge.append(convert_csv(df_csv, mapping))
    
    # Append the new data to the existing data
    merged_df = pd.DataFrame(pd.concat(merge, axis=0))
    merged_df.to_csv(output_path, mode='a', index=False, header=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

