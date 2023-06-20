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

The code should work regardless of which directory it is placed in.
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
    - df: The original, raw DataFrame. Assume the DataFrame only contains data from one day & hour-ending pair.
    - mapping: The pre-processed mapping containing all 

Output:
    - df_converted: The converted DataFrame after the above operations have been performed.
"""
def convert_csv(df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
    filtered_df = df[df['Settlement_Point'].isin(nodes)]
    
    datetime_obj = datetime.strptime(filtered_df.iloc[0, 0], "%m/%d/%Y %H:%M:%S")
    next_hour = (datetime_obj + timedelta(hours=1)).strftime("%H")
    df_date = (datetime_obj + timedelta(hours=1)).strftime("%m/%d/%Y")
    
    if next_hour == "00":
        next_hour = "24"

    next_hour = next_hour.lstrip('0')
    hourEnding = [next_hour] * len(filtered_df)

    # Search for ShadowPrice and FacilityType
    shadowPrices = []
    facilityTypes = []

    facilityMapping = mapping[df_date][next_hour]

    for _, row in filtered_df.iterrows():
        row_constraint = row['Constraint_Name']
        row_contingency = row['Contingency_Name']

        found = False
        for facilityName in facilityMapping:
            if row_constraint in facilityName:
                for contingency, shadow, fac_type in facilityMapping[facilityName]:
                    if row_contingency == contingency:
                        shadowPrices.append(shadow)
                        facilityTypes.append(fac_type)
                        found = True
                        break

        if not found:
            shadowPrices.append(" ")
            facilityTypes.append(" ")

    filtered_df.insert(1, 'Hour_Ending', hourEnding)
    filtered_df['Shadow_Price'] = shadowPrices
    filtered_df['Facility_Type'] = facilityTypes

    print(df_date)
    return filtered_df


if not os.path.isfile(json_path):
    mapping = dict(process_mapping("12/31/" + str(year - 1), "12/31/" + str(year)))

    if year != datetime.now().year:
        mapping["Latest Date Queried"] = "12/31"

    else:
        mapping["Latest Date Queried"] = datetime.now().strftime("%m/%d")

    json_data = json.dumps(mapping)

    with open(json_path, "w") as file:
        file.write(json_data)

elif year != datetime.now().year:
    with open(json_path, "r") as file:
        mapping = json.load(file)

else:
    with open(json_path, "r") as file:
        mapping = json.load(file)

    latest = mapping["Latest Date Queried"]

    if latest != datetime.now().strftime("%m/%d"):
        mapping.update(dict(process_mapping(latest + "/" + str(year - 1), "today")))
        mapping["Latest Date Queried"] = datetime.now().strftime("%m/%d")

        with open(json_path, "w") as file:
            json.dump(mapping, file)


# Grab the list of RT Hourly Zip Files for the year.
merge = []
yearly_zip_files = os.listdir(zip_base)
for zip_file in yearly_zip_files:
    with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
        df_csv = pd.read_csv(zip_path.open(zip_path.namelist()[0]))
        merge.append(convert_csv(df_csv, mapping))

merged_df = pd.DataFrame(pd.concat(merge, axis=0))
merged_df.to_csv(output_path, index=False)



end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

