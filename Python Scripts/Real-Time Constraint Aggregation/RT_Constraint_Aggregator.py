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
¡
https://services.yesenergy.com/PS/rest/collection/node/2697330
"""
warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = 2023

# How many days we look back
days_back = 2

zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\130_SSPSF"
json_path = "./../../Data/Aggregated RT Constraint Data/current_" + str(year) + "_web_data.json"
json_summary = "./../../Data/Aggregated RT Constraint Data/processed_" + str(year) + "_summary.json"
output_path = "./../../Data/Aggregated RT Constraint Data/RT_Summary_" + str(year) + ".csv"
credential_path = "./../../credentials.txt"

yes_energy = "https://services.yesenergy.com/PS/rest/constraint/hourly/RT/ERCOT?"

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
            - c: The full FacilityName.

        Each tuple (x, y, z) in res[a][b][c] gives
            - x: Contingency
            - y: ShadowPrice
            - z: FacilityType
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
        facility_name = row['FACILITYNAME']

        contingency = row['CONTINGENCY']
        shadowPrice = row['SHADOWPRICE']
        facility_type = row['FACILITYTYPE']
        peak_type = row['PEAKTYPE']

        res[date][hour][facility_name].append((contingency, shadowPrice, facility_type, peak_type))
    
    return res


def post_process(raw_data: pd.DataFrame) -> Dict[str, Dict[str, List[Tuple[str, str, str, float]]]]:
    """
    Post-processes the summary dataFrame data into a nested dictionary in order to accelerate the
    searching & aggregation process for future tasks.

    Input:
        - raw_data: A DataFrame storing the data desired to be summarized.

    Output:
        - A post-processed dictionary with shadowPrice * shiftFactor column.
    """

    unique_nodes = {'GUADG_CCU2', 'CCEC_ST1', 'BVE_UNIT1', 'PSG_PSG_GT3', 'SAN_SANMIGG1', 'CCEC_GT1', 'DDPEC_GT4',
                    'BOSQ_BSQSU_5', 'CTL_GT_104', 'BTE_BTE_G3', 'MIL_MILG345', 'BTE_BTE_G4', 'DUKE_GST1CCU', 'CAL_PUN2',
                    'DDPEC_GT6', 'HB_WEST', 'BOSQ_BSQS_12', 'BTE_BTE_G1', 'TXCTY_CTA', 'JACKCNTY_STG', 'LZ_WEST',
                    'DDPEC_GT2', 'STELLA_RN', 'BOSQ_BSQS_34', 'DC_E', 'CTL_GT_103', 'NED_NEDIN_G2', 'FREC_2_CCU',
                    'TXCTY_CTB', 'HB_SOUTH', 'HB_NORTH', 'GUADG_CCU1', 'NED_NEDIN_G3', 'LZ_SOUTH', 'NED_NEDIN_G1',
                    'JCKCNTY2_ST2', 'BTE_BTE_G2', 'DUKE_GT2_CCU', 'CAL_PUN1', 'PSG_PSG_GT2', 'DDPEC_GT3', 'DDPEC_ST1',
                    'BVE_UNIT2', 'FREC_1_CCU', 'BTE_PUN1', 'LZ_LCRA', 'BVE_UNIT3', 'BTE_PUN2', 'TEN_CT1_STG',
                    'CHE_LYD2', 'PSG_PSG_ST1', 'CHE_LYD', 'TXCTY_CTC', 'TXCTY_ST', 'CTL_ST_101', 'WND_WHITNEY',
                    'LZ_HOUSTON', 'LZ_NORTH', 'CTL_GT_102', 'HB_HOUSTON', 'CHEDPW_GT2', 'CCEC_GT2', 'DDPEC_GT1'}
    raw_data = raw_data[raw_data['Settlement_Point'].isin(unique_nodes)]

    raw_data['Shadow_Price'] = pd.to_numeric(raw_data['Shadow_Price'], errors='coerce')
    raw_data['Shift_Factor'] = pd.to_numeric(raw_data['Shift_Factor'], errors='coerce')

    res = defaultdict(lambda: defaultdict(list))

    for _, row in raw_data.iterrows():
        settlement = row['Settlement_Point']
        full_date = row['SCED_Time_Stamp']
        peak_type = row['PeakType']
        constraint = row['Constraint_Name']
        contingency = row['Contingency_Name']
        shadowShift = float(row['Shadow_Price']) * float(row['Shift_Factor'])

        res[settlement][full_date].append((contingency, constraint, peak_type, float(row['Shift_Factor']), shadowShift))

    return res


def findDesired(mapping: Dict, row) -> Tuple[str, str, str, str]:
    """
    Given a DataFrame row, this helper method searches a mapping for the shadowPrice
    and facilityType corresponding to certain entries in the row.

    Inputs:
        - mapping: A dictionary mapping facilityNames to a tuple of contingency,
                   shadowPrice and facilityType.

    Output:
        - A tuple of shadowPrice, facilityType, peak_type, and facilityName, if found.
          Blank strings otherwise.
    """

    row_constraint = row['Constraint_Name']
    row_contingency = row['Contingency_Name']

    for facilityName in mapping:
        # Parse the facilityName string, since row_constraint can be a substring.
        if row_constraint in facilityName:
            for contingency, shadow, fac_type, peak_type in mapping[facilityName]:
                # Contingencies must match
                if row_contingency == contingency:
                    return shadow, fac_type, peak_type, facilityName

    return "", "", "", ""


def convert_csv(df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
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
    peakTypes = []
    facilityNames = []

    if next_hour not in mapping[df_date]:
        shadowPrices = [""] * len(filtered_df)
        facilityTypes = [""] * len(filtered_df)
        peakTypes = [""] * len(filtered_df)
        facilityNames = [""] * len(filtered_df)

    else:
        facilityMapping = mapping[df_date][next_hour]
        for _, row in filtered_df.iterrows():
            shadow, facType, peakType, facName = findDesired(facilityMapping, row)
            shadowPrices.append(shadow)
            facilityTypes.append(facType)
            peakTypes.append(peakType)
            facilityNames.append(facName)

    # Add in the new columns and return the resulting DataFrame
    filtered_df.insert(1, 'Hour_Ending', hourEnding)
    filtered_df.insert(2, "PeakType", peakTypes)
    filtered_df['Shadow_Price'] = shadowPrices
    filtered_df['Facility_Type'] = facilityTypes
    filtered_df['Constraint_Name'] = facilityNames

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
    lower_bound = datetime.strptime(latest_date + "/" + str(year), "%m/%d/%Y") - timedelta(days=days_back)
    new_data = dict(process_mapping(str(lower_bound.date()), "today"))
    mapping.update(new_data)
    mapping["Latest Date Queried"] = datetime.now().strftime("%m/%d")

    with open(json_path, "w") as file:
        json.dump(mapping, file)

"""
Now that we have the mapping, we can begin converting and aggregating the data.
"""
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
    merged_df['Shadow_Price'] = pd.to_numeric(merged_df['Shadow_Price'], errors='coerce')
    merged_df = merged_df[merged_df['Shadow_Price'] > 0]
    merged_df = merged_df.drop_duplicates(subset=['SCED_Time_Stamp', 'Hour_Ending',
                                                  'Contingency_Name', 'Settlement_Point'], keep='last')
        
    # Update the current summary JSON
    try:
        with open(json_summary) as js_sum:
            existing_sum = json.load(js_sum)
        
    except json.JSONDecodeError:
        existing_sum = {}

    new_processed = post_process(merged_df)

    for node in new_processed:
        if node not in existing_sum:
            existing_sum[node] = new_processed[node]
        
        else:
            for date in new_processed[node]:
                if date not in existing_sum[node]:
                    existing_sum[node][date] = new_processed[node][date]

    with open(json_summary, "w") as json_sum:
        json_sum.write(json.dumps(existing_sum))
        
    merged_df.to_csv(output_path, index=False)

# Otherwise, if the output CSV does exist, only update if requested year is the current year
elif year == datetime.now().year:
    current_data = pd.read_csv(output_path)
    latest_date = datetime.strptime(latest_date + "/" + str(year), "%m/%d/%Y")

    for zip_file in yearly_zip_files:
        zip_date = datetime.strptime(zip_file[34:36] + "/" + zip_file[36:38] + "/" + str(year), "%m/%d/%Y")

        # Convert all newly added CSVs since the last aggregation
        if zip_date >= latest_date - timedelta(days=days_back):
            with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
                df_csv = pd.read_csv(zip_path.open(zip_path.namelist()[0]))
                merge.append(convert_csv(df_csv, mapping))

    # Append the new data to the existing data
    merged_df = pd.DataFrame(pd.concat(merge, axis=0))
    combined_data = pd.concat([current_data, merged_df], axis=0)
    
    # Update the current summary 2023 JSON
    try:
        with open(json_summary) as js_sum:
            existing_sum = json.load(js_sum)
        
    except json.JSONDecodeError:
        existing_sum = {}

    new_processed = post_process(merged_df)

    for node in new_processed:
        if node not in existing_sum:
            existing_sum[node] = new_processed[node]
        
        else:
            for date in new_processed[node]:
                if date not in existing_sum[node]:
                    existing_sum[node][date] = new_processed[node][date]

    with open(json_summary, "w") as json_sum:
        json_sum.write(json.dumps(existing_sum))
    
    # Post-processing of the data.
    combined_data['Shadow_Price'] = pd.to_numeric(combined_data['Shadow_Price'], errors='coerce')
    combined_data = combined_data[combined_data['Shadow_Price'] > 0]
    combined_data = combined_data.drop_duplicates(subset=['SCED_Time_Stamp', 'Hour_Ending',
                                                          'Contingency_Name', 'Settlement_Point'], keep='last')

    combined_data.to_csv(output_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
