from io import StringIO
import zipfile
import json
import requests
import warnings
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import time
from datetime import timedelta, datetime, date
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
year = date.today().year

# How many days we look back
days_back = 2

# Flag that determines if we would like to update the RT_Constraint table as well.
table_flag = False

zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\130_SSPSF"
json_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/current_" + str(year) + "_web_data.json"
json_summary = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/processed_" + str(year) + "_summary.json"
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/RT_Summary_" + str(year) + ".csv"
credential_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"
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

        Each tuple (w, x, y, z) in res[a][b][c] gives
            - w: Contingency
            - x: ShadowPrice
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
        shadowPrice = row['SHADOWPRICE']
        facility_type = row['FACILITYTYPE']
        peak_type = row['PEAKTYPE']

        res[date][hour][facility_name].append((contingency, shadowPrice, facility_type, peak_type))

    return res


def post_process(existing_dict: Dict, raw_data: pd.DataFrame):
    """
    Post-processes some summary dataFrame data into a nested dictionary and updates
    a pre-existing dictionary with those entries. Returns the new updated dictionary.

    Inputs:
        - existing_dict: The existing dictionary of summary data. For reference, existing_dict[a][b]
        gives a list of 5-element tuples corresponding to
            - a: The Settlement Point (such as HB_North)
            - b: The full date in MM/DD/YYYY HH:MM:SS format

        Each tuple (v, w, x, y, z) in existing_dict[a][b] gives
            - v: The Contingency Name
            - w: The Constraint Name
            - x: The Peak Type
            - y: The Shift Factor
            - z: The 'ShadowShift' - the Shift Factor multiplied by the Shadow Price

        - raw_data: A DataFrame storing the data desired to be summarized.

    Output:
        - Nothing, but updates the existing dictionary with new entries in the raw data. Writes the
        updated dictionary as a JSON to json_summary.
    """
    raw_data = raw_data[raw_data['Settlement_Point'].isin(nodes)]

    raw_data['Shadow_Price'] = pd.to_numeric(raw_data['Shadow_Price'], errors='coerce')
    raw_data['Shift_Factor'] = pd.to_numeric(raw_data['Shift_Factor'], errors='coerce')

    res = defaultdict(lambda: defaultdict(list))

    # Convert the data in the DataFrame to the same format as existing_dict
    for _, row in raw_data.iterrows():
        settlement = row['Settlement_Point']
        full_date = row['SCED_Time_Stamp']
        peak_type = row['PeakType']
        constraint = row['Constraint_Name']
        contingency = row['Contingency_Name']
        shadowShift = float(row['Shadow_Price']) * float(row['Shift_Factor'])

        res[settlement][full_date].append((contingency, constraint, peak_type, float(row['Shift_Factor']), shadowShift))

    for node in res:
        if node not in existing_dict:
            existing_dict[node] = res[node]

        else:
            for date in res[node]:
                existing_dict[node][date] = res[node][date]

    with open(json_summary, "w") as json_sum:
        json_sum.write(json.dumps(existing_sum))


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

    except (json.JSONDecodeError, FileNotFoundError):
        existing_sum = {}

    post_process(existing_sum, merged_df)

    if table_flag:
        merged_df.to_csv(output_path, index=False)

# Otherwise, if the output CSV does exist, only update if requested year is the current year
elif year == datetime.now().year:
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

    # Update the current summary 2023 JSON
    try:
        with open(json_summary) as js_sum:
            existing_sum = json.load(js_sum)

    except json.JSONDecodeError:
        existing_sum = {}

    post_process(existing_sum, merged_df)
    # Post-processing of the data
    if table_flag:
        current_data = pd.read_csv(output_path)
        combined_data = pd.concat([current_data, merged_df], axis=0)
        combined_data['Shadow_Price'] = pd.to_numeric(combined_data['Shadow_Price'], errors='coerce')
        combined_data = combined_data[combined_data['Shadow_Price'] > 0]
        combined_data = combined_data.drop_duplicates(subset=['SCED_Time_Stamp', 'Hour_Ending',
                                                              'Contingency_Name', 'Settlement_Point'], keep='last')

        combined_data = combined_data.sort_values(by=['SCED_Time_Stamp', 'Hour_Ending'])
        combined_data.to_csv(output_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")