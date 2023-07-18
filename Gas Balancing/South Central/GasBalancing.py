import ast
import itertools
import os
import warnings
from datetime import date, timedelta
import pandas as pd
import urllib.parse
from typing import Union, Tuple, List, Set
import requests
from io import StringIO

"""
Blaine's original code: Y:\5_Trans Analysis\Models\Development\Python\Scripts\SouthCentralGasBalancePython.py
"""

# Ignore warnings.
warnings.simplefilter("ignore")

os.chdir("\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\Gas Balancing\\South Central")

"""
Global Parameters and Values. Tweak as necessary.
"""
# Absolute Path to the mapping from Gas Pipeline Names to Gas Pipeline IDs.
mapping_path = "\\\\pzpwcmfs01\\CA\\1_Market Analysis\\Trading\\Desk - Natural Gas\\Pipeline_FilterValues.xlsx"
pipeline_mapping = {row['Gas Pipeline Name']: row['Gas Pipeline ID'] for _, row in
                    pd.read_excel(mapping_path).iterrows()}

output_path = "./Aggregated_BalanceSheets.csv"

south_central = "\\\\pzpwcmfs01\\CA\\1_Market Analysis\\Trading\\Desk - Natural Gas\\Copy Of SouthCentral Balance.xlsx"

# Dataset Number
dataset = 2359
# The number of days we look back
days_back = 90

# Paths File
path_file = "./pathNames_" + str(dataset) + ".txt"

# List of query items that we are interested in.
columns = (27496, 2630, 27545, 27548, 27535, 27536, 13244, 17062, 4969, 10895, 4973, 4978, 17147, 17148, 17149,
           9566, 27498, 2629, 27546, 13245)

# Florence's API key.
api_key = "ba71a029-a6fd-40cc-b0b7-6e4eb905dc57"


def extract_paths(excel_path: str) -> List[Tuple[str, str]]:
    """
    Given an absolute path to an Excel sheet, this helper method reads through the tabs and extracts
    a list of "paths" consisting of a (Gas Pipeline Name, Flow Point Name) ordered pair. If the Excel sheet
    is very large, this method may take a while to extract all the paths.

    Inputs:
        - excel_path: The absolute path to an Excel sheet.

    Output:
        - A list of tuples giving the set of all paths in that particular Excel sheet.
    """

    # Load the Excel file
    excel_file = pd.ExcelFile(excel_path)

    # Get the names of all the sheets in the Excel file
    sheet_names = excel_file.sheet_names

    # Create an empty list to store the extracted paths
    paths = []

    # Iterate through each sheet in the Excel file
    for sheet_name in sheet_names:
        try:
            # Read the Gas Pipeline and Flow Point Name columns from the Excel sheet
            df = pd.read_excel(excel_file, sheet_name=sheet_name,
                               usecols=['Gas_x0020_Pipeline_x0020_Name', 'Flow_x0020_Point_x0020_Name'])

            # Extract the non-null and unique pairs as tuples
            paths.extend(list(df[['Gas_x0020_Pipeline_x0020_Name',
                                  'Flow_x0020_Point_x0020_Name']].dropna().drop_duplicates().to_records(index=False)))
        except (KeyError, ValueError):
            # Ignore any exceptions raised due to missing or invalid columns
            pass

        try:
            # Some of the Excel sheets have the columns on the second row - try again with this approach.
            df_alt = pd.read_excel(excel_file, sheet_name=sheet_name, header=1,
                                   usecols=['Gas_x0020_Pipeline_x0020_Name', 'Flow_x0020_Point_x0020_Name'])

            # Extract the non-null and unique records as tuples
            paths.extend(list(df_alt[['Gas_x0020_Pipeline_x0020_Name',
                                      'Flow_x0020_Point_x0020_Name']].dropna().drop_duplicates().to_records(index=False)))

        except (KeyError, ValueError):
            # Ignore any exceptions raised due to missing or invalid columns
            pass

    # Return the list of extracted paths
    return paths


def fetch_gas_data(pipeline: str, flow_point: Set[str], days_behind: int, dataset=dataset, columns=columns) -> Union[
    pd.DataFrame, None]:
    """
    Given an input Gas Pipeline Name, Flow Point Names, and a specified date range, this helper method performs
    a query on the velocity suite API in order to extract the desired raw data.

    Inputs:
        - pipeline: A string storing the Gas Pipeline Name. Assumed to be a key in the pipeline mapping.
        - flow_point: A set of strings storing all possible flow point names.
        - days_behind: An integer giving the number of days to look behind.
        - dataset: The dataset number to query from. By default, this is 2359.
        - columns: The columns to include in the output DataFrame. The column numbers are stored by default above
                    as a global parameter.
    Output:
        A Pandas DataFrame storing the requested data. An error message will print if an invalid argument is given.
    """

    if pipeline not in pipeline_mapping:
        print("Invalid argument. The requested pipeline does not appear to be in the mapping. Check for typos.")
        return

    else:
        pipeline_id = pipeline_mapping[pipeline]
        parsed_flow_points = ""

        for fp in flow_point:
            parsed_flow_points += urllib.parse.quote_plus(fp) + "|"

        lower_date = (date.today() - timedelta(days=days_behind)).strftime('%m-%d-%Y')

        # Build the URL - start with the base and the column numbers.
        url_base = f"https://api.velocitysuiteonline.com/v1/iq?dataset={dataset}&format=csv&"

        for col_num in columns:
            url_base += ("item=" + str(col_num) + "&")

        # Add in the filters.
        filter_endOfDay = "filter=W|10895||In|Yes&"
        filter_lowerDate = "filter=W|4969||gte|" + urllib.parse.quote_plus(lower_date) + "&"
        filter_pipeline = "filter=W|27496||In|" + str(pipeline_id) + "&"
        filter_flow = "filter=W|27535||In|" + parsed_flow_points + "&"

        filtered_url = f"{url_base}{filter_endOfDay}{filter_lowerDate}{filter_pipeline}{filter_flow}"

        # Add the API-key.
        final_url = f"{filtered_url}api_key={api_key}"

        # Make a request to the API using the final URL.
        r = requests.get(final_url, verify=False)

        if r.status_code == 200:
            return pd.read_csv(StringIO(r.text))

        else:
            print("not found!")
            return


def find_recent_capacities(overall_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given the overall merged DataFrame, this method computes the most recent capacity
    for each unique flow point name in the DataFrame.

    Currently a rather lazy (but concise!) approach.

    Input:
        - overall_df: The aggregated DataFrame storing all the Gas Balance data from velocity suite.

    Output:
        - filtered_df: A DataFrame mapping each flow point name to its most recent capacity.
    """
    filtered_df = overall_df.drop_duplicates(subset=['Gas Pipeline Name', 'Flow Point Name', 'Flow Direction'], keep="first")
    return filtered_df[['Flow Point Name', 'Flow Direction', 'Sum Scheduled Quantity Dth']]


paths = []
# Extract the (pipeline, flow-point) paths from the path text file, or create it if it does not already exist.
if os.path.isfile(path_file):
    # Read in the paths from the pre-existing path file
    with open(path_file, 'r') as pth_file:
        for path in pth_file.read().split("\n"):
            paths.append((path.split(", ")[0], path.split(", ")[1]))

else:
    paths = extract_paths(south_central)

    # Create and write in the paths otherwise.
    with open(path_file, 'w') as pth_file:
        for path in paths:
            pth_file.write(path[0] + ", " + path[1] + "\n")
    
    pth_file.close()

merge = []

# Group together the paths by the gas pipeline.
processed_paths = {key: set(val for _, val in group) for key, group in itertools.groupby(paths, key=lambda x: x[0])}

# Fetch the data for each group of gas pipeline
for key in processed_paths:
    merge.append(fetch_gas_data(key, processed_paths[key], days_back))

# Merge together the queried data and output it to a CSV
merged_df = pd.concat(merge, axis=0)
merged_df.to_csv(output_path, index=False)

# find_recent_capacities(merged_df).to_csv(output_path, sheet_name="Capacity", index=False)

