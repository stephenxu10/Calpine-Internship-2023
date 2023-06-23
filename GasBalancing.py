import warnings
from datetime import date, timedelta
import pandas as pd
import urllib.parse
from typing import Union, Tuple, List
import requests
from io import StringIO

# Ignore warnings. Whatever.
warnings.simplefilter("ignore")

"""
Global Parameters and Values. Tweak as necessary.
"""
# Absolute Path to the mapping from Gas Pipeline Names to Gas Pipeline IDs.
mapping_path = "\\\\pzpwcmfs01\\CA\\1_Market Analysis\\Trading\\Desk - Natural Gas\\Pipeline_FilterValues.xlsx"
pipeline_mapping = {row['Gas Pipeline Name']: row['Gas Pipeline ID'] for _, row in
                    pd.read_excel(mapping_path).iterrows()}

south_central = "\\\\pzpwcmfs01\\CA\\1_Market Analysis\\Trading\\Desk - Natural Gas\\Copy Of SouthCentral Balance.xlsx"

# List of query items that we are interested in.
# TODO: Add column-id mapping.

columns = (27496, 2630, 27545, 27548, 27535, 27536, 13244, 17062, 4969, 10895, 4973, 4978, 17147, 17148, 17149,
           9566, 27498, 2629, 27546, 13245)

# Florence's API key.
api_key = "ba71a029-a6fd-40cc-b0b7-6e4eb905dc57"

# Dataset Number
dataset = 2359


def fetch_gas_data(pipeline: str, flow_point: str, days_behind: int, dataset=dataset, columns=columns) -> Union[
        pd.DataFrame, None]:
    """
    Given an input Gas Pipeline Name, Flow Point Name, and a specified date range, this helper method performs
    a query on the velocity suite API in order to extract the desired raw data.

    Inputs:
        - pipeline: A string storing the Gas Pipeline Name. Assumed to be a key in the pipeline mapping.
        - flow_point: A string storing the Flow Point Name.
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
        parsed_flow_point = urllib.parse.quote_plus(flow_point)
        lower_date = (date.today() - timedelta(days=days_behind)).strftime('%m-%d-%Y')

        # Build the URL - start with the base and the column numbers.
        url_base = f"https://api.velocitysuiteonline.com/v1/iq?dataset={dataset}&format=csv&"

        for col_num in columns:
            url_base += ("item=" + str(col_num) + "&")

        # Add in the filters.
        filter_endOfDay = "filter=W|10895||In|Yes&"
        filter_lowerDate = "filter=W|4969||gte|" + urllib.parse.quote_plus(lower_date) + "&"
        filter_pipeline = "filter=W|27496||In|" + str(pipeline_id) + "&"
        filter_flow = "filter=W|27535||In|" + parsed_flow_point + "&"

        filtered_url = f"{url_base}{filter_endOfDay}{filter_lowerDate}{filter_pipeline}{filter_flow}"

        # Add the API-key.
        final_url = f"{filtered_url}api_key={api_key}"

        # Make a request to the API using the final URL.
        r = requests.get(final_url, verify=False)

        if r.status_code == 200:
            return pd.read_csv(StringIO(r.text)).head()

        else:
            print("not found!")
            return

def extract_paths(excel_path: str) -> List[Tuple[str, str]]:
    """
    Given an absolute path to an Excel sheet, this helper method reads through the tabs and extracts
    a list of "paths" consisting of a (Gas Pipeline Name, Flow Point Name) tuple.

    Inputs:
        - excel_path: The absolute path to an Excel sheet.

    Output:
        - A list of tuples giving the set of all paths in that particular Excel sheet.
    """
    excel_file = pd.ExcelFile(excel_path)
    sheet_names = excel_file.sheet_names

    paths = []
    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=5)
        df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=5, header=1)
        columns = df.columns.tolist()

        if not df.empty:
            if 'Gas_x0020_Pipeline_x0020_Name' in columns:
                paths.append((df.loc[0, 'Gas_x0020_Pipeline_x0020_Name'], df.loc[0, 'Flow_x0020_Point_x0020_Name']))

            elif 'Gas_x0020_Pipeline_x0020_Name' in df.values.tolist()[0]:
                paths.append(df.loc[0, 'Gas_x0020_Pipeline_x0020_Name'], df.loc[0, 'Flow_x0020_Point_x0020_Name'])

    return paths


print(extract_paths(south_central))
# print(fetch_gas_data("Enable Gas Transmission LLC", "Rockcliff Ic St-1 Panol", 90))
