import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
import requests
import time
from io import StringIO
from typing import Union

"""
This Python script aims to aggregate Generic Transmission Constraints across all years. Furthermore,
the script queries Yes Energy to accumulate, filter, and reshape the raw data.

Running this script will append the current day's data to "./../../Data/GTC Aggregates/GTC_Aggregator.csv".
Should finish running in about 10 seconds.
"""

# Global Variables and Parameters.
start_time = time.time()
min_year = 2019
max_year = 2050

desired_names = [
    "RV_RH",
    "WESTEX",
    "VALEXP",
    "N_TO_H",
    "PNHNDL",
    "NELRIO",
    "NE_LOB",
    "MCCAMY",
    "VALIMP",
    "EASTEX",
]

ercot_root = "\\\\Pzpwuplancli01\\Uplan\\ERCOT\\"
mapping_root = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\02 - Summary\\MappingDocument\\"
credential_path = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\credentials.txt"
final_output_path = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\Data\\GTC Aggregates\\GTC_Aggregator.csv"

with open(credential_path, "r") as credentials:
    my_auth = tuple(credentials.read().split())


def extract_date(file_path: str) -> Union[datetime, str]:
    """
    Simple helper method to extract the date from a file path string
    in MM/DD format.

    Input:
        - file_path: A string containing 'Generic_Constraints_' giving the file path
        to a day's GTC data.

    Output:
        - The date from the file path in MM/DD format.
    """
    idx = file_path.rfind("Generic_Constraints_")

    try:
        res = datetime.strptime(
            file_path[idx + 20: idx + 22] + "/" + file_path[idx + 22: idx + 24],
            "%m/%d",
        )
        return res

    except ValueError:
        return ""


def aggregate_year(year: int, start: str = "01/01") -> pd.DataFrame:
    """
    Given an input year and a start date, this helper method accumulates and reformats
    all the Generic Constraint data beyond that start date.

    Inputs:
        - year: The year to aggregate
        - start: The starting date to aggregate from in MM/DD format. January 1st by default.

    Returns:
        - A Pandas DataFrame of the filtered data. The columns include: MarketDate, HourEnding,
          Name, and Limit
    """
    # Create the list of absolute paths to each Generic Constraint sheet
    start = datetime.strptime(start, "%m/%d")
    zip_base = ercot_root + f"MIS {year}\\" + "12_GTL\\"
    constraint_files = glob.glob(zip_base + "*.Generic_Constraints*")

    # Return nothing if no files are found
    if len(constraint_files) == 0:
        return pd.DataFrame()

    # Merge together the sheets, drop duplicates, and do some reformatting
    if start == "01/01":
        yearly_df_list = [pd.read_excel(file) for file in constraint_files]

    else:
        yearly_df_list = [
            pd.read_excel(file)
            for file in constraint_files
            if extract_date(file) != "" and extract_date(file) >= start
        ]

    merged_df = pd.concat(yearly_df_list, ignore_index=True)
    merged_df.drop_duplicates(subset=["Time"])
    merged_df.insert(1, "Marketdate", merged_df["Time"].dt.strftime("%m/%d/%Y"))
    merged_df.insert(2, "HourEnding", merged_df["Time"].dt.hour + 1)
    merged_df = merged_df.drop("Time", axis=1)
    merged_df = merged_df.melt(
        id_vars=["Marketdate", "HourEnding"], var_name="NAME", value_name="Limit"
    )

    print("Finished " + str(year))
    return merged_df


def filter_raw_data(raw_merged: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """
    Given a raw DataFrame and a starting date, this helper method filters it by reshaping the
    DataFrame, dropping unnecessary columns, and combining it with Yes Energy data.

    Inputs:
        - raw_merged: A raw DataFrame of aggregated data.
        - start_date: The day to start querying from in Yes Energy

    Output:
        - A finalized DataFrame in the desired format.
    """
    # First, reshape the raw data
    df_mapping = pd.read_csv(mapping_root + "GTC_Mapping.csv")
    DAM_data = raw_merged[raw_merged["NAME"].str.contains("DAM")].copy()
    DAM_data.loc[:, "NAME"] = DAM_data["NAME"].str.replace("\nDAM", "")
    RT_data = raw_merged[~raw_merged["NAME"].str.contains("DAM")]

    RT_data.columns = ["MARKETDAY", "HOURENDING", "Name", "Operating Limit"]
    DAM_data.columns = ["MARKETDAY", "HOURENDING", "Name", "DAM Limit"]

    raw_merged = pd.merge(
        DAM_data, RT_data, on=["MARKETDAY", "HOURENDING", "Name"], how="inner"
    )
    raw_merged = pd.merge(
        raw_merged, df_mapping, how="inner", left_on=["Name"], right_on=["Name"]
    )
    raw_merged = raw_merged.drop("Name", axis=1)

    # Query Yes Energy to grab Object IDs for a set of names
    query_one = "https://services.yesenergy.com/PS/rest/timeseries/COMBINED_CONSTRAINT_LIMIT.csv"
    call_one = requests.get(query_one, auth=my_auth)
    df_constraint_mapping = pd.read_csv(StringIO(call_one.text))
    df_constraint_mapping = df_constraint_mapping[
        df_constraint_mapping["NAME"].isin(desired_names)
    ]

    url_string = [
        "COMBINED_CONSTRAINT_LIMIT:" + str(i) for i in df_constraint_mapping.OBJECTID
    ]
    url_string = ",".join(str(j) for j in url_string)

    # Query Yes Energy again using this URL string to grab the Real-Time Limits
    enddate = "today+1"
    query_two = f"https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate={start_date}&enddate={enddate}&items={url_string}"
    call_two = requests.get(query_two, auth=my_auth)
    df_rt = pd.read_csv(StringIO(call_two.text))

    # Drop unnecessary columns and do some reshaping
    columns_to_drop = ["DATETIME", "MONTH", "YEAR", "PEAKTYPE"]
    df_rt.drop(columns=columns_to_drop, inplace=True)

    df_rt.rename(
        columns=lambda col: col.split(" (COMBINED_CONSTRAINT_LIMIT)")[0], inplace=True
    )
    df_rt = pd.melt(
        df_rt,
        id_vars=["MARKETDAY", "HOURENDING"],
        var_name="NAME",
        value_name="RT Limit",
    )
    df_rt.fillna(9999, inplace=True)

    # Merge the RT Limits with the other limit data
    df_final = pd.merge(
        df_rt,
        raw_merged,
        how="inner",
        left_on=["MARKETDAY", "HOURENDING", "NAME"],
        right_on=["MARKETDAY", "HOURENDING", "NAME"],
    )
    df_final = df_final.drop_duplicates()
    df_final = df_final.replace(np.nan, 9999)

    return df_final


# If the output path does not exist, aggregate and convert all the data.
if not os.path.isfile(final_output_path):
    # Aggregate and process the raw data from the drive.
    raw_merged = pd.concat(
        [aggregate_year(year) for year in range(min_year, max_year + 1)], axis=0
    )
    df_final = filter_raw_data(raw_merged, "01/01/2019")
    df_final.to_csv(final_output_path, index=False)

# Otherwise, assume the Data has been recently updated.
else:
    current_data = pd.read_csv(final_output_path)
    current_year = datetime.now().year
    current_date = datetime.now().strftime("%m/%d")

    new_current = aggregate_year(current_year, current_date)
    new_current = filter_raw_data(new_current, "today")
    current_data = pd.concat([current_data, new_current], axis=0)
    current_data = current_data.drop_duplicates()
    current_data.to_csv(final_output_path, header=False, index=False)

# Output summary statistics
end_time = time.time()
execution_time = end_time - start_time
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
