import pandas as pd
import time
import os
import io
import glob
import warnings

warnings.simplefilter("ignore")

# Global parameters & variables
DATE = "2022-10-11"

start_time = time.time()
path_base = f"//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/02 - Input/Transmission/Planning SSWG Base case/{DATE}"
output_root = f"//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Bus Load Data Raw File Parsing/"
aggregate_path = f"//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Bus Load Data Raw File Parsing/ALL_YEARS_LOAD_DATA.csv"

dictionary_sheet = glob.glob(os.path.join(
    path_base, "Planning_Data_Dictionary*.xlsx"))[0]


def grab_raw_lines(raw_file_name: str, start_key: str, end_key: str):
    """
    Extracts lines from a file between start_key and end_key.

    Args:
    raw_file_name: Name of the raw file.
    start_key: Starting key to begin data extraction.
    end_key: Ending key to stop data extraction.

    Returns:
    Extracted data as a string.
    """
    data = ''
    full_path = os.path.join(path_base, raw_file_name)
    start_reading = False

    with open(full_path, 'r') as file:
        for line in file:
            if start_reading:
                data += line
            if start_key in line:
                start_reading = True
            if end_key in line:
                break

    return data


def parse_raw_file(raw_file_name: str) -> pd.DataFrame:
    """
    Parses a .raw file to obtain two DataFrames - one for bus data and the other for load data
    """
    try:
        delimiter_regex = r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)'
        # Obtain the Bus Data
        bus_raw_lines = grab_raw_lines(
            raw_file_name,
            "BEGIN BUS DATA",
            "END OF BUS DATA")
        bus_io = io.StringIO(bus_raw_lines)

        bus_df = pd.read_csv(bus_io, sep=delimiter_regex, engine='python')
        bus_df.rename(
            columns={
                bus_df.columns[0]: 'ID',
                bus_df.columns[1]: 'NAME'},
            inplace=True)
        bus_df = bus_df.dropna()
        bus_df['FILE NAME'] = raw_file_name

        # Obtain the Load Data
        load_raw_lines = grab_raw_lines(
            raw_file_name,
            "BEGIN LOAD DATA",
            "END OF LOAD DATA")

        load_io = io.StringIO(load_raw_lines)
        load_df = pd.read_csv(load_io, sep=delimiter_regex, engine='python')
        load_df.rename(columns={load_df.columns[0]: 'I'}, inplace=True)
        load_df = load_df.dropna()

        final_idx = raw_file_name.upper().index("_FINAL")
        load_df['FILE NAME'] = raw_file_name[:final_idx]

        load_df = load_df.merge(
            bus_df[['ID', 'NAME']], left_on='I', right_on='ID', how='left')

        load_df.rename(columns={'NAME': 'BUS NAME'}, inplace=True)
        load_df.drop('ID', axis=1, inplace=True)

        new_order = ['I', 'BUS NAME'] + \
            [col for col in load_df.columns if col not in ['I', 'BUS NAME']]
        load_df = load_df[new_order]

        return load_df
    
    except:
        print(f"An exception occurred while reading file name {raw_file_name}")
        return pd.DataFrame()


def group_names(load_df: pd.DataFrame):
    grouped_df = load_df.groupby("BUS NAME").agg({
        load_df.columns[0]: "first",
        "PL": "sum",
        "FILE NAME": "first"
    }).reset_index()

    grouped_df["I"] = grouped_df["I"].astype(int)
    grouped_df = grouped_df.sort_values(by="I")
    grouped_df.rename(columns={"PL": "Aggregate PL",
                      "I": "SSWG BUS NUMBER"}, inplace=True)

    grouped_df["BUS NAME"] = grouped_df["BUS NAME"].str[1:-1]

    return grouped_df


"""
Parse and aggregate all the raw files in the path_base directory.
"""
raw_file_names = [f for f in os.listdir(path_base) if f.endswith("raw") and "Peaker" not in f]
output_name = raw_file_names[2][:raw_file_names[2].index("_")]
df_aggregate = pd.concat([group_names(parse_raw_file(f))
                         for f in raw_file_names])

# Read in the Excel Sheet and merge some relevant columns into df_aggregate
df_data_dictionary = pd.read_excel(
    dictionary_sheet, sheet_name="Data Dictionary")

df_data_dictionary.columns = [col.replace("_", " ") for col in df_data_dictionary.columns]
merged_df = pd.merge(df_aggregate, df_data_dictionary[[
                     'SSWG BUS NUMBER', 'NMMS WEATHER ZONE', 'NMMS SETTLEMENT ZONE', 'PLANNING BUS COUNTY', 'NMMS STATION NAME']], on="SSWG BUS NUMBER", how='left')

merged_df = merged_df.reindex(columns=[merged_df.columns[1], merged_df.columns[0]] + list(merged_df.columns[2:]))
merged_df.to_csv(output_root + f"{output_name}_LOAD_DATA.csv", index=False)

# Aggregate together all years' Load Data into a big file
df_all_years = pd.concat([pd.read_csv(os.path.join(output_root, f)) for f in os.listdir(output_root) if "SSWG" in f])
df_all_years.to_csv(aggregate_path, index=False)

end_time = time.time()
execution_time = end_time - start_time
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")