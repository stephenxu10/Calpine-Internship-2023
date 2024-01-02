import pandas as pd
import time
import os
import io

# Global parameters & variables
start_time = time.time()
path_base = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/02 - Input/Transmission/Planning SSWG Base case/2023-10-12"

sample_file = "22SSWG_2024_WIN1_U3_Final_10092023.raw"

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


def parse_raw_file(raw_file_name: str):
    """
    Parses a .raw file to obtain two DataFrames - one for bus data and the other for load data
    """
    delimiter_regex = r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)'
    # Obtain the Bus Data
    bus_raw_lines = grab_raw_lines(raw_file_name, "BEGIN BUS DATA", "END OF BUS DATA")
    bus_io = io.StringIO(bus_raw_lines)

    bus_df = pd.read_csv(bus_io, sep=delimiter_regex, engine='python')
    bus_df.rename(columns={bus_df.columns[0]: 'ID', bus_df.columns[1]: 'NAME'}, inplace=True)
    bus_df = bus_df.dropna()
    bus_df['FILE NAME'] = raw_file_name
    print(bus_df.head())
    print("=" * 200)

    # Obtain the Load Data
    load_raw_lines = grab_raw_lines(raw_file_name, "BEGIN LOAD DATA", "END OF LOAD DATA")

    load_io = io.StringIO(load_raw_lines)
    load_df = pd.read_csv(load_io, sep=delimiter_regex, engine='python')
    load_df.rename(columns={load_df.columns[0]: 'I'}, inplace=True)
    load_df = load_df.dropna()
    load_df['FILE NAME'] = raw_file_name

    load_df = load_df.merge(bus_df[['ID', 'NAME']], left_on='I', right_on='ID', how='left')

    load_df.rename(columns={'NAME': 'BUS NAME'}, inplace=True)
    load_df.drop('ID', axis=1, inplace=True)

    new_order = ['I', 'BUS NAME'] + [col for col in load_df.columns if col not in ['I', 'BUS NAME']]
    load_df = load_df[new_order]
    print(load_df.head())

    return bus_df, load_df

bus_df, load_df = parse_raw_file(sample_file)