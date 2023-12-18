import pandas as pd
from pandas import to_datetime
import os
import glob
import numpy as np
from datetime import datetime, timedelta
import requests
import time
from io import StringIO
from typing import Union

"""
Error checking proecdure for the GTC_Aggregation Python script. Intended to run after GTC_Aggregation.py finishes.

This traverses through the raw DataFrame that the script
generates and checks for two things:
    - missing dates in the DataFrame
    - negative RT limits
These errors are written to the errorLog.txt file. We handle negative RT Limits by replacing the values with the
nearest positive RT Limit above.
"""

final_output_path = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\Data\\GTC Aggregates\\GTC_Aggregator.csv"
error_output_path = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\Data\\GTC Aggregates\\GTC_Error_Log.csv"
log_path = "./errorLog.txt"


def find_missing_dates(df, date_column):
    """
    Finds missing dates in a DataFrame's date column.

    Parameters:
    df (DataFrame): The DataFrame to search.
    date_column (str): The name of the date column.

    Returns:
    List: A list of missing dates.
    """
    # Convert the date column to datetime
    df[date_column] = to_datetime(df[date_column])

    # Generate a complete range of dates
    min_date = df[date_column].min()
    max_date = df[date_column].max()
    complete_dates = pd.date_range(start=min_date, end=max_date, freq='D')

    # Find missing dates
    missing_dates = complete_dates.difference(df[date_column])

    return missing_dates

def find_latest_positive(df: pd.DataFrame, row_index: int, limit_type: str) -> int:
    assert(df.loc[row_index, limit_type]) < 0

    curr_index = row_index
    while curr_index > 0 and df.loc[curr_index, limit_type] < 0:
        curr_index -= 1
    
    return df.loc[curr_index, limit_type]

def find_and_replace_negative_capacities(df: pd.DataFrame, limit_type: str):
    replacements = pd.DataFrame()
    names = []
    limit_types = []
    dates = []
    hourEnding = []
    original = []
    replaced = []

    for index, row in df.iterrows():
        rt_limit = row[limit_type]
        if rt_limit < 0:
            limit_types.append(limit_type)
            original.append(rt_limit)
            names.append(row["NAME"])
            dates.append(row["MARKETDAY"])
            hourEnding.append(row["HOURENDING"])

            replaced_value = find_latest_positive(df, index, limit_type)
            df.loc[index, limit_type] = replaced_value
            replaced.append(replaced_value)

    replacements["Name"] = names
    replacements["Limit Type"] = limit_types
    replacements["Date"] = dates
    replacements["HourEnding"] = hourEnding
    replacements["Original"] = original
    replacements["Replaced Value"] = replaced
    
    return replacements

def write_to_log(missing_dates, log_path):
    """
    Writes missing dates to a log file.

    Parameters:
    missing_dates (DatetimeIndex): Missing dates.
    log_path (str): Path to the log file.
    """
    with open(log_path, "w") as log_file:
        log_file.write("Missing Dates:\n")
        for date in missing_dates:
            log_file.write(f"{date.strftime('%Y-%m-%d')}\n")
        log_file.write("\n")

    
    log_file.close()


gtc_df = pd.read_csv(final_output_path)
missing_dates = find_missing_dates(gtc_df, "MARKETDAY")

replacements_rt = find_and_replace_negative_capacities(gtc_df, "RT Limit")
replacements_da = find_and_replace_negative_capacities(gtc_df, "DAM Limit")
write_to_log(missing_dates, log_path)

gtc_df.to_csv(final_output_path, index=False)
replacements = pd.concat([replacements_rt, replacements_da])
replacements = replacements.drop_duplicates()
replacements.to_csv(error_output_path, index=False)

