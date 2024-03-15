import os
import zipfile
import time
from datetime import datetime
import re
import pandas as pd

from typing import Union, List

"""
This Python tool aims to compare the generator data from two unique operating dates from
historical Day-Ahead Market data. Given the date and hour parameters for each file, the script locates
and reads each CSV (if valid), and outputs the comparison results to a new text file in the 
./Data/MIS Gen_CIM Comparisons directory.

Be sure to change the global date/hour parameters immediately below to compare the desired files.
"""

# Global parameters and variables. For convenience, the first_date & time should be before the second_date & time.
first_date = "03/14/2019"  # Must be in MM/DD/YYYY format
first_hour = 16  # Must be between 1 and 24 (inclusive)
second_date = "06/16/2023"  # Must be in MM/DD/YYYY format
second_hour = 16  # Must be between 1 and 24 (inclusive)

start_time = time.time()
path_base = "\\\\Pzpwuplancli01\\Uplan\\ERCOT"

output_txt = f"./../Data/MIS Gen_CIM Comparisons/" \
              f"{first_date.replace('/', '')}_{first_hour}_{second_date.replace('/', '')}_{second_hour}.txt"

output_csv = f"./../Data/MIS Gen_CIM Comparisons/" \
              f"{first_date.replace('/', '')}_{first_hour}_{second_date.replace('/', '')}_{second_hour}.csv"

success = True

def findMatch(names: List[str], pattern: str):
    """
    Simple helper method to find and return the entry in a list of strings that contains a
    certain pattern.
    """
    for entry in names:
        if pattern in entry:
            return entry

    return None


def find_generator_data(date: str, hour: int) -> Union[pd.DataFrame, None]:
    """
    Finds and returns the Pandas DataFrame giving the generator mapping data at a specific date and time. Returns nothing
    and outputs an error message if the input parameters are invalid, or if the desired file could not be located.

    Inputs:
        - date: A string in the "MM/DD/YYYY" format.
        - hour: An integer from 1 to 24, inclusive.

    Output:
        - The Pandas DataFrame of the CIM generator data at this specific time.
    """

    pattern = r"\d{2}/\d{2}/\d{4}"

    if re.match(pattern, date):
        if 1 <= hour <= 24:
            date_replaced = date.replace("/", "")
            year = date_replaced[-4:]

            year_base = os.path.join(path_base, "MIS " + year + "\\" + "56_DPNOMASF")
            if os.path.isdir(year_base):
                yearly_zip_files = os.listdir(year_base)
                zip_name = findMatch(yearly_zip_files, date_replaced)

                if zip_name is None:
                    print(f"The data for the input date ({date}) was not found. Check the validity of the date.")
                    return

                else:
                    zip_path = os.path.join(year_base, zip_name)
                    try:
                        with zipfile.ZipFile(zip_path, "r") as zip_ref:
                            csv_file = findMatch(zip_ref.namelist(), date_replaced + "_Gn_" + str(hour).zfill(3))

                            if csv_file is None:
                                print("Unable to find data for an input date & hour pairing. Perhaps the data is missing.")
                                return

                            with zip_ref.open(csv_file) as csv:
                                return pd.read_csv(csv)

                    # Ignore any invalid zip files, log the exception to the console.
                    except zipfile.BadZipFile:
                        print("Invalid/Bad zip file located at " + str(zip_path) + ". No action was taken. \n")
                        return

            else:
                print(f"The MIS yearly data was not found/not available for {date}, Hour {hour}")
                return

        else:
            print(f"The input hour ({hour}) is not within bounds.")
            return

    else:
        print(f"The input date ({date}) is not in the desired MM/DD/YYYY format.")
        return

def compare_statuses(df1: pd.DataFrame, df2: pd.DataFrame, date1: str, date2: str):
    """
    Inputs:
        - df1, df2: The two Pandas DataFrames that give line data.

    Output:
        - A DataFrame showing the changes in their statuses
    """
    status_key = "Generator"
    df1.columns = [col.strip() for col in df1.columns]
    df2.columns = [col.strip() for col in df2.columns]

    df1 = df1[['Generator Name', f'{status_key} Status']]
    df2 = df2[['Generator Name', f'{status_key} Status']]

    merged_df = pd.merge(df1, df2, on='Generator Name', suffixes=('_1', '_2'), how='outer')

    def describe_change(row):
        status_1 = row[f'{status_key} Status_1']
        status_2 = row[f'{status_key} Status_2']
        if pd.notnull(status_1) and pd.notnull(status_2):
            if status_1 == status_2:
                if status_1 == "In-Service":
                    return "No Change"
                else:
                    return "Still Out-Of-Service"
            else:
                return "Changed"
        elif pd.isnull(status_1):
            return "Missing-In-First"
        elif pd.isnull(status_2):
            return "Missing-In-Second"

    merged_df['Description'] = merged_df.apply(describe_change, axis=1)
    merged_df.rename(columns={ f'{status_key} Status_1': f"{date1} Status",  f'{status_key} Status_2': f"{date2} Status"}, inplace=True)

    merged_df = merged_df[merged_df['Description'] != "No Change"]
    merged_df = merged_df.sort_values(by=['Description'])
    merged_df.fillna('', inplace=True)

    return merged_df    


if __name__ == "__main__":
    df_first = find_generator_data(first_date, first_hour)
    df_second = find_generator_data(second_date, second_hour)

    # Perform some quick checks on the input dates.
    if first_date == second_date and first_hour == second_hour:
        print("The input dates & times are exactly the same. No comparisons can be done.")
        success = False

    elif first_date == second_date and first_hour > second_hour:
        print("Switch up the hours. The first hour should come before the second hour here.")
        success = False

    elif datetime.strptime(first_date, "%m/%d/%Y") > datetime.strptime(second_date, "%m/%d/%Y"):
        print("Please make sure that the first date & time comes before the second date & time. Switch up the variables.")
        success = False

    else:
        if df_first is not None and df_second is not None:
            df_csv = compare_statuses(df_first, df_second, first_date, second_date)
            df_csv.to_csv(output_csv, index=False)
        else:
            success = False

    end_time = time.time()
    execution_time = (end_time - start_time)
    print("Generation Complete." if success else "No Result Generated.")
    print(f"The script took {execution_time:.2f} seconds to run.")
