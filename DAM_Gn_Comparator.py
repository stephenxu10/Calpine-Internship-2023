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
first_date = "02/13/2022"  # Must be in MM/DD/YYYY format
first_hour = 16  # Must be between 1 and 24 (inclusive)
second_date = "06/14/2023"  # Must be in MM/DD/YYYY format
second_hour = 16  # Must be between 1 and 24 (inclusive)

# Flag that determines if the script should generate a text file summary in addition to the CSV
text_flag = True

start_time = time.time()
path_base = "\\\\Pzpwuplancli01\\Uplan\\ERCOT"
output_txt = f"./Data/MIS Gen_CIM Comparisons/" \
              f"{first_date.replace('/', '')}_{first_hour}_{second_date.replace('/', '')}_{second_hour}.txt"

output_csv = f"./Data/MIS Gen_CIM Comparisons/" \
              f"{first_date.replace('/', '')}_{first_hour}_{second_date.replace('/', '')}_{second_hour}.csv"

success = True

"""
Simple helper method to find and return the entry in a list of strings that contains a
certain pattern.
"""
def findMatch(names: List[str], pattern: str):
    for entry in names:
        if pattern in entry:
            return entry

    return None


"""
Finds and returns the Pandas DataFrame giving the generator mapping data at a specific date and time. Returns nothing
and outputs an error message if the input parameters are invalid, or if the desired file could not be located.

Inputs:
    - date: A string in the "MM/DD/YYYY" format.
    - hour: An integer from 1 to 24, inclusive.

Output:
    - The Pandas DataFrame of the CIM generator data at this specific time.
"""
def find_generator_data(date: str, hour: int) -> Union[pd.DataFrame, None]:
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


"""
Given two DataFrames of generator data, this method compares the generator names and 
service status and outputs a tuple of four sets that give the following information:
    - which (name, status) pairs are the same from the first date to second date.
    - which generator statuses have changed from first date to second date.
    - which generators are unique to the first & second DataFrames

Inputs:
    - df1, df2: The two Pandas DataFrames that give generator data.
                
Output:
    - sharedPairs - the set that gives (name, status) pairs that are the same from date1 to date2
    - uniqueFirst, uniqueSecond - the generators unique to the first, second DataFrames
    - changed - (name, int) pairs that indicate change from date1 to date2.
        - 0: changed from In-Service to Out-Of-Service
        - 1: changed from Out-Of-Service to In-Service
"""
def compare_data(df1: pd.DataFrame, df2: pd.DataFrame):
    first_statuses = dict(zip(df1[df1.columns[5]], df1[df1.columns[6]]))
    second_statuses = dict(zip(df2[df2.columns[5]], df2[df1.columns[6]]))

    sharedPairs = set()
    uniqueFirst = set()
    uniqueSecond = set()
    changed = set()

    for gen_name in first_statuses:
        if gen_name in second_statuses:
            if first_statuses[gen_name] == second_statuses[gen_name]:
                sharedPairs.add((gen_name, first_statuses[gen_name]))
            else:
                changed.add((gen_name, 0 if first_statuses[gen_name] == 'In-Service' else 1))

        else:
            uniqueFirst.add((gen_name, first_statuses[gen_name]))

    for remaining in second_statuses:
        if remaining not in first_statuses:
            uniqueSecond.add((remaining, second_statuses[remaining]))

    return sharedPairs, uniqueFirst, uniqueSecond, changed


"""
Combines the summary results into a new Pandas DataFrame.
"""
def write_results(share, un_first, un_second, changes, date1, date2) -> pd.DataFrame:
    gen_names = []
    first_statuses = []
    second_statuses = []

    for unit, stat in changes:
        gen_names.append(unit)
        first_statuses.append("In-Service" if stat == 0 else "Out-Of-Service")
        second_statuses.append("Out-Of-Service" if stat == 0 else "In-Service")

    gen_names.append(" ")
    first_statuses.append(" ")
    second_statuses.append(" ")

    for unit, stat in un_first:
        gen_names.append(unit)
        first_statuses.append(stat)
        second_statuses.append("")

    gen_names.append(" ")
    first_statuses.append(" ")
    second_statuses.append(" ")

    for unit, stat in un_second:
        gen_names.append(unit)
        first_statuses.append("")
        second_statuses.append(stat)

    gen_names.append(" ")
    first_statuses.append(" ")
    second_statuses.append(" ")

    for unit, stat in share:
        gen_names.append(unit)
        first_statuses.append(stat)
        second_statuses.append(stat)

    result = pd.DataFrame()
    result['Generator Name '] = gen_names
    result[f" {date1} Status "] = first_statuses
    result[f" {date2} Status "] = second_statuses

    return result


if __name__ == "__main__":
    df_first = find_generator_data(first_date, first_hour)
    df_second = find_generator_data(second_date, second_hour)

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
            shared, first, second, change = compare_data(df_first, df_second)

            df_csv = write_results(shared, first, second, change)
            df_csv.to_csv(output_csv, index=False)

            if text_flag:
                with open(output_txt, 'w') as file:
                    file.write(f"Comparison Results from {first_date}, Hour {first_hour} to {second_date}, Hour {second_hour}\n")
                    file.write("\n")

                    # Output the changed generators
                    header = f"The following generator statuses have changed from from {first_date}, Hour {first_hour} to {second_date}, Hour {second_hour}\n"
                    file.write(header)
                    file.write("=" * (len(header) + 1) + "\n")

                    for name, status in change:
                        if status == 0:
                            file.write(f"{name} has changed from In-Service to Out-Of-Service \n")
                        else:
                            file.write(f"{name} has changed from Out-Of-Service to In-Service \n")

                    file.write("\n")
                    # Output the unique to first
                    header = f"The following generator statuses are unique to {first_date}, Hour {first_hour}'s data.\n"
                    file.write(header)
                    file.write("=" * (len(header) + 1) + "\n")

                    for name, status in first:
                        file.write(name + " " + status + "\n")

                    file.write("\n")
                    # Output the unique to second
                    header = f"The following generator statuses are unique to {second_date}, Hour {second_hour}'s data.\n"
                    file.write(header)
                    file.write("=" * (len(header) + 1) + "\n")

                    for name, status in second:
                        file.write(name + " " + status + "\n")

                    file.write("\n")
                    # Output all the shared data
                    header = f"The following generator statuses are shared between the two input dates. \n"
                    file.write(header)
                    file.write("=" * (len(header) + 1) + "\n")

                    for name, status in shared:
                        file.write(name + " " + status + "\n")
        else:
            success = False

    end_time = time.time()
    execution_time = (end_time - start_time)
    print("Generation Complete." if success else "No Result Generated.")
    print(f"The script took {execution_time:.2f} seconds to run.")
