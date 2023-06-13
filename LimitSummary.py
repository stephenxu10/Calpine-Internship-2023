import csv
import time
import os
from typing import *
from utils import convert

"""
This Python script aims to summarize the limits from monthly CRR models of the non-thermal constraints across all years. 
Running this file will automatically generate the combined CSV file (by default in the data subfolder). Be sure
to adjust the global parameters below if necessary.

At a high level, the code uses File I/O and the built-in CSV reader to generate a dictionary mapping each year to 
its aggregate company data. This company data, also represented as a dictionary, maps each unique company from the 
current calendar year to its limits across all twelve months. Missing entries are represented as blanks. 

Note: This script will only work properly if it is ran from
\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates
due to the File I/O.

The output should generate after about ten seconds. File IO takes a bit of time.
"""

# Global parameters & variables
start_time = time.time()
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
path_base = "./../../06 - CRR/Monthly"
output_path = "./Data/CRR_NonThermalConstraints_Combined.csv"  # Relative file path of the outputted CSV.

filter_missing_entries = False  # Flag bit that filters out missing entries if set to true.

# Starting and ending years. By default, this encompasses all years with available data.
start_year = 2018
end_year = 2050

"""
Given the currently aggregated company_data dictionary, this helper method adds to that dictionary by 
including the data from the requested month.

Inputs:
    csv_file: The file path to the CSV file to be analyzed. It is guaranteed to be valid.
    company_data: A dictionary mapping each unique company name to it's limits across all twelve months.
    month: The requested month to analyze (e.g. 2019-02).
    
Output:
    Returns nothing, but inputs the input company_data parameter to add data from the month.
"""


def add_month_data(csv_file: str, company_data: Dict[str, List[str]], month: int):
    with open(csv_file, 'r') as csv_file:
        # Open and read the CSV file
        reader = csv.reader(csv_file)

        # Skip the header
        next(reader)

        # Iterate through the rows and collect the data.
        for row in reader:
            key = row[0]  # The company name.

            # By default, initialize all new companies to a list of twelve empty entries
            if key not in company_data:
                company_data[key] = [""] * 12

            # Update the entry
            company_data[key][month - 1] = row[1]


"""
Main method of the script. Given an input year, this method accumulates all data across all twelve months 
and returns the result in a dictionary.

Input:
    - year: An integer representing the year to query. It is guaranteed to be between start_year and end_year.
    
Output:
    Collects the data into a dictionary that maps each unique company across the entire year to a list of 
    twelve entries representing the limit in each month. Utilizes the above helper methods along the way.
"""


def accumulate_year(year: int) -> Dict[str, List[str]]:
    # Perform some File I/O to access the CSV files.
    new_base = path_base + "/" + str(year) if year != 2019 else path_base + "/" + str(year) + "/999 - Month"
    company_data = {}

    for month in range(1, 13):
        month_dir = new_base + "/" + str(year) + "-" + convert(month) + "/Network Model"

        if os.path.isdir(month_dir):
            csv_file = month_dir + "/" + str(year) + "." + months[
                month - 1] + '.Monthly.Auction.Non-ThermalConstraints.csv'

            csv_alt = month_dir + "/" + "Common_Non_ThermalConstraints_" + str(year) + "." + months[
                month - 1] + ".Monthly.Auction_AUCTION_" \
                      + months[month - 1] + "_" + str(year) + ".CSV"

            # Only perform the analysis if the CSV file was successfully found.
            if os.path.isfile(csv_file):
                add_month_data(csv_file, company_data, month)

            elif os.path.isfile(csv_alt):
                add_month_data(csv_alt, company_data, month)

    return company_data


# Collect and write the data to the output_path. Map each year to its accumulated data in the aggregate_data dictionary.
aggregate_data = {}
for yr in range(start_year, end_year + 1):
    aggregate_data[yr] = accumulate_year(yr)

header_list = ["Year", "Month", "Name", "Limit"]

with open(output_path, 'w', newline="") as output_file:
    writer = csv.writer(output_file)
    writer.writerow(header_list)

    for yr in range(start_year, end_year + 1):
        for name in aggregate_data[yr]:
            for mth in range(12):
                # Write in the rows, filter out missing entries if specified.
                if not (aggregate_data[yr][name][mth] == "" and filter_missing_entries):
                    rw = [str(yr), months[mth], name, aggregate_data[yr][name][mth]]
                    writer.writerow(rw)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
