import csv
import os
from typing import Union
import pandas as pd

from utils import convert

"""
This Python script performs an aggregation on the private monthly auction files while also
appending the Path, Plant, and Size (MW) information from the NodeToPlant mapping as
separate columns.

Note:
This script will only work properly if it is ran from
\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates
due to the File I/O.

The resultant output is stored as a CSV file in the data subfolder. Runtime should be no longer than
a minute or two.
"""

# Global parameters & variables
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
path_base = "./../../06 - CRR/Monthly"
output_path = "./data/auction_combined.csv"  # Relative file path of the outputted CSV.

# Starting and ending years. By default, this encompasses all years with available data.
start_year = 2018
end_year = 2023

"""
Helper method that locates the Private_(year).(month).Monthly.Auction_AUCTION.CSV file and creates
a Pandas DataFrame that adds three additional columns to the original CSV:
    - Path: Gives the source + sink path
    - Plant: Gives the corresponding Power Plant
    - Size: Gives the capacity of that plant in MW

Inputs:
    - year: An integer between start_year and end_year
    - month: An integer in the interval [1, 12]

Output:
    - Returns this newly created DataFrame, or None if the CSV file was not located.
"""
def modifyCSV(year: int, month: int) -> Union[None, pd.DataFrame]:
    c_month = convert(month)
    year = str(year)
    paths = []
    plants = []
    sizes = []

    # 2019 is in a different directory - use a ternary expression
    new_base = path_base + "/" + year if year != "2019" else path_base + "/" + year + "/999 - Month"
    csv_dir = new_base + "/" + year + "-" + c_month + ("/Market Results" if year >= "2021" else "/Market Result")
    csv_file = csv_dir + "/Private_" + year + "." + months[month-1] + ".Monthly.Auction_AUCTION.CSV"

    if os.path.isfile(csv_file):
        df_auction = pd.read_csv(csv_file)

        for index, row in df_auction.iterrows():
            source = row['Source']
            sink = row['Sink']
            path = source + "+" + sink

            paths.append(path)

            if path in path_to_plant:
                plants.append(path_to_plant[path][0])
                sizes.append(path_to_plant[path][1])

            else:
                plants.append("")
                sizes.append("")

        df_auction.insert(0, 'Size (MW)', sizes)
        df_auction.insert(0, 'Plant', plants)
        df_auction.insert(0, 'Path', paths)
        return df_auction
    else:
        return None


# Begin by constructing a dictionary that maps each path to a tuple of its Plant and Size (MW).
path_to_plant = {}
nodeToPlantFile = "./../../06 - CRR/01 - General/Extracts/NodePlantMapping.CSV"

with open(nodeToPlantFile, "r") as mapping_file:
    reader = csv.reader(mapping_file)
    next(reader)

    for rw in reader:
        path_to_plant[rw[3]] = rw[1], rw[4]

# Read and aggregate the CSV files
merge = []
for yr in range(start_year, end_year + 1):
    for mth in range(1, 13):
        df = modifyCSV(yr, mth)

        if df is not None:
            merge.append(df)

merged_df = pd.concat(merge, axis=0)
merged_df = pd.DataFrame(merged_df)
merged_df.to_csv(output_path, index=False)

print("Generation Complete")
