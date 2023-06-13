import csv
import time
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

The resultant output is stored as a CSV file in the data subfolder. As usual, assuming a consistent file structure, 
running this code will generate an updated output whenever new data is added.
"""

# Global parameters & variables
start_time = time.time()
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
path_base = "./../../06 - CRR/Monthly"

# Flag that groups together all matching Paths (within current year) by averaging ShadowPricePerMWH and BidPricePerMWH
grouping_by = False


# Relative file path of the outputted CSV., dependent on the flag
output_path = "./data/Private_auction_grouped.csv" if grouping_by else "./data/Private_auction_combined.csv"


# Starting and ending years. By default, this encompasses all years with available data.
start_year = 2019
end_year = 2023

# Begin by constructing a dictionary that maps each path to a tuple of its Plant and Size (MW).
path_to_plant = {}
nodeToPlantFile = "./../../06 - CRR/01 - General/Extracts/NodePlantMapping.CSV"

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
    - Returns this newly created DataFrame, or None if the CSV file was not located. No data appears to be
      available for 2016 and 2017.
"""
def modifyCSV(year: int, month: int) -> Union[None, pd.DataFrame]:
    # Initialize and convert all necessary variables
    c_month = convert(month)
    year = str(year)
    paths = []
    plants = []
    sizes = []

    # 2019 is in a different directory - use a ternary expression
    new_base = path_base + "/" + year if year != "2019" else path_base + "/" + year + "/999 - Month"
    csv_dir = new_base + "/" + year + "-" + c_month + ("/Market Results" if year >= "2021" else "/Market Result")
    csv_file = csv_dir + "/Private_" + year + "." + months[month-1] + ".Monthly.Auction_AUCTION.CSV"

    # Only perform the analysis if the CSV file was successfully found
    if os.path.isfile(csv_file):
        df_auction = pd.read_csv(csv_file)

        for index, row in df_auction.iterrows():
            source = row['Source']
            sink = row['Sink']
            path = source + "+" + sink

            # Generate the path and extract corresponding data from the path_to_plant mapping
            paths.append(path)

            if path in path_to_plant:
                plants.append(path_to_plant[path][0])
                sizes.append(path_to_plant[path][1])

            else:
                print("The Path: " + path + " was not found in the NodeToPlant mapping")
                plants.append("")
                sizes.append("")

        # Insert the new columns at the beginning of the output CSV
        df_auction.insert(0, 'Size (MW)', sizes)
        df_auction.insert(0, 'Plant', plants)
        df_auction.insert(0, 'Path', paths)
        
        if grouping_by:
            average_columns = ['ShadowPricePerMWH', 'BidPricePerMWH']
            grouped = df_auction.groupby(['Path', 'HedgeType'])[average_columns].mean().reset_index()
            df_auction = df_auction.drop(average_columns, axis=1)
            df_auction = df_auction.merge(grouped, on=['Path', 'HedgeType'], how='left')
            df_auction = df_auction.drop_duplicates(subset=['Path', 'HedgeType'], keep='first')

        return df_auction
    else:
        return None


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

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
