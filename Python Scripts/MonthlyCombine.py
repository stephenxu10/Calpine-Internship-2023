import os
import time
from typing import *
import pandas as pd
from utils import convert, getSourceSinks


"""
This Python script aims to aggregate the historical Commercial_CreditCoefficient CSV files into one large
CSV file. Additionally, two columns are generated: a 'Path' column that stores the concatenated source, sink,
and TimeOfUse, and a 'FileName' column storing the document where the data originated from. Finally, we
do some filtering to only include entries with paths from

https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?

Note: This script will only work properly if it is ran from
\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates
due to the File I/O.

The final output file is located in the Data subfolder. It should take about 2 minutes to produce.
"""

# Global parameters & variables
start_time = time.time()
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
path_base = "./../../../06 - CRR/Monthly"

# Starting and ending years. By default, this encompasses all years with available data.
start_year = 2018
end_year = 2050

output_path = "./../Data/Commercial_CreditCoefficient_Combined.csv"  # Relative file path of the outputted CSV.
sourceSinks = getSourceSinks()

def collect_csv(yr: int, csv_files: List[str]):
    """
    Helper method that performs File I/O to append the list of all valid Commercial_CreditCoefficient
    CSV file paths to the existing csv_files list. Returns nothing.

    Inputs:
        - yr: The year to perform the search on
        - csv_files: The existing list of CSV (relative) file paths
    """

    # Perform some File I/O to access the CSV files.
    new_base = path_base + "/" + str(yr) if yr != 2019 else path_base + "/" + str(yr) + "/999 - Month"

    for month in range(1, 13):
        month_dir = new_base + "/" + str(yr) + "-" + convert(month) + "/Network Model"
        csv_file = month_dir + "/" + "Common_CreditCoefficient_" + str(yr) + "." + months[
            month - 1] + ".Monthly.Auction_AUCTION.CSV"

        if os.path.isfile(csv_file):
            csv_files.append(csv_file)


# Build the list of all CSV file paths across all years
CSV_list = []
for year in range(start_year, end_year + 1):
    collect_csv(year, CSV_list)

# Merge the CSVs through Pandas
merge = []
for f in CSV_list:
    merge.append(pd.read_csv(f))

merged_df = pd.concat(merge, axis=0)

# Append the new columns
paths = []
fileNames = []
for index, row in merged_df.iterrows():
    paths.append(row['Source'] + " + " + row['Sink'] + " + " + row['TimeOfUse'])
    date = row['StartDate']
    fileNames.append(date[len(date) - 4:] + "." + months[int(date[:2]) - 1])

merged_df.insert(0, 'Path', paths)
merged_df['FileName'] = fileNames

# Perform the filtering operations
filtered = merged_df[merged_df['Source'].isin(sourceSinks)]

finalFiltered = []
for row in filtered.iterrows():
    if row[1][3] in sourceSinks[row[1][2]]:
        finalFiltered.append(row[1])

finalFiltered = pd.DataFrame(finalFiltered)
finalFiltered.to_csv(output_path, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
