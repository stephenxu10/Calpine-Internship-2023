import os
import zipfile
import time
import pandas as pd
from typing import List

# Global parameters & variables
start_time = time.time()

lower_year = 2019
upper_year = 2023

# Relative file path of the outputted CSV.
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/MIS Aggregates/SCED_Reported_Load.csv"
resource_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/MIS Aggregation/resources_SCED.txt"

with open(resource_path, "r") as resource_file:
    resource_names = resource_file.read().split("\n")

def aggregate_zip(zip_path: str) -> pd.DataFrame:
    """
    Given the full file path to a zip file, this helper method unzips the contents and
    aggregates all relevant CSV files into one large Pandas DataFrame while also
    adding a new Date column.

    Inputs:
        - zip_path: The valid file path to a specific zip file

    Output:
        - A Pandas DataFrame containing the merged CSVs with the new column.
    """

    merge = []
    try:
        # Open the relevant CSV files and aggregate them through Pandas
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_contents = zip_ref.namelist()
            csv_files = [f for f in zip_contents if "60d_Load" in f]
            
            # Add the date column into each DataFrame
            for csv_file in csv_files:
                with zip_ref.open(csv_file) as csv:
                    df_csv = pd.read_csv(csv)
                    df_csv = df_csv[df_csv['Resource Name'].isin(resource_names)]
                    df_csv.insert(0, 'Interval', pd.to_datetime(df_csv['SCED Time Stamp']).dt.minute // 5 + 1)
                    df_csv.insert(0, 'HourEnding', pd.to_datetime(df_csv['SCED Time Stamp']).dt.hour + 1)
                    df_csv.insert(0, 'Date', pd.to_datetime(df_csv['SCED Time Stamp']).dt.strftime('%m/%d/%Y'))
                    df_csv.drop("SCED Time Stamp", axis=1)
                    merge.append(df_csv)

    # Ignore any invalid zip files, log the exception to the console.
    except zipfile.BadZipFile:
        print("Invalid/Bad zip file located at " + str(zip_path) + ". No action was taken. \n")
        return pd.DataFrame()

    return pd.DataFrame(pd.concat(merge, axis=0))


def aggregate_year(year: int) -> pd.DataFrame:
    path_base = "\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS " + str(year) + "\\46_6DSDR"
    
    # Grab the zip files across the input year
    yearly_zip_files = os.listdir(path_base)

    final_merge = []
    for zip_file in yearly_zip_files:
        full_path = os.path.join(path_base, zip_file)
        final_merge.append(aggregate_zip(full_path))
        print("Finished " + zip_file)
    
    final_merged_df = pd.concat(final_merge, axis=0)
    final_merged_df = pd.DataFrame(final_merged_df)
    return final_merged_df


merged = []
for yr in range(lower_year, upper_year + 1):
    merged.append(aggregate_year(yr))
    
final_merged = pd.concat(merged, axis=0)
final_merged.to_csv(output_path, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
