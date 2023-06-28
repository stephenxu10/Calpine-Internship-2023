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
output_path = "./../../Data/MIS Aggregates/MIS_DAM_Resource.csv"

# List of load names that we care about
resource_names = ["SNDOW_LD1", "SNDSW_LD1", "SNDSW_LD2", "SNDSW_LD3",
                  "SNDSW_LD5", "SNDSW_LD6", "SNDSW_LD7", "SNDSW_LD8", "SNDOW_LD1"]

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
