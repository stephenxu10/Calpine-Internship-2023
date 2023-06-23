import os
import zipfile
import time
import pandas as pd


"""
This Python script aims to aggregate all of the day-ahead market transactions across each year in the
MIS. Additionally, for ease of access, an additional column that gives the date is added 
for each entry in the final output file.

Note:
This script will only work properly if it is ran from
\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates
due to the File I/O.

The final output CSV is enormous (could be up to 10 million lines), please give this some time
to finish running.
"""

# Global parameters & variables
start_time = time.time()
year = 2019 # Aggregates the DAM data for this particular year
path_base = "\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS " + str(year) + "\\56_DPNOMASF"

# Relative file path of the outputted CSV.
output_path = "./../Data/MIS Aggregates/MIS_DAM_" + str(year) + ".csv"

# Grab the zip files across each year
yearly_zip_files = os.listdir(path_base)

def aggregate_zip(zip_path: str) -> pd.DataFrame:
    """
    Given the full file path to a zip file, this helper method unzips the contents and
    aggregates all relevant CSV files (contains _Gn_) into one large Pandas DataFrame while also
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
            csv_files = [f for f in zip_contents if "_Gn_" in f]
            date = csv_files[0][3:5] + "/" + csv_files[0][5: 7] + "/" + csv_files[0][7:11]
            
            # Add the date column into each DataFrame
            for csv_file in csv_files:
                with zip_ref.open(csv_file) as csv:
                    df_csv = pd.read_csv(csv)
                    df_csv['Date'] = [date] * len(df_csv)
                    merge.append(df_csv)
    
    # Ignore any invalid zip files, log the exception to the console.
    except zipfile.BadZipFile:
        print("Invalid/Bad zip file located at " + str(zip_path) + ". No action was taken. \n")
        return pd.DataFrame()

    return pd.DataFrame(pd.concat(merge, axis=0))


final_merge = []
for zip_file in yearly_zip_files:
    full_path = os.path.join(path_base, zip_file)

    final_merge.append(aggregate_zip(full_path))

final_merged_df = pd.concat(final_merge, axis=0)
final_merged_df = pd.DataFrame(final_merged_df)
final_merged_df.to_csv(output_path, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
