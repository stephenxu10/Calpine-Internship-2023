import os
import zipfile
import time
import pandas as pd
from typing import List


# Global parameters & variables
start_time = time.time()

lower_year = 2019
upper_year = 2050

# Relative file path of the outputted CSV.
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/HRUC Aggregates/HRUC_Aggregate.csv"


def process_zip(zip_path: str) -> pd.DataFrame:
    """
    Given the full file path to a zip file, this helper method unzips the contents and
    reads the CSV inside into one Pandas DataFrame while also processing some of its
    columns.

    Inputs:
        - zip_path: The valid file path to a specific zip file

    Output:
        - A Pandas DataFrame containing the merged CSVs with the new column.
    """
    res = pd.DataFrame()
    try:
        # Open the relevant CSV files and aggregate them through Pandas
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_contents = zip_ref.namelist()

            """
            Process the CSV within by:
                - Converting the HourEnding column to an integer
                - Add new 'Day', 'Month', 'Year' columns derived from RUCTimeStamp
            """
            for csv_file in zip_contents:
                with zip_ref.open(csv_file) as csv:
                    res = pd.read_csv(csv)

                    res['RUCTimeStamp'] = res['RUCTimeStamp'].str.strip()
                    res['HourEnding'] = res['HourEnding'].str.strip()
                    res['RUCTimeStamp'] = pd.to_datetime(res['RUCTimeStamp'], format='%m/%d/%Y %H:%M:%S')
                    res['HourEnding'] = res['HourEnding'].str[:2].astype(int)

                    # Inserting new columns for Day, Month, and Year
                    res.insert(1, "Year", res['RUCTimeStamp'].dt.year)
                    res.insert(2, "Month", res['RUCTimeStamp'].dt.month)
                    res.insert(3, "Day", res['RUCTimeStamp'].dt.day)

    # Ignore any invalid zip files, log the exception to the console.
    except zipfile.BadZipFile:
        print("Invalid/Bad zip file located at " + str(zip_path) + ". No action was taken. \n")
        return pd.DataFrame()

    return res

def aggregate_year(year: int) -> pd.DataFrame:
    path_base = "\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS " + str(year) + "\\91_HRCODR"

    if os.path.exists(path_base):
        # Grab the zip files across the input year
        yearly_zip_files = os.listdir(path_base)

        final_merge = []
        for zip_file in yearly_zip_files:
            full_path = os.path.join(path_base, zip_file)
            year_df = process_zip(full_path)
            
            if not year_df.empty:
                final_merge.append(year_df)
        
        final_merged_df = pd.concat(final_merge, axis=0)
        final_merged_df = pd.DataFrame(final_merged_df)

        print("Finished " + str(year))
        return final_merged_df

merged = []
for yr in range(lower_year, upper_year + 1):
    merged.append(aggregate_year(yr))
    
final_merged = pd.concat(merged, axis=0)
final_merged = final_merged.drop_duplicates()
final_merged.to_csv(output_path, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")