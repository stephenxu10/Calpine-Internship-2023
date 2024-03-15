import os
import zipfile
import time
import pandas as pd
import DAM_Gn_Comparator
import warnings
import re
import DAM_Ln_Xf_Comparator

from typing import Union, List

warnings.simplefilter("ignore")
os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/Automated Generator Comparators")

"""
This Python tool aims to compare hand-picked files from the Day Ahead PSS/E Network Operations Model. 
Given the date and hour parameters for each file, the script locates and reads each CSV (if valid), and outputs the 
comparison results to a CSV/txt file. 

Be sure to change the global date/hour parameters immediately below to compare the desired files.
"""
VERSION = "Xf" # Should be either Ln, Xf, or Gn.

first_date = "03/14/2019"  # Must be in MM/DD/YYYY format
first_hour = 16  # Must be between 1 and 24 (inclusive)
second_date = "06/16/2023"  # Must be in MM/DD/YYYY format
second_hour = 16  # Must be between 1 and 24 (inclusive)

start_time = time.time()
path_base = "\\\\Pzpwuplancli01\\Uplan\\ERCOT"

output_root = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/MIS Gen_Ln_Xf Comparisons"
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
    
    if VERSION not in ["Gn", "Ln", "Xf"]:
        print("Invalid VERSION parameter. Must be either Gn, Ln, or Xf.")
        return

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
                            csv_file = findMatch(zip_ref.namelist(), date_replaced + f"_{VERSION}_" + str(hour).zfill(3))

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
    

df_first = find_generator_data(first_date, first_hour)
df_second = find_generator_data(second_date, second_hour)
formatted_first_date = first_date.replace("/", "")
formatted_second_date = second_date.replace("/", "")

# Perform some quick checks on the input dates.
if first_date == second_date and first_hour == second_hour:
    print("The input dates & times are exactly the same. No comparisons can be done.")
    success = False

if VERSION == "Gn":
    gen_comp_df = DAM_Gn_Comparator.compare_statuses(df_first, df_second, first_date, second_date)
    _, final_df = DAM_Gn_Comparator.merge_with_mapping(gen_comp_df)
    
    final_df.to_csv(output_root + f"/Gn/{formatted_first_date}_{first_hour}_{formatted_second_date}_{second_hour}.csv", index=False)

else:
    line = True if VERSION == "Ln" else False
    
    status_df = DAM_Ln_Xf_Comparator.compare_statuses(df_first, df_second, first_date, second_date, line)
    ratea_df = DAM_Ln_Xf_Comparator.compare_rates(df_first, df_second, first_date, second_date, "RATEA")
    rateb_df = DAM_Ln_Xf_Comparator.compare_rates(df_first, df_second, first_date, second_date, "RATEB")
    
    status_df.to_csv(output_root + f"/{VERSION}/{formatted_first_date}_{first_hour}_{formatted_second_date}_{second_hour}_status.csv", index=False)
    ratea_df.to_csv(output_root + f"/{VERSION}/{formatted_first_date}_{first_hour}_{formatted_second_date}_{second_hour}_RATEA.csv", index=False)
    rateb_df.to_csv(output_root + f"/{VERSION}/{formatted_first_date}_{first_hour}_{formatted_second_date}_{second_hour}_RATEB.csv", index=False)

end_time = time.time()
execution_time = end_time - start_time
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
