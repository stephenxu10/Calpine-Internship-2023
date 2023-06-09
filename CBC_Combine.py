import os
import time
from typing import Dict, List, Tuple
from utils import convert
import pandas as pd

"""
In this Python script, two separate tasks are accomplished. First, for all historical Common Binding
Constraint Monthly Auction records, the entries in the leftmost (DeviceName) column are replaced with
their operation name in the corresponding Monthly Auction Mapping Document. Next, all of the modified
records are aggregated into a large CSV file, stored in the data subfolder.

Note:
This script will only work properly if it is ran from
\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates
due to the File I/O.

The searching and file-building process takes a while, expect to wait around 3 minutes for the script to
finish generating. As usual, assuming a consistent file structure, running this script will generate
an updated output whenever future data is added. 
"""

# Global parameters & variables
start_time = time.time()
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
path_base = "./../../06 - CRR/Monthly"
output_path = "./Data/final_combined.csv"  # Relative file path of the outputted CSV.


# Starting and ending years. By default, this encompasses all years with available data.
start_year = 2019
end_year = 2050


"""
Given an input year and month, this helper method performs File I/O to return a tuple of two strings:
the relative path to the Common_BindingConstraint document followed by the path to the Monthly Auction
Mapping Document Excel sheet

Inputs:
    - year: An integer between start_year and end_year
    - month: An integer in the interval [1, 12]
    
Output: A two-element tuple containing the file paths specified above. If any of the files are invalid,
        we return an empty two-element tuple.
"""
def get_files(year: int, month: int) -> Tuple[str, str]:
    c_month = convert(month)
    year = str(year)

    # 2019 is in a different directory - use a ternary expression
    new_base = path_base + "/" + year if year != "2019" else path_base + "/" + year + "/999 - Month"

    # Years before 2021 refer to the Market Results folder as "Market Result"
    cbc_dir = new_base + "/" + year + "-" + c_month + ("/Market Results" if year >= "2021" else "/Market Result")
    excel_dir = new_base + "/" + year + "-" + c_month + "/Network Model"

    cbc_file = cbc_dir + "/Common_BindingConstraint_" + year + "." + months[month-1] + ".Monthly.Auction_AUCTION.CSV"
    excel_file = excel_dir + "/" + year + "." + months[month-1] + ".Monthly.Auction.MappingDocument.xlsx"

    if os.path.isfile(cbc_file) and os.path.isfile(excel_file):
        return cbc_file, excel_file

    else:
        return "", ""
    
"""
Performs a binary search on an input 2-dimensional list of strings. Searches the list for the unique
index such that the first column equals to target and outputs the corresponding second column value.

Inputs:
    - input: A 2-dimensional list of strings. It is assumed that the first column is sorted in ASCII order.
    - target: The target string

Output:
    - Performs a binary search to find the second column value corresponding to target.
    
     In practice, this helper method gives the corresponding operation name to a device name.
"""
def mod_binary_search(input: List[List[str]], target: str) -> str:
    low = 0
    high = len(input) - 1
    
    while low != high:
        mid = (low + high) // 2
        
        if input[mid][0] == target:
            return input[mid][1]
        
        elif input[mid][0] < target:
            low = mid + 1
    
        else:
            high = mid - 1
            
    
    return input[low][1]


"""
Helper method that processes an input csv file and replaces the 'DeviceName' column with its
corresponding operation name found in the mapping excel file. Returns a new Pandas DataFrame with
the updated entries

Inputs:
    - csv_file: A string giving the path to the CSV file. It is guaranteed to be valid.
    - excel_file: A string giving the path to the Excel file. It is also guaranteed to be valid.
    
Output:
    This method outputs a Pandas DataFrame that contains the updated entry for each element in the 'DeviceName'
    column of the CSV file, matched with their corresponding Operation Names found in the Excel file.
"""
def replace_csv(csv_file: str, excel_file: str) -> pd.DataFrame:
    # Read both sheets of the Excel file.
    df_lines = pd.read_excel(excel_file, sheet_name=0)
    df_autos = pd.read_excel(excel_file, sheet_name=1)

    # First sort the rows of each Excel sheet based on the first column in ASCII order. This step improves the search time
    # overall, since we are making thousands of queries to the Excel sheet at a time. Performing simple linear
    # search each time feels too inefficient.
    df_lines_sorted = df_lines.sort_values(by=['CRR_Tag']).values
    df_autos_sorted = df_autos.sort_values(by=['CRR Name']).values
    df_csv = pd.read_csv(csv_file)
    operationNames = []

    # There are quite a few duplicate DeviceNames in the CSV file, so recording the found matches in a dictionary
    # also will slightly speed up the process.
    lines_opnames = {}
    autos_opnames = {}

    # Iterate through each row of the CSV file.
    for index, row in df_csv.iterrows():
        deviceName = row['DeviceName']
        deviceType = row['DeviceType']

        # Search the lines page of the Excel sheet
        if deviceType == "Line":
            if deviceName in lines_opnames.keys():
                operationNames.append(lines_opnames[deviceName])
            else:
                # Since the lines sheet is sorted, a simple Binary Search is optimal here.
                corresponding_val = mod_binary_search(df_lines_sorted, deviceName)
                operationNames.append(corresponding_val)

        # Search the autos page of the Excel sheet
        elif deviceType == "Transformer":
            if deviceName in autos_opnames.keys():
                operationNames.append(autos_opnames[deviceName])
            else:
                corresponding_val = mod_binary_search(df_autos_sorted, deviceName)
                operationNames.append(corresponding_val)

        # Make no change otherwise.
        else:
            operationNames.append(deviceName)

    # Add the new column and remove the old 'DeviceName' column.
    df_csv.pop("DeviceName")
    df_csv.insert(0, 'Operations_Name', operationNames)
    return df_csv


"""
Main procedure. Filters each CSV file and aggregates them all into a merged CSV. The final result
is then stored in output_path.
"""
merge = []
for yr in range(start_year, end_year + 1):
    for mth in range(1, 13):
        csv_f, excel = get_files(yr, mth)
        if csv_f != "":
            merge.append(replace_csv(csv_f, excel))

merged_df = pd.concat(merge, axis=0)
merged_df = pd.DataFrame(merged_df)
merged_df.to_csv(output_path, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")







