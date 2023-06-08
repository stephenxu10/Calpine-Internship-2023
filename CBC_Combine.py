import bisect, os, csv
from typing import *
from utils import convert, getFirstColValue
import pandas as pd

## TODO: Add documentation, comments and write a report

# Global parameters & variables
months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
path_base = "./../../06 - CRR/Monthly"
output_path = "./data/final_combined.csv"  # Relative file path of the outputted CSV.

# Starting and ending years. By default, this encompasses all years with available data.
start_year = 2019
end_year = 2023


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
def getFiles(year: int, month: int) -> Tuple[Union[str, Any], Union[str, Any]]:
    c_month = convert(month)
    year = str(year)

    new_base = path_base + "/" + year if year != "2019" else path_base + "/" + year + "/999 - Month"

    cbc_dir = new_base + "/" + year + "-" + c_month + ("/Market Results" if year >= "2021" else "/Market Result")
    excel_dir = new_base + "/" + year + "-" + c_month + "/Network Model"

    cbc_file = cbc_dir + "/Common_BindingConstraint_" + year + "." + months[month-1] + ".Monthly.Auction_AUCTION.CSV"
    excel_file = excel_dir + "/" + year + "." + months[month-1] + ".Monthly.Auction.MappingDocument.xlsx"

    if os.path.isfile(cbc_file) and os.path.isfile(excel_file):
        return cbc_file, excel_file

    else:
        return "", ""


"""

"""
def replaceCSV(csv_file: str, excel_file: str):
    df_lines = pd.read_excel(excel_file, sheet_name=0)
    df_autos = pd.read_excel(excel_file, sheet_name=1)

    df_lines_sorted = df_lines.sort_values(by=['CRR_Tag']).values
    df_autos_sorted = df_autos.sort_values(by=['CRR Name']).values
    df_csv = pd.read_csv(csv_file)
    operationNames = []

    lines_opnames = {}
    autos_opnames = {}

    for index, row in df_csv.iterrows():
        deviceName = row['DeviceName']
        deviceType = row['DeviceType']

        if deviceType == "Line":
            if deviceName in lines_opnames.keys():
                operationNames.append(lines_opnames[deviceName])
            else:
                corresponding_val = df_lines_sorted[bisect.bisect_left(df_lines_sorted, deviceName, key=getFirstColValue)][1]
                operationNames.append(corresponding_val)
                lines_opnames[deviceName] = corresponding_val

        elif deviceType == "Transformer":
            if deviceName in autos_opnames.keys():
                operationNames.append(autos_opnames[deviceName])
            else:
                corresponding_val = df_autos_sorted[bisect.bisect_left(df_autos_sorted, deviceName, key=getFirstColValue)][1]
                operationNames.append(corresponding_val)
                autos_opnames[deviceName] = corresponding_val

        else:
            operationNames.append(deviceName)

    df_csv.pop("DeviceName")
    df_csv.insert(0, 'Operations_Name', operationNames)
    return df_csv


merge = []
for yr in range(start_year, end_year + 1):
    for mth in range(1, 13):
        csv_f, excel = getFiles(yr, mth)
        if csv_f != "":
            merge.append(replaceCSV(csv_f, excel))

merged_df = pd.concat(merge, axis=0)
merged_df = pd.DataFrame(merged_df)
merged_df.to_csv(output_path, index=False)
print("Generation Complete")








