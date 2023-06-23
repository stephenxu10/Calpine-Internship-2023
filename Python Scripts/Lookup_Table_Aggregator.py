import os
import time
from typing import List
import pandas as pd
import warnings

# Global parameters & variables
start_time = time.time()
year = 2023

path_base = "./../../../02 - Input/Transmission/Standard Ctgcs"
output_raw = "./../Data/Lookup Tables/Lookup_Table_Combined_" + str(year) + "_raw.csv"  # Relative file path of the outputted raw CSV.
output_drop = "./../Data/Lookup Tables/Lookup_Table_Combined_" + str(year) + "_dropped.csv"  # Relative file path of the outputted filtered CSV.

warnings.simplefilter("ignore")
yearly_excel = [x for x in os.listdir(path_base) if str(year) in x]


def fill_gaps(names: List[str]) -> List[str]:
    """
    Fills in missing NaN gaps in a list of strings as follows:
        [2, NaN, NaN, NaN, 3, NaN, NaN, Nan, 4] -> [2, 2, 2, 2, 3, 3, 3, 3, 4]
    Useful helper method for later.

    Inputs:
        - names: A list of strings.

    Output:
        - res: The "filled-in" list as demonstrated above.
    """

    # Assume the first element is not NaN.
    res = [names[0]]

    for i in range(1, len(names)):
        if pd.isnull(names[i]):
            res.append(res[i-1])

        else:
            res.append(names[i])

    return res

def convert_sheet(sheet_name: str) -> pd.DataFrame:
    """
    Locates and reads a given Excel sheet name and converts it by filling in gaps
    and filtering out unnecessary data.

    Input:
        sheet_name: The name of the Excel File,
            e.g. CIM_Apr_ML1_1_04112023_Complete_StandardContingencyReport.xlsx

    Output:
        A Pandas DataFrame with the converted information.
    """

    # Extract the full date and read the Excel sheet.
    idx = sheet_name.index(str(year))
    date = sheet_name[idx - 4: idx + 4]

    full_path = os.path.join(path_base, sheet_name)
    excel_sheet = pd.read_excel(full_path, sheet_name=2)
    
    # Build the new, filled-in columns.
    new_names = fill_gaps(excel_sheet['Contingency ID'].tolist())
    new_des = fill_gaps(excel_sheet['Contingency Description'].tolist())
    dates = [date[0:2] + "/" + date[2:4] + "/" + date[4:]] * len(excel_sheet)
    
    # Drop old columns, add in new ones.
    excel_sheet = excel_sheet.drop(['Contingency ID', 
                                    'Contingency Description', 
                                    'Radial', 'Operator', 'Contingency Group'], axis=1)
    excel_sheet.insert(0, 'Contingency Description', new_des)
    excel_sheet.insert(0, 'Contingency ID', new_names)
    excel_sheet.insert(0, 'Date', dates)

    return excel_sheet


final_merge = []
for sheet in yearly_excel:
    # Ignore temporary Excel sheets.
    if not sheet.startswith("~"):
        final_merge.append(convert_sheet(sheet))
        print("done!")

# Merge all the DataFrames into one massive CSV.
final_merged_df = pd.concat(final_merge, axis=0)
final_merged_df = pd.DataFrame(final_merged_df)

# Ignore the 'header' lines
final_merged_df = final_merged_df.dropna(subset=['From Station', 'Voltage Level'], how='all')

final_dropped = final_merged_df.drop_duplicates(subset=['Contingency ID', 'Element Name'])
final_dropped = final_dropped.drop(['Date'], axis=1)

# Output to respective CSVs
final_dropped.to_csv(output_drop, index=False)
final_merged_df.to_csv(output_raw, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
