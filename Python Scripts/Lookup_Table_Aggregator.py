import os
import time
from typing import Dict, List, Tuple
from utils import convert
import pandas as pd
import warnings

# Global parameters & variables
start_time = time.time()
path_base = "./../../../02 - Input/Transmission/Standard Ctgcs"
output_path = "./../Data/Lookup_Table_Combined.csv"  # Relative file path of the outputted CSV.

year = 2023

warnings.simplefilter("ignore")
yearly_excel = [x for x in os.listdir(path_base) if str(year) in x]

def fill_gaps(names: List[str]) -> List[str]:
    res = [names[0]]

    for i in range(1, len(names)):
        if pd.isnull(names[i]):
            res.append(res[i-1])

        else:
            res.append(names[i])

    return res

def convert_sheet(sheet_name: str) -> pd.DataFrame:
    idx = sheet_name.index(str(year))
    date = sheet_name[idx - 4: idx + 4]

    full_path = os.path.join(path_base, sheet_name)
    excel_sheet = pd.read_excel(full_path, sheet_name=2)

    new_names = fill_gaps(excel_sheet['Contingency ID'].tolist())
    new_des = fill_gaps(excel_sheet['Contingency Description'].tolist())
    dates = [date] * len(excel_sheet)

    excel_sheet = excel_sheet.drop(['Contingency ID', 'Contingency Description', 'Radial', 'Operator', 'Contingency Group'], axis=1)
    excel_sheet.insert(0, 'Contingency Description', new_des)
    excel_sheet.insert(0, 'Contingency ID', new_names)
    excel_sheet.insert(0, 'Date', dates)

    return excel_sheet


final_merge = []
for sheet in yearly_excel:
    final_merge.append(convert_sheet(sheet))

final_merged_df = pd.concat(final_merge, axis=0)
final_merged_df = pd.DataFrame(final_merged_df)
final_merged_df.to_csv(output_path, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")

