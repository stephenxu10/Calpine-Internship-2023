import pandas as pd
import os
import warnings
import time
from typing import *
from datetime import timedelta

warnings.simplefilter("ignore")

# Global Variables and Parameters
start_time = time.time()

input_data_base = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Hourly Generation Data"
basis_sheet_name = "ERCOT_LMP_HR_Filtered.xlsx"
gen_sheet_name = "Hourly_Central_Gen.xlsx"

output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Hourly Generation Data/Weighted_Averages.csv"
mapping_name = "Gen_Basis_Mapping.txt"

df_hr_basis = pd.read_excel(os.path.join(input_data_base, basis_sheet_name), sheet_name="Sheet1")
df_hr_gen = pd.read_excel(os.path.join(input_data_base, gen_sheet_name), sheet_name="Sheet1")

# Calculate the weighted average
def weighted_average(group, basis_col, gen_col):
    total_generation = group[gen_col].sum()
    total_weighted_value = (group[gen_col] * group[basis_col]).sum()
    
    if total_generation != 0:
        return total_weighted_value / total_generation
    else:
        return None


def compute_weighted(df_basis, basis_col, df_gen, gen_col) -> pd.DataFrame:
    """
    Given two DataFrames and two corresponding columns between them, this
    helper method creates a new DataFrame that finds the weighted average
    between generation and basis across a PeakType (OFFPEAK, WD/WEPEAK) for
    the whole sheets.

    Assumes:
        - Each DataFrame has the MARKETDAY and HOURENDING columns. 
    
    To avoid Division by Zero exceptions, if the sum of hourly generation 
    across a PeakType is 0, we fill the corresponding cell in the output
    DataFrame with N/A.

    Inputs:
        - df_basis, df_gen: The two DataFrames we will merge and compute
        weighted averages on.
        - basis_col, gen_col: The corresponding columns, i.e. ('Baytown', 'BTE_BTE_G1-H(RT)')
    
    Output:
        - A DataFrame with three columns - MARKETDAY, HOURENDING, and
        the weighted average between hourly generation and basis.
    """
    df_basis = df_basis[['MARKETDAY', "HOURENDING", "PEAKTYPE", basis_col]]
    df_gen = df_gen[['MARKETDAY', "HOURENDING", gen_col]]
    df_basis['MARKETDAY'] = pd.to_datetime(df_basis['MARKETDAY'])
    df_gen['MARKETDAY'] = pd.to_datetime(df_gen['MARKETDAY'])

    df_merged = df_basis.merge(df_gen, how='inner', on=['MARKETDAY', 'HOURENDING'])
    sep = basis_col.index("(")

    df_merged = df_merged.groupby(['MARKETDAY', 'PEAKTYPE']).apply(weighted_average, basis_col, gen_col).reset_index(name='Basis-Generation WA')
    df_merged.insert(2, 'Path', basis_col[:sep])
    df_merged.insert(3, 'Type', basis_col[sep + 1: sep + 3])

    print(f"Completed {basis_col} {gen_col}")
    return df_merged


merged = []
with open(os.path.join(input_data_base, mapping_name), "r") as mapping:
    rows = mapping.read().split("\n")

    for row in rows:
        gen, basis = row.split(", ")[0], row.split(", ")[1]
        merged.append(compute_weighted(df_hr_basis, basis, df_hr_gen, gen))

overall_df = pd.concat(merged, axis=0)
overall_df['MARKETDAY'] = pd.to_datetime(overall_df['MARKETDAY'])
overall_df = overall_df.sort_values(by=['MARKETDAY', 'Path'])

overall_df.to_csv(output_path, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")