import pandas as pd
import os
import warnings
import time
from typing import *

warnings.simplefilter("ignore")

# Global Variables and Parameters
start_time = time.time()

input_data_base = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Hourly Generation Data"
basis_sheet_name = "ERCOT_LMP_HR_Filtered.xlsx"
gen_sheet_name = "Hourly_Central_Gen.xlsx"

output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Hourly Generation Data/Output Files/weighted_by_day.csv"
output_path_1 = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Hourly Generation Data/Output Files/weighted_by_month.csv"
output_path_2 = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Hourly Generation Data/Output Files/raw_merged.csv"
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
    df_basis = df_basis[['MARKETDAY', "HOURENDING", "MONTH", "YEAR", "PEAKTYPE", basis_col]]
    df_gen = df_gen[['MARKETDAY', "HOURENDING", gen_col]]
    df_basis['MARKETDAY'] = pd.to_datetime(df_basis['MARKETDAY'])
    df_gen['MARKETDAY'] = pd.to_datetime(df_gen['MARKETDAY'])

    df_merged = df_basis.merge(df_gen, how='inner', on=['MARKETDAY', 'HOURENDING'])
    sep = basis_col.index("(")

    df_grouped_daily = df_merged.groupby(['MARKETDAY', 'PEAKTYPE']).apply(weighted_average, basis_col, gen_col).reset_index(name='Basis-Generation WA')
    df_grouped_monthly = df_merged.groupby(['MONTH', 'YEAR', 'PEAKTYPE']).apply(weighted_average, basis_col, gen_col).reset_index(name='Basis-Generation WA')
    df_grouped_daily.insert(2, 'Path', basis_col[:sep])
    df_grouped_daily.insert(3, 'Type', basis_col[sep + 1: sep + 3])
    df_grouped_monthly.insert(2, 'Path', basis_col[:sep])
    df_grouped_monthly.insert(3, 'Type', basis_col[sep + 1: sep + 3])

    df_merged.insert(5, "Path", basis_col)
    df_merged.rename(columns={basis_col: "Basis", gen_col: "Hourly Generation"}, inplace=True)

    print(f"Completed {basis_col} {gen_col}")
    return df_merged, df_grouped_daily, df_grouped_monthly


raw = []
daily = []
monthly = []
with open(os.path.join(input_data_base, mapping_name), "r") as mapping:
    rows = mapping.read().split("\n")

    for row in rows:
        gen, basis = row.split(", ")[0], row.split(", ")[1]
        r, d, m = compute_weighted(df_hr_basis, basis, df_hr_gen, gen)
        raw.append(r)
        daily.append(d)
        monthly.append(m)

raw_df = pd.concat(raw, axis=0)
daily_df = pd.concat(daily, axis=0)
monthly_df = pd.concat(monthly, axis=0)

daily_df['MARKETDAY'] = pd.to_datetime(daily_df['MARKETDAY'])
daily_df = daily_df.sort_values(by=['MARKETDAY', 'Path'])

raw_df['MARKETDAY'] = pd.to_datetime(raw_df['MARKETDAY'])
raw_df = raw_df.sort_values(by=['MARKETDAY', 'HOURENDING'])

monthly_df = monthly_df.sort_values(by=['YEAR', 'MONTH'])

daily_df.to_csv(output_path, index=False)
monthly_df.to_csv(output_path_1, index=False)
raw_df.to_csv(output_path_2, index=False)

# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
