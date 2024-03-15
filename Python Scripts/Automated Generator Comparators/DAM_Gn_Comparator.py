import pandas as pd
from typing import Tuple

MAPPING_PATH = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/06 - CRR/02 - Summary/MappingDocument/Gen Mapping CDR to Operations.xlsx"

def compare_statuses(df1: pd.DataFrame, df2: pd.DataFrame, date1: str, date2: str):
    """
    Inputs:
        - df1, df2: The two Pandas DataFrames that give line data.

    Output:
        - A DataFrame showing the changes in their statuses
    """
    status_key = "Generator"
    df1.columns = [col.strip() for col in df1.columns]
    df2.columns = [col.strip() for col in df2.columns]

    df1 = df1[['Generator Name', f'{status_key} Status']]
    df2 = df2[['Generator Name', f'{status_key} Status']]

    merged_df = pd.merge(df1, df2, on='Generator Name', suffixes=('_1', '_2'), how='outer')

    def describe_change(row):
        status_1 = row[f'{status_key} Status_1']
        status_2 = row[f'{status_key} Status_2']
        if pd.notnull(status_1) and pd.notnull(status_2):
            if status_1 == status_2:
                if status_1 == "In-Service":
                    return "No Change"
                else:
                    return "Still Out-Of-Service"
            else:
                return "Changed"
        elif pd.isnull(status_1):
            return "Missing-In-First"
        elif pd.isnull(status_2):
            return "Missing-In-Second"

    merged_df['Description'] = merged_df.apply(describe_change, axis=1)
    merged_df.rename(columns={ f'{status_key} Status_1': f"{date1} Status",  f'{status_key} Status_2': f"{date2} Status"}, inplace=True)

    merged_df = merged_df[merged_df['Description'] != "No Change"]
    merged_df = merged_df.sort_values(by=['Description'])
    merged_df.fillna('', inplace=True)

    return merged_df    

def merge_with_mapping(merged_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mapping_cols = ["English Name", "Size", "Type", "Generator in Load Zone", "Generator Name"]
    mapping_df = pd.read_excel(MAPPING_PATH, sheet_name="Master", usecols=mapping_cols)


    combined_df = pd.merge(mapping_df, merged_df, on="Generator Name", how='inner')
    unknown_df = combined_df[combined_df['English Name'] == 'Unkwn']
    known_df = combined_df[combined_df['English Name'] != 'Unkwn']

    unknown_df = unknown_df.sort_values(by=['Description', unknown_df.columns[-2]])
    known_df = known_df.sort_values(by=['Description', known_df.columns[-2]])

    known_df = known_df[known_df.columns[[4, 1, 2, 3, 0, 5, 6, 7]]]
    return unknown_df, known_df
