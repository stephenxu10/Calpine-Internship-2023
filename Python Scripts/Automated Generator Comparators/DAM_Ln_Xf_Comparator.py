import pandas as pd
import numpy as np

def pre_process_branch(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[(df['From PSS/E KV']==df['To PSS/E KV']) & ((df['From PSS/E KV'] == 0)), 'Expanded Branch Name'] = df['Branch Name']
    df.loc[(df['From PSS/E KV'] == df['To PSS/E KV']) & (df['From PSS/E KV'] != 0), 'Expanded Branch Name'] = (df['From Station Name/PSS/E Bus Name'].astype(str).str.strip() + '-' +
                                                                                                            df['To Station Name/PSS/E Bus Name'].astype(str) + ' ' +
                                                                                                            df['From PSS/E KV'].astype(str) + 'KV ' +
                                                                                                            df['Branch Name'].astype(str))

    df.loc[(df['From PSS/E KV'] != df['To PSS/E KV']) & (df['From PSS/E KV'] != 0), 'Expanded Branch Name'] = (df['From Station Name/PSS/E Bus Name'].astype(str).str.strip() + ' ' +
                                                                                                            df['From PSS/E KV'].astype(str) + 'KV ' +
                                                                                                            df['Branch Name'].astype(str))


    condition = (df['From PSS/E KV'] != df['To PSS/E KV']) & (df['From PSS/E KV'] != 0)
    df['Expanded Branch Name'] = np.where(
        condition,
        np.where(
            pd.notna(df['From Station Name/PSS/E Bus Name']) & pd.notna(df['From PSS/E KV']),
            df['From Station Name/PSS/E Bus Name'].astype(str) + ' ' + df['From PSS/E KV'].astype(str) + 'KV ' + df['Branch Name'],
            df['Branch Name']
        ),
        df['Expanded Branch Name']  # Keeps the existing value if the condition is False
    )
    
    df.drop(columns=['From Station Name/PSS/E Bus Name', 'From PSS/E KV', 'To Station Name/PSS/E Bus Name', 'To PSS/E KV', 'Branch Name'], inplace=True)
    return df

def compare_statuses(df1: pd.DataFrame, df2: pd.DataFrame, date1: str, date2: str, line: bool=True):
    """
    Inputs:
        - df1, df2: The two Pandas DataFrames that give line data.

    Output:
        - A DataFrame showing the changes in their statuses
    """
    status_key = "Branch" if line else "Transformer"
    df1.columns = [col.strip() for col in df1.columns]
    df2.columns = [col.strip() for col in df2.columns]

    df1 = pre_process_branch(df1)
    df2 = pre_process_branch(df2)

    df1 = df1[['Expanded Branch Name', f'{status_key} Status']]
    df2 = df2[['Expanded Branch Name', f'{status_key} Status']]

    merged_df = pd.merge(df1, df2, on='Expanded Branch Name', suffixes=('_1', '_2'), how='outer')

    def describe_change(row):
        status_1 = row[f'{status_key} Status_1']
        status_2 = row[f'{status_key} Status_2']
        if pd.notnull(status_1) and pd.notnull(status_2):
            if status_1 == status_2:
                if status_1 == "In-Service":
                    return "No Change"
                else:
                    return "No Change"
            else:
                return "Changed"
        elif pd.isnull(status_1):
            return "Missing-In-First"
        elif pd.isnull(status_2):
            return "Missing-In-Second"
    
    merged_df['Description'] = merged_df.apply(describe_change, axis=1)
    merged_df.rename(columns={ f'{status_key} Status_1': f"{date1} Status",  f'{status_key} Status_2': f"{date2} Status"}, inplace=True)

    merged_df = merged_df[merged_df['Description'] != "No Change"]
    merged_df.fillna('', inplace=True)
    merged_df = merged_df.sort_values(by=['Description',f'{date1} Status'])
    return merged_df    

def compare_rates(df1: pd.DataFrame, df2: pd.DataFrame, date1: str, date2: str, key: str) -> pd.DataFrame:
    df1.columns = [col.strip() for col in df1.columns]
    df2.columns = [col.strip() for col in df2.columns]

    # Select only the necessary columns
    df1 = df1[['Expanded Branch Name', key]]
    df2 = df2[['Expanded Branch Name', key]]

    # Perform inner merge
    merged_df = pd.merge(df1, df2, on='Expanded Branch Name', suffixes=('_1', '_2'), how='inner')
    merged_df['Percent Change'] = abs(merged_df[f'{key}_1'] - merged_df[f'{key}_2']) / ((merged_df[f'{key}_1'] + merged_df[f'{key}_2']) / 2) * 100
    filtered_df = merged_df[merged_df['Percent Change'] >= 15]

    filtered_df.rename(columns={ f'{key}_1': f"{date1}  {key}",  f'{key}_2': f"{date2 } {key}"}, inplace=True)
    
    return filtered_df

