import pandas as pd
import os
from datetime import datetime
import glob
import numpy as np

TIME_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/05 - Schedules/Timeofuse"
PATH_BASE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/Monthly"
NODE_PLANT_MAPPING = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/501 - Templates/Extracts/NodePlantMapping.csv"
OUTPUT_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Monthly Auction Summary Reports"

def custom_format(x):
    if pd.isna(x):  # Check for NaN values first
        return x
    elif x == 0:
        return '          -'
    elif isinstance(x, (int, float)) and x < 0:
        return f"({-x})"
    return x


month_mapping = {
    "January": 0,
    "February": 1,
    "March": 2,
    "April": 3,
    "May": 4,
    "June": 5,
    "July": 6,
    "August": 7,
    "September": 8,
    "October": 9,
    "November": 10,
    "December": 11
}


def aggregate_weights(time_path, month_mapping):
    """
    Aggregate all time of use data through the time_file directory.  This procedure results in a 3D array weights, with
    weights[i] corresponding to the time of use information for year 2013 + i. 
    
    Each weights[i] is a 2D array with weights[i][j] resulting in an array of three elements: OFFPEAK, PEAKWD, PEAKWE,
    respectively.
    """
    weights = []
    
    time_files = [f for f in os.listdir(time_path) if "CRR" in f and "~" not in f]
    time_dfs = [pd.read_excel(os.path.join(time_path, f), skiprows=1) for f in time_files]
    
    combined_time_df = pd.concat(time_dfs).drop_duplicates() 
    combined_time_df = combined_time_df.sort_values(by="Year")
    combined_time_df = combined_time_df.rename(columns={"OffPeak": "OFFPEAK", "PeakWD": "PEAKWD", "PeakWE": "PEAKWE"})
    
    start = 2013
    curr = [[0, 0, 0] for _ in range(12)]
    for row in combined_time_df.iterrows():
        row = row[1]
        if int(row[0]) != start:
            weights.append(curr)
            curr = [[0, 0, 0] for _ in range(12)]
            start += 1
        
        curr[month_mapping[row[1]]][0] = row[2]
        curr[month_mapping[row[1]]][1] = row[3]
        curr[month_mapping[row[1]]][2] = row[4]
    
    weights.append(curr)
    return weights

def find_and_preprocess(node_path, path_base):
    node_plant_df = pd.read_csv(node_path)
    plant_mapping = dict(zip(node_plant_df['Path'], node_plant_df['Plant']))
    
    """
    Check for the most Monthly Auction Report
    """
    today = datetime.now()
    year = today.year
    month = today.month
    
    # Increment the month
    next_month = month
    
    if next_month == 13:
        next_month = 1
        year += 1
    
    next_month_f = f"{next_month:02d}"
    monthly_path = os.path.join(path_base, str(year), f"{year}-{next_month_f}", "Market Results")
    
    if not os.path.exists(monthly_path):
        print(f"ERROR: Auction data does not exist for year {year}, month {next_month}")
        return
    
    file_matches = glob.glob(os.path.join(monthly_path, "Private_*_AUCTION.CSV"))
    if len(file_matches) > 0:
        # Should only be one match.
        private_auction_file = file_matches[0]
    
    else:
        print(f"ERROR: No Auction Data is available for the next month.")
        return
    
    time_of_use_mapping = {"PeakWD": weights[year - 2013][next_month - 1][1], "PeakWE": weights[year - 2013][next_month - 1][2], "Off-peak": weights[year - 2013][next_month - 1][0]}
    
    """
    Read in and process the Monthly Auction File
    """
    auction_df = pd.read_csv(private_auction_file)
    relevant_columns = ["Source", "Sink", "TimeOfUse", "MW", "HedgeType", "ShadowPricePerMWH", "BidPricePerMWH", "ACI99"]
    auction_df = auction_df[relevant_columns]
    auction_df['Path'] = auction_df['Source'].astype(str) + "+" + auction_df['Sink'].astype(str)
    
    # Replace the Source column with the corresponding plant
    auction_df['Plant'] = auction_df['Path'].map(plant_mapping)
    auction_df.drop(columns=['Source', 'Sink', 'Path'], inplace=True)
    
    # Add in the Timeofuse hours
    auction_df["Hours"] = auction_df['TimeOfUse'].map(time_of_use_mapping)
    
    return auction_df, time_of_use_mapping


weights = aggregate_weights(TIME_PATH, month_mapping)
auction_df, time_of_use_mapping = find_and_preprocess(NODE_PLANT_MAPPING, PATH_BASE)

"""
Obtain the MW Data
"""
auction_MW_df = auction_df.groupby(['TimeOfUse', "Plant", "HedgeType"])['MW'].sum().reset_index()
auction_MW_df = auction_MW_df.round(0)
auction_MW_df = auction_MW_df.pivot_table(index=['Plant', 'HedgeType'], columns='TimeOfUse', values='MW', aggfunc='max').reset_index()
auction_MW_df.columns = ['Plant', 'HedgeType', 'Off-Peak MW', 'PeakWD MW', 'PeakWE MW']

auction_MW_df = auction_MW_df[(auction_MW_df[['PeakWD MW', 'PeakWE MW', 'Off-Peak MW']].fillna(0) != 0).any(axis=1)]
auction_MW_df = auction_MW_df.sort_values(by=['HedgeType', 'Plant'])
auction_MW_df = auction_MW_df[['HedgeType', 'Plant', 'PeakWD MW', 'PeakWE MW', 'Off-Peak MW']]

"""
Obtain the Invest $ Data
"""
invest_df = auction_df.groupby(['TimeOfUse', "Plant", "HedgeType"]).apply(lambda x: (x['MW'] * x['ShadowPricePerMWH']).sum()).reset_index()
invest_df.columns = ['TimeOfUse', 'Plant', 'HedgeType', 'DotProduct']
invest_df = invest_df.pivot_table(index=['Plant', 'HedgeType'], columns='TimeOfUse', values='DotProduct').reset_index()
invest_df.columns = ['Plant', 'HedgeType', 'Off-Peak Invest', 'PeakWD Invest', 'PeakWE Invest']

invest_df = invest_df[(invest_df[['PeakWD Invest', 'PeakWE Invest', 'Off-Peak Invest']].fillna(0) != 0).any(axis=1)]
invest_df = invest_df[['HedgeType', 'Plant', 'PeakWD Invest', 'PeakWE Invest', 'Off-Peak Invest']]

# Multiply by the TimeOfUse
invest_df['Off-Peak Invest'] = invest_df['Off-Peak Invest'] * time_of_use_mapping["Off-peak"]
invest_df['PeakWD Invest'] = invest_df['PeakWD Invest'] * time_of_use_mapping["PeakWD"]
invest_df['PeakWE Invest'] = invest_df['PeakWE Invest'] * time_of_use_mapping["PeakWE"]
invest_df = invest_df.round(0)

# Merge together the tables!
merged_df = pd.merge(auction_MW_df, invest_df, on=['Plant', 'HedgeType'])
grand_total = merged_df.select_dtypes(include=[np.number]).sum()
new_row = ['Grand Total'] + [np.nan] * (len(merged_df.columns) - len(grand_total) - 1) + grand_total.tolist()
grand_total_row = pd.DataFrame([new_row], columns=merged_df.columns)

merged_df = merged_df.append(grand_total_row, ignore_index=True)
merged_df = merged_df.applymap(custom_format)

multi_index_columns = pd.MultiIndex.from_tuples([
    ('', 'HedgeType'), ('', 'Plant'), 
    ('', 'WDPEAK'), ('MW', 'WEPEAK'), ('', 'OFFPEAK'), 
    ('', 'WDPEAK'), ('Invest $', 'WEPEAK'), ('', 'OFFPEAK')
])

merged_df.columns = multi_index_columns
merged_df.to_csv(OUTPUT_PATH + "/invest_report_1.csv")


