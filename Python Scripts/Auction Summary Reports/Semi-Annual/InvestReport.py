import pandas as pd
import os
import glob

YEAR = 2024
PATH_BASE = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/06 - CRR/Semi-Annual/2024-2H/Auction/E-S2/Market Results"
NODE_PLANT_MAPPING = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/501 - Templates/Extracts/NodePlantMapping.csv"
OUTPUT_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Semi-Annual Auction Summary Reports"
TIME_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/05 - Schedules/Timeofuse"

file_matches = glob.glob(os.path.join(PATH_BASE, "Private_*_AUCTION.CSV"))
if len(file_matches) > 0:
    private_auction_file = file_matches[0]

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

def find_and_preprocess(node_path, auction_file):
    node_plant_df = pd.read_csv(node_path)
    plant_mapping = dict(zip(node_plant_df['Path'], node_plant_df['Plant']))
    
    """
    Read in and process the Monthly Auction File
    """
    auction_df = pd.read_csv(auction_file)
    relevant_columns = ["Source", "Sink", "StartDate", "EndDate", "TimeOfUse", "MW", "HedgeType", "ShadowPricePerMWH"]
    auction_df = auction_df[relevant_columns]
    auction_df['Path'] = auction_df['Source'].astype(str) + "+" + auction_df['Sink'].astype(str)
    
    # Replace the Source column with the corresponding plant
    auction_df['Plant'] = auction_df['Path'].map(plant_mapping)
    auction_df.drop(columns=['Source', 'Sink', 'Path'], inplace=True)
    
    # Reformat the Start and EndDate columns
    auction_df["StartDate"] = pd.to_datetime(auction_df['StartDate'], format="%m/%d/%Y")
    auction_df["EndDate"] = pd.to_datetime(auction_df['EndDate'], format="%m/%d/%Y")
    
    return auction_df

def held_mw_month(auction_df: pd.DataFrame, month: int) -> pd.DataFrame:
    """
    Obtains the MW data for a certain month
    """
    auction_df = auction_df[(auction_df["StartDate"].dt.month <= month) & (auction_df["EndDate"].dt.month >= month)]
    auction_df = auction_df.drop(columns=['StartDate', 'EndDate'])
    auction_MW_df = auction_df.groupby(['TimeOfUse', "Plant", "HedgeType"])['MW'].sum().reset_index()
    auction_MW_df = auction_MW_df.round(0)
    auction_MW_df = auction_MW_df.pivot_table(index=['Plant', 'HedgeType'], columns='TimeOfUse', values='MW', aggfunc='max').reset_index()
    auction_MW_df.columns = ['Plant', 'HedgeType', 'Off-Peak', 'PeakWD', 'PeakWE']
    
    auction_MW_df = auction_MW_df.sort_values(by=['HedgeType', 'Plant'])
    auction_MW_df = auction_MW_df[['HedgeType', 'Plant', 'PeakWD', 'PeakWE', 'Off-Peak']]
    
    auction_MW_df["Month"] = month
    return auction_MW_df

def invest_month(auction_df: pd.DataFrame, month: int) -> pd.DataFrame:
    """
    Obtain the Invest $ Data
    """
    auction_df = auction_df[(auction_df["StartDate"].dt.month <= month) & (auction_df["EndDate"].dt.month >= month)]
    invest_df = auction_df.groupby(['TimeOfUse', "Plant", "HedgeType"]).apply(lambda x: (x['MW'] * x['ShadowPricePerMWH']).sum()).reset_index()
    invest_df.columns = ['TimeOfUse', 'Plant', 'HedgeType', 'DotProduct']
    invest_df = invest_df.pivot_table(index=['Plant', 'HedgeType'], columns='TimeOfUse', values='DotProduct').reset_index()
    invest_df.columns = ['Plant', 'HedgeType', 'Off-Peak', 'PeakWD', 'PeakWE']
    invest_df = invest_df[['HedgeType', 'Plant', 'PeakWD', 'PeakWE', 'Off-Peak']]
    
    # Multiply by the TimeOfUse
    invest_df['Off-Peak'] = invest_df['Off-Peak'] * weights[YEAR - 2013][month - 1][0]
    invest_df['PeakWD'] = invest_df['PeakWD'] * weights[YEAR - 2013][month - 1][1]
    invest_df['PeakWE'] = invest_df['PeakWE'] * weights[YEAR - 2013][month - 1][2]
    invest_df = invest_df.round(0)
        
    invest_df["Month"] = month
    return invest_df
    

weights = aggregate_weights(TIME_PATH, month_mapping)
auction_df = find_and_preprocess(NODE_PLANT_MAPPING, private_auction_file)

mw_df = pd.concat([held_mw_month(auction_df, m) for m in range(7, 13)], axis=0)
invest_df = pd.concat([invest_month(auction_df, m) for m in range(7, 13)], axis=0)

mw_pivot_df = mw_df.pivot_table(index=['HedgeType', 'Plant'], 
                          columns=['Month'], 
                          values=['PeakWD', 'PeakWE', 'Off-Peak'],
                          aggfunc='sum')

invest_pivot_df = invest_df.pivot_table(index=['HedgeType', 'Plant'], 
                          columns=['Month'], 
                          values=['PeakWD', 'PeakWE', 'Off-Peak'],
                          aggfunc='sum')

combined_pivot_df = pd.concat([mw_pivot_df, invest_pivot_df], axis=1)
num_mw_columns = len(mw_pivot_df.columns)  
num_invest_columns = len(invest_pivot_df.columns)  

new_top_level_labels = ['Held MWs'] * num_mw_columns + ['Invest ($)'] * num_invest_columns
new_columns = pd.MultiIndex.from_tuples([(top_level,) + col for top_level, col in zip(new_top_level_labels, combined_pivot_df.columns)])
combined_pivot_df.columns = new_columns

# Export to Excel
with pd.ExcelWriter(OUTPUT_PATH + "/invest_report.xlsx", engine='xlsxwriter') as writer:
    combined_pivot_df.to_excel(writer)
