import pandas as pd
import os
from datetime import datetime
import glob
import numpy as np

NODE_PLANT_MAPPING = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/501 - Templates/Extracts/NodePlantMapping.csv"
PATH_BASE = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/06 - CRR/Semi-Annual/2024-2H/Auction/E-S2/Market Results"
OUTPUT_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Semi-Annual Auction Summary Reports"

file_matches = glob.glob(os.path.join(PATH_BASE, "Private_*_AUCTION.CSV"))
if len(file_matches) > 0:
    private_auction_file = file_matches[0]

def find_and_preprocess(node_path, auction_file):
    node_plant_df = pd.read_csv(node_path)
    plant_mapping = dict(zip(node_plant_df['Path'], node_plant_df['Plant']))
    
    """
    Read in and process the Monthly Auction File
    """
    auction_df = pd.read_csv(auction_file)
    relevant_columns = ["Source", "Sink", "StartDate", "EndDate", "TimeOfUse", "MW", "HedgeType", "BidPricePerMWH", "ShadowPricePerMWH"]
    auction_df = auction_df[relevant_columns]
    auction_df['Path'] = auction_df['Source'].astype(str) + "+" + auction_df['Sink'].astype(str)
    
    # Replace the Source column with the corresponding plant
    auction_df['Plant'] = auction_df['Path'].map(plant_mapping)
    auction_df.drop(columns=['Source', 'Sink', 'Path'], inplace=True)
    
    # Reformat the Start and EndDate columns
    auction_df["StartDate"] = pd.to_datetime(auction_df['StartDate'], format="%m/%d/%Y")
    auction_df["EndDate"] = pd.to_datetime(auction_df['EndDate'], format="%m/%d/%Y")
    
    return auction_df

def auction_cleared(auction_df: pd.DataFrame, month: int) -> pd.DataFrame:
    """
    Obtain the Auction Cleared Price
    """
    auction_df = auction_df[(auction_df["StartDate"].dt.month <= month) & (auction_df["EndDate"].dt.month >= month)]
    auction_df = auction_df.drop(columns=['StartDate', 'EndDate'])
    auction_SP_df = auction_df.groupby(['TimeOfUse', "Plant", "HedgeType"])["ShadowPricePerMWH"].min().reset_index()
    auction_SP_df = auction_SP_df.round(3)
    auction_SP_df = auction_SP_df.pivot_table(index=['Plant', 'HedgeType'], columns='TimeOfUse', values='ShadowPricePerMWH', aggfunc='max').reset_index()
    auction_SP_df.columns = ['Plant', 'HedgeType', 'Off-Peak', 'PeakWD', 'PeakWE']

    auction_SP_df = auction_SP_df.sort_values(by=['HedgeType', 'Plant'])
    auction_SP_df = auction_SP_df[['HedgeType', 'Plant', 'PeakWD', 'PeakWE', 'Off-Peak']]
    
    auction_SP_df["Month"] = month
    return auction_SP_df

def max_bid_price(auction_df: pd.DataFrame, month: int) -> pd.DataFrame:
    """
    Obtain the Max Bid Price
    """
    auction_df = auction_df[(auction_df["StartDate"].dt.month <= month) & (auction_df["EndDate"].dt.month >= month)]
    auction_df = auction_df.drop(columns=['StartDate', 'EndDate'])
    auction_bid_df = auction_df.groupby(['TimeOfUse', 'Plant', 'HedgeType'])['BidPricePerMWH'].max().reset_index()
    auction_bid_df = auction_bid_df.round(2)
    auction_bid_df = auction_bid_df.pivot_table(index=['Plant', 'HedgeType'], columns='TimeOfUse', values='BidPricePerMWH', aggfunc='max').reset_index()
    auction_bid_df.columns = ['Plant', 'HedgeType', 'Off-Peak', 'PeakWD', 'PeakWE']

    auction_bid_df = auction_bid_df.sort_values(by=['HedgeType', 'Plant'])
    auction_bid_df = auction_bid_df[['HedgeType', 'Plant', 'PeakWD', 'PeakWE', 'Off-Peak']]
    
    auction_bid_df['Month'] = month
    return auction_bid_df


def bid_price_difference(auction_bid_df: pd.DataFrame, auction_SP_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the Bid Price Difference 
    """
    auction_bid_df_numerical = auction_bid_df.select_dtypes(include=['float64'])
    auction_SP_df_numerical = auction_SP_df.select_dtypes(include=['float64'])
    numerical_diff_df = auction_bid_df_numerical - auction_SP_df_numerical

    non_numerical_columns = auction_bid_df.select_dtypes(exclude=['float64'])
    auction_diff_df = pd.concat([non_numerical_columns, numerical_diff_df], axis=1)
    auction_diff_df = auction_diff_df.round(2)

    return auction_diff_df

def post_process(auction_df: pd.DataFrame) -> pd.DataFrame:
    return auction_df.pivot_table(index=['HedgeType', 'Plant'], 
                          columns=['Month'], 
                          values=['PeakWD', 'PeakWE', 'Off-Peak'],
                          aggfunc='sum')

auction_df = find_and_preprocess(NODE_PLANT_MAPPING, private_auction_file)

cleared_df = pd.concat([auction_cleared(auction_df, m) for m in range(7, 13)], axis=0)
max_bid_df = pd.concat([max_bid_price(auction_df, m) for m in range(7, 13)], axis=0)
bid_price_diff = bid_price_difference(cleared_df, max_bid_df)

cleared_pivot_df = post_process(cleared_df)
bid_pivot_df = post_process(max_bid_df)
diff_pivot = post_process(bid_price_diff)

combined_pivot_df = pd.concat([cleared_pivot_df, bid_pivot_df, diff_pivot], axis=1)
# Calculate the number of columns for each original dataframe
num_cleared_columns = len(cleared_pivot_df.columns)
num_bid_price_columns = len(bid_pivot_df.columns)
num_diff_columns = len(diff_pivot.columns)


new_top_level_labels = ['Auction Cleared Price ($MWh)'] * num_cleared_columns + ['Calpine Max Bid Price ($MWh)'] * num_bid_price_columns + ['Bid Price Difference ($MWh)'] * num_diff_columns
new_columns = pd.MultiIndex.from_tuples([(top_level,) + col if isinstance(col, tuple) else (top_level, col) for top_level, col in zip(new_top_level_labels, combined_pivot_df.columns)])

combined_pivot_df.columns = new_columns
with pd.ExcelWriter(OUTPUT_PATH + "/bid_price_report.xlsx", engine='xlsxwriter') as writer:
    combined_pivot_df.to_excel(writer)



