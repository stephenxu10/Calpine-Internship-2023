import pandas as pd
import os
from datetime import datetime
import glob

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

def find_and_preprocess(node_path, path_base):
    """
    Parse in the Node-Plant Mapping
    """
    node_plant_df = pd.read_csv(node_path)
    plant_mapping = dict(zip(node_plant_df['Path'], node_plant_df['Plant']))

    today = datetime.now()
    year = today.year
    month = today.month

    """
    Check for the most Monthly Auction Report
    """
    # Increment the month
    next_month = month + 1

    if next_month == 13:
        next_month = 1
        year += 1

    next_month_f = f"{next_month:02d}"
    monthly_path = os.path.join(path_base, str(year), f"{year}-{next_month_f}", "Market Results")

    if not os.path.exists(monthly_path):
        print(f"ERROR: Auction data does not exist for year {year}, month {next_month}")
        exit()

    file_matches = glob.glob(os.path.join(monthly_path, "Private_*_AUCTION.CSV"))
    if len(file_matches) > 0:
        # Should only be one match.
        private_auction_file = file_matches[0]

    else:
        print(f"ERROR: No Auction Data is available for the next month.")
        exit()

    """
    Read in and process the Monthly Auction File
    """
    auction_df = pd.read_csv(private_auction_file)
    relevant_columns = ["Source", "Sink", "TimeOfUse", "HedgeType", "ShadowPricePerMWH", "BidPricePerMWH", "ACI99"]
    auction_df = auction_df[relevant_columns]
    auction_df = auction_df[auction_df['HedgeType'] == "OPT"]
    auction_df = auction_df.drop(columns=['HedgeType'])
    auction_df['Path'] = auction_df['Source'].astype(str) + "+" + auction_df['Sink'].astype(str)

    # Replace the Source column with the corresponding plant
    auction_df['Plant'] = auction_df['Path'].map(plant_mapping)
    auction_df.drop(columns=['Source', 'Sink', 'Path'], inplace=True)

    return auction_df

def auction_cleared(auction_df: pd.DataFrame) -> pd.DataFrame:
    """
    Obtain the Auction Cleared Price
    """
    auction_SP_df = auction_df.groupby(['TimeOfUse', "Plant"])["ShadowPricePerMWH"].min().reset_index()
    auction_SP_df = auction_SP_df.round(3)
    auction_SP_df = auction_SP_df.pivot_table(index=['Plant'], columns='TimeOfUse', values='ShadowPricePerMWH', aggfunc='max').reset_index()
    auction_SP_df.columns = ['Plant', 'Off-Peak', 'PeakWD', 'PeakWE']

    auction_SP_df = auction_SP_df.sort_values(by=['Plant'])
    auction_SP_df = auction_SP_df[['Plant', 'PeakWD', 'PeakWE', 'Off-Peak']]

    return auction_SP_df

def max_bid_price(auction_df: pd.DataFrame) -> pd.DataFrame:
    """
    Obtain the Max Bid Price
    """
    auction_bid_df = auction_df.groupby(['TimeOfUse', 'Plant'])['BidPricePerMWH'].max().reset_index()
    auction_bid_df = auction_bid_df.round(2)
    auction_bid_df = auction_bid_df.pivot_table(index=['Plant'], columns='TimeOfUse', values='BidPricePerMWH', aggfunc='max').reset_index()
    auction_bid_df.columns = ['Plant', 'Off-Peak', 'PeakWD', 'PeakWE']

    auction_bid_df = auction_bid_df.sort_values(by=['Plant'])
    auction_bid_df = auction_bid_df[['Plant', 'PeakWD', 'PeakWE', 'Off-Peak']]

    return auction_bid_df

def bid_price_difference(auction_bid_df: pd.DataFrame, auction_SP_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the Bid Price Difference 
    """
    auction_bid_df_numerical = auction_bid_df.select_dtypes(include=['number'])
    auction_SP_df_numerical = auction_SP_df.select_dtypes(include=['number'])
    numerical_diff_df = auction_bid_df_numerical - auction_SP_df_numerical

    non_numerical_columns = auction_bid_df.select_dtypes(exclude=['number'])
    auction_diff_df = pd.concat([non_numerical_columns, numerical_diff_df], axis=1)
    auction_diff_df = auction_diff_df.round(2)

    return auction_diff_df

def generate_report() -> pd.DataFrame:
    auction_df = find_and_preprocess(NODE_PLANT_MAPPING, PATH_BASE)
    auction_SP_df = auction_cleared(auction_df)
    auction_bid_df = max_bid_price(auction_df)
    auction_diff_df = bid_price_difference(auction_bid_df, auction_SP_df)
    """
    Merge together the DataFrames and do some final reformatting
    """
    merged_df = pd.merge(auction_SP_df, auction_bid_df, on=['Plant'])
    final_merge = pd.merge(merged_df, auction_diff_df, on=['Plant'])

    multi_index_columns = pd.MultiIndex.from_tuples([
        ('', 'Plant'), 
        ('', 'WDPEAK'), ('Auction Cleared Price ($MWh)', 'WEPEAK'), ('', 'OFFPEAK'), 
        ('', 'WDPEAK'), ('Calpine Max Bid Price ($MWh)', 'WEPEAK'), ('', 'OFFPEAK'),
        ('', 'WDPEAK'), ('Bid Price Difference ($MWh)', 'WEPEAK'), ('', 'OFFPEAK')
    ])
    final_merge = final_merge.applymap(custom_format)
    final_merge.columns = multi_index_columns
    final_merge.to_csv(OUTPUT_PATH + "./bid_price_report.csv")


if  __name__ == "__main__":
    generate_report()
