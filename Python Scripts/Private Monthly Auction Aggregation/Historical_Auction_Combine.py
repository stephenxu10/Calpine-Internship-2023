from collections import defaultdict
from typing import Dict, Tuple
import pandas as pd
import time

"""
This Python script performs an aggregation of the grouped monthly auction data
with the ERCOT historical basis assets. For each entry in the ERCOT historical
assets, the corresponding entry (based on Path, Period, and HedgeType) is found
in the grouped auction CSV. Next, the Plant, Size, ShadowPricePerMWH, and
BidPricePerMWH and recorded and the columns are aggregated into a new
output file.

Note:
This script will only work properly if it is ran from
\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\101 - Misc\CRR Limit Aggregates
due to the File I/O.

The resultant output file is located under the data subfolder. This script
assumes the auction_combined_grouped CSV file has already been fully generated.
If any updates need to be made to that file, run AuctionCombine.py first.
"""

# Global Parameters and Variables
start_time = time.time()
auction_grouped = "./../../Data/Private_auction_grouped.CSV"
historical = "./../../Data/ERCOT Assets/ERCOT_historical_basis_assets.CSV"

output_path = "./../../Data/Aggregated Auction Data/ERCOT_historical_married.csv"

df_auction = pd.read_csv(auction_grouped)
df_historical = pd.read_csv(historical)

plants = []
sizes = []
shadowPrices = []
bidPrices = []
hedgeMapping = {"Option": "OPT", "Obligation": "OBL"}


def convertDate(date: str):
    year = date[:4]
    month = date[5:7]
    day = date[8:]

    return month + "/" + day + "/" + year

def process(df_auction) -> Dict[str, Dict[str, Dict[str, Tuple[str, str, str, str]]]]:
    """
    Helper method to accumulate the data inside df_auction into a
    large dictionary. This helps to reduce search time for each query in the historical ERCOT
    data to O(1) time, which dramatically speeds up the program.
    """
    auction_summary = defaultdict(lambda: defaultdict(dict))

    for _, row in df_auction.iterrows():
        path = row['Path']
        hedgeType = row['HedgeType']
        date = row['StartDate']
        plant = row['Plant']
        size = row['Size (MW)']
        shadowPrice = row['ShadowPricePerMWH']
        bidPrice = row['BidPricePerMWH']

        auction_summary[date][path][hedgeType] = (plant, size, shadowPrice, bidPrice)
    
    return auction_summary

def grab_data(rw, data):
    """
    Given a DataFrame row, this method grabs the plant, size, shadowPricePerMWH, and
    bidPricePerMWH from the auction_grouped CSV. Performs a simple linear search to
    find these values.

    Input:
        - rw: The DataFrame row from the ERCOT historical basis data
        - data: The pre-processed monthly auction grouped data.

    Output:
        - A four-element tuple storing the plant, size (MW), shadowPricePerMWH,
        and bidPricePerMWH
    """

    row_path = rw['Path']
    row_hedge = rw['HEDGETYPE']
    row_period = str(convertDate(rw['Period']))
    
    # Handle a strange case with August 2022, otherwise read the pre-processed dictionary
    if row_period in data or row_period == "08/01/2022":
        if row_period == "08/01/2022":
            row_period = "8/1/2022"
        if row_path in data[row_period]:
            if hedgeMapping[row_hedge] in data[row_period][row_path]:
                return data[row_period][row_path][hedgeMapping[row_hedge]]
            
    # Return empty entries if not found.
    return "", "", "", ""


# Pre-process the monthly auction data into a nested dictionary.
data = process(df_auction)

# Grab the desired extra data for each row in the ERCOT historical data.
for index, row in df_historical.iterrows():
    plant, size, shadowMWH, bidMWH = grab_data(row, data)

    plants.append(plant)
    sizes.append(size)
    shadowPrices.append(shadowMWH)
    bidPrices.append(bidMWH)


# Add these new columns to the aggregated DataFrame
df_historical['Plant'] = plants
df_historical['Size (MW)'] = sizes
df_historical['ShadowPricePerMWH'] = shadowPrices
df_historical['BidPricePerMWH'] = bidPrices


# Output the file and print total execution time
df_historical = pd.DataFrame(df_historical)
df_historical.to_csv(output_path, index=False)


end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
