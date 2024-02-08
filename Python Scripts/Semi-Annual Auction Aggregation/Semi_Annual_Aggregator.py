import os
import time
import pandas as pd

"""
This Python script aims to aggregate Common Binding Constraint and Private Auction files for historical
and current Semi-Annual Auction data.

Outputs the resulting CSVs to the Data folder.
"""

# Global parameters & variables
start_time = time.time()
min_year = 2022

# Relative file path of the outputted CSV.
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Semi-Annual Auction Data/"
mapping_path = r"\\pzpwcmfs01\CA\11_Transmission Analysis\ERCOT\501 - Templates\Extracts\NodePlantMapping.csv"
base_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/Semi-Annual/"

relevant_folders = ["A-S6", "B-S5", "C-S4", "D-S3", "E-S2", "F-S1"]

# Read in the mapping file
mapping_df = pd.read_csv(mapping_path)
path_plant = dict(zip(mapping_df['Path'], mapping_df['Plant']))

def extract_csv(time_frame: str, type: int) -> pd.DataFrame:
    time_path = os.path.join(base_path, time_frame, "Auction")
    merged = []

    if not os.path.isdir(time_path):
        print("extract_csv: input time frame auction path does not exist: ", time_path)
        return
    
    for sub_folder in os.listdir(time_path):
        if sub_folder in relevant_folders:
            market_folder = os.path.join(time_path, sub_folder, "Market Result")
            sub_market_res = market_folder if os.path.exists(market_folder) else os.path.join(time_path, sub_folder, "Market Results")

            if not os.path.exists(sub_market_res):
                print("extract_csv: market result file path does not exist: ", sub_market_res)
                return
            
            if type == 0:
                for auction_file in os.listdir(sub_market_res):
                    if "Common_BindingConstraint_" in auction_file:
                        auction_df = pd.read_csv(os.path.join(sub_market_res, auction_file))
                        auction_df["Auction Name"] = auction_file[25:]
                        auction_df = auction_df[['Auction Name'] + [col for col in auction_df.columns if col != 'Auction Name']]
                        merged.append(auction_df)
            
            elif type == 1:
                for auction_file in os.listdir(sub_market_res):
                    if "Private_" in auction_file and auction_file.endswith("CSV"):
                        auction_df = pd.read_csv(os.path.join(sub_market_res, auction_file))
                        auction_df["Auction Name"] = auction_file[8:]
                        auction_df = auction_df[['Auction Name'] + [col for col in auction_df.columns if col != 'Auction Name']]
                        merged.append(auction_df)
            
            else:
                print("extract_csv: only types 0 and 1 are allowed - unsupported type ", type)
                return
    
    return pd.concat(merged, axis=0)

cbc = []
# Combine all the Common Binding Constraint Files
for sub_folder in os.listdir(base_path):
    if sub_folder.startswith("20") and int(sub_folder[:4]) >= min_year:
        cbc.append(extract_csv(sub_folder, 0))

merged_cbc = pd.concat(cbc, axis=0)
merged_cbc.to_csv(output_path + "Common_BindingConstraint_Aggregate.csv", index=False)

# Combine all the Private Auction Files
private = []
for sub_folder in os.listdir(base_path):
    if sub_folder.startswith("20") and int(sub_folder[:4]) >= min_year:
        private.append(extract_csv(sub_folder, 1))

merged_private = pd.concat(private, axis=0)
merged_private['Path'] = merged_private['Source'].astype(str) + "+" + merged_private['Sink'].astype(str)
plant_column = merged_private['Path'].map(path_plant)

merged_private.insert(11, "Plant", plant_column)
merged_private.drop(columns=['Path'], inplace=True)
merged_private.to_csv(output_path + "Private_Auction_Aggregate.csv", index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")