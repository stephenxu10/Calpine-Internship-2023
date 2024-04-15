import pandas as pd
import time

start_time = time.time()

GTC_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/GTC Aggregates/GTC_Aggregator.csv"
MONTHLY_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Non-Thermal Constraint Aggregates/CRR_NonThermalConstraints_Monthly.csv"
SEMI_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Non-Thermal Constraint Aggregates/CRR_NonThermalConstraints_SemiAnnual.csv"

OUTPUT_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/GTC Aggregates/GTC_Aggregator_MERGED.csv"

gtc_df = pd.read_csv(GTC_FILE)
monthly_df = pd.read_csv(MONTHLY_FILE)
semi_df = pd.read_csv(SEMI_FILE)

gtc_df['MARKETDAY'] = pd.to_datetime(gtc_df['MARKETDAY'])
gtc_df['Year'] = gtc_df['MARKETDAY'].dt.year
gtc_df['Month'] = gtc_df['MARKETDAY'].dt.month

monthly_df.rename(columns={'Name': 'NAME'}, inplace=True)
semi_df.rename(columns={'Name': 'NAME'}, inplace=True)
merged_df = pd.merge(gtc_df, monthly_df[['Year', 'Month', 'NAME', 'Limit']], 
                     on=['Year', 'Month', 'NAME'], 
                     how='left')

merged_df = merged_df.rename(columns={"Limit": "CRR Limit"})
merged_df = merged_df.drop_duplicates()

final_df = pd.merge(merged_df, semi_df[['Year', 'Month', 'NAME', 'Seq1', 'Seq2', 'Seq3', 'Seq4', 'Seq5', 'Seq6']], on=['Year', 'Month', 'NAME'], how='left')
final_df = final_df.drop_duplicates(subset=['MARKETDAY', 'NAME', 'HOURENDING']).drop(columns=['Year', 'Month'])
final_df.to_csv(OUTPUT_FILE, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
