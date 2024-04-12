import pandas as pd
import time

start_time = time.time()
GTC_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/GTC Aggregates/GTC Aggregator.csv"
MONTHLY_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Non-Thermal Constraint Aggregates/CRR_NonThermal_Constraints_Monthly.csv"
SEMI_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Non-Thermal Constraint Aggregates/CRR_NonThermal_Constraints_SemiAnnual.csv"

gtc_df = pd.read_csv(GTC_FILE)
monthly_df = pd.read_csv(MONTHLY_FILE)
semi_df = pd.read_csv(SEMI_FILE)




end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
