#%%
import requests
import pandas as pd
import numpy as np
from io import StringIO
import datetime
import os

my_auth = ('transmission.yesapi@calpine.com', 'texasave717')
periodst = '01/01/2020'
perioded = '01/01/2026'
PortfolioID = '759847'

time_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/05 - Schedules/Timeofuse"
outputroot = "//pzpwcmfs01/ca/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/ERCOT Basis Analysis/ERCOT_historical_basis_assets.csv"

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

"""
Aggregate all time of use data through the time_file directory.  This procedure results in a 3D array weights, with
weights[i] corresponding to the time of use information for year 2013 + i. 

Each weights[i] is a 2D array with weights[i][j] resulting in an array of three elements: OFFPEAk, PEAKWD, PEAKWE,
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
    
#%%                       
"""
Make requests to Yes Energy - aggregate historical data to 'merge_final_history'.
"""
call1 = f"https://services.yesenergy.com/PS/rest/ftr/portfolio/{PortfolioID}/paths.csv?"
call_one = requests.get(call1, auth=my_auth)
df1 = pd.read_csv(StringIO(call_one.text))

df1['Path'] = df1['SOURCE'] + '+' + df1['SINK']

df1['AUCTIONDATE'] = pd.to_datetime(df1['AUCTIONDATE'])
df1['CONTRACTSTARTDATE'] = pd.to_datetime(df1['CONTRACTSTARTDATE'])
df1['CONTRACTENDDATE'] = pd.to_datetime(df1['CONTRACTENDDATE'])

df1 = df1[['Path', 'SOURCE', 'SINK', 'PEAKTYPE', 'PATHSIZE', 'COSTOVERRIDE', 'CONTRACTSTARTDATE', 'CONTRACTENDDATE', 'HEDGETYPE']]


call2 = f"https://services.yesenergy.com/PS/rest/ftr/portfolio/{PortfolioID}/pathanalysis.csv?hedgetype=Obligation&items=DA%20congestion,RT%20LMP,Annual%20Auction%20Seq6,Annual%20Auction%20Seq5,Annual%20Auction%20Seq4,Annual%20Auction%20Seq3,Annual%20Auction%20Seq2,Annual%20Auction%20Seq1,Monthly%20Auction&startdate={periodst}&enddate={perioded}&units=$/MWH"
print(call2)
call_two = requests.get(call2, auth=my_auth)
df2a = pd.read_csv(StringIO(call_two.text))

#%%   

call2 = f"https://services.yesenergy.com/PS/rest/ftr/portfolio/{PortfolioID}/pathanalysis.csv?hedgetype=Option&items=DA%20congestion,Annual%20Auction%20Seq6,Annual%20Auction%20Seq5,Annual%20Auction%20Seq4,Annual%20Auction%20Seq3,Annual%20Auction%20Seq2,Annual%20Auction%20Seq1,Monthly%20Auction&startdate={periodst}&enddate={perioded}&units=$/MWH"
print(call2)
call_two = requests.get(call2, auth=my_auth)
df2b = pd.read_csv(StringIO(call_two.text))

df2b.loc[:, 'RT LMP'] = 0

df2 = pd.concat([df2a, df2b], sort=False)

cols_to_date = ['PERIODSTARTDATE', 'PERIODENDDATE']
for cols in cols_to_date:
    df2[cols] = pd.to_datetime(df2[cols])

df2['Path'] = df2['SOURCE'] + '+' + df2['SINK']

merge_initial = pd.merge(df1, df2, how='left', left_on=['Path', 'PEAKTYPE', 'HEDGETYPE'], right_on=['Path', 'PEAKTYPE', 'HEDGETYPE'])
merge_final = merge_initial[['Path', 'SOURCE_x', 'SINK_x', 'PEAKTYPE', 'HEDGETYPE', 'PERIODSTARTDATE', 'DA congestion', 'RT LMP', 'Annual Auction Seq6', 'Annual Auction Seq5', 'Annual Auction Seq4', 'Annual Auction Seq3', 'Annual Auction Seq2', 'Annual Auction Seq1', 'Monthly Auction']]

merge_final = merge_final.rename(columns={'SOURCE_x': 'Sourcename', 'SINK_x': 'Sinkname', 'PERIODSTARTDATE': 'Period'})
merge_final.columns = [col.replace('Annual Auction ', '') for col in merge_final.columns]

merge_final['Monthly Auction PnL'] = merge_final['DA congestion'] - merge_final['Monthly Auction']
merge_final['Seq1 PnL'] = merge_final['DA congestion'] - merge_final['Seq1']
merge_final['Seq2 PnL'] = merge_final['DA congestion'] - merge_final['Seq2']
merge_final['Seq3 PnL'] = merge_final['DA congestion'] - merge_final['Seq3']
merge_final['Seq4 PnL'] = merge_final['DA congestion'] - merge_final['Seq4']
merge_final['Seq5 PnL'] = merge_final['DA congestion'] - merge_final['Seq5']
merge_final['Seq6 PnL'] = merge_final['DA congestion'] - merge_final['Seq6']

merge_final_history = merge_final[merge_final['Period'] < datetime.datetime.now()]

#%%
"""
Compute a new DataFrame with the weighted averages of all numerical quantities from merge_final_history
"""
merge_final_history = merge_final_history.drop_duplicates()
merge_final_history = merge_final_history.fillna(0)

# Group together the original DataFrame off of Path, HedgeType, and Period
groups = merge_final_history.groupby(["Path", "Sourcename", "Sinkname", "HEDGETYPE", "Period"])

def weighted_average(group, column):
    # Only do the grouping if there is data for OFFPEAK, WDPEAK, and WEPEAK.
    if len(group[column]) == 3:
        years = group["Period"].dt.year - 2013
        months = group["Period"].dt.month - 1
        
        group_yr = years.iloc[0]
        group_month = months.iloc[0]
                
        return np.average(group[column], weights=weights[group_yr][group_month])

averaging_columns = merge_final_history.columns[6:]

# Initialize an empty DataFrame to store the results
averaged_dfs = pd.DataFrame()

# Iterate through each column, apply the function, and concatenate the results
for col in averaging_columns:
    weighted_avg_col = groups.apply(lambda g: weighted_average(g, col))
    weighted_avg_col.name = col 
    averaged_dfs = pd.concat([averaged_dfs, weighted_avg_col], axis=1)


#%%
averaged_dfs = averaged_dfs.reset_index()
rename_mapping =  {"level_0": "Path", "level_1": "Sourcename", "level_2": "Sinkname", "level_3": "HEDGETYPE", "level_4": "Period"}

averaged_dfs = averaged_dfs.rename(columns=rename_mapping)
averaged_dfs = averaged_dfs.dropna()

averaged_dfs["PEAKTYPE"] = "24HR"
combined_df = pd.concat([merge_final_history, averaged_dfs], axis=0)

#%%
combined_df = combined_df.sort_values(by=["Period", "PEAKTYPE"])
combined_df.to_csv(outputroot, index=False)