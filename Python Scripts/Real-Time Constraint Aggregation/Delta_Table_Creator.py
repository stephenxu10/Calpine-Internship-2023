from collections import defaultdict
import json
from itertools import product

import requests
import pandas as pd
import time
from io import StringIO
from typing import Dict, List, Tuple, Union

"""
This Python task aims to utilize the summary of the real-time constraint data of 2023 to generate
a table that contains the progression of Delta data for a certain set of paths.

The outputted table is located in the Data subfolder.
"""

# Global Variables and Parameters
start_time = time.time()
year = 2023

summary_path = "./../../Data/Aggregated RT Constraint Data/RT_Summary_" + str(year) + ".csv"
json_processed = "./../../Data/Aggregated RT Constraint Data/processed_" + str(year) + "summary.json"
output_path = "./../../Data/Aggregated RT Constraint Data/Delta_Table" + str(year) + ".csv"

# Grab the set of all paths that we are interested in
auth = ('transmission.yesapi@calpine.com', 'texasave717')

call1 = "https://services.yesenergy.com/PS/rest/ftr/portfolio/759847/paths.csv?"
r = requests.get(call1, auth=auth)
df = pd.read_csv(StringIO(r.text))
df['Path'] = df['SOURCE'] + '+' + df['SINK']
df = df[['Path', 'SOURCE', 'SINK']]
df = df.drop_duplicates()


unique_nodes = {'GUADG_CCU2', 'CCEC_ST1', 'BVE_UNIT1', 'PSG_PSG_GT3', 'SAN_SANMIGG1', 'CCEC_GT1', 'DDPEC_GT4', 'BOSQ_BSQSU_5', 'CTL_GT_104', 'BTE_BTE_G3', 'MIL_MILG345', 'BTE_BTE_G4', 'DUKE_GST1CCU', 'CAL_PUN2', 'DDPEC_GT6', 'HB_WEST', 'BOSQ_BSQS_12', 'BTE_BTE_G1', 'TXCTY_CTA', 'JACKCNTY_STG', 'LZ_WEST', 'DDPEC_GT2', 'STELLA_RN', 'BOSQ_BSQS_34', 'DC_E', 'CTL_GT_103', 'NED_NEDIN_G2', 'FREC_2_CCU', 'TXCTY_CTB', 'HB_SOUTH', 'HB_NORTH', 'GUADG_CCU1', 'NED_NEDIN_G3', 'LZ_SOUTH', 'NED_NEDIN_G1', 'JCKCNTY2_ST2', 'BTE_BTE_G2', 'DUKE_GT2_CCU', 'CAL_PUN1', 'PSG_PSG_GT2', 'DDPEC_GT3', 'DDPEC_ST1', 'BVE_UNIT2', 'FREC_1_CCU', 'BTE_PUN1', 'LZ_LCRA', 'BVE_UNIT3', 'BTE_PUN2', 'TEN_CT1_STG', 'CHE_LYD2', 'PSG_PSG_ST1', 'CHE_LYD', 'TXCTY_CTC', 'TXCTY_ST', 'CTL_ST_101', 'WND_WHITNEY', 'LZ_HOUSTON', 'LZ_NORTH', 'CTL_GT_102', 'HB_HOUSTON', 'CHEDPW_GT2', 'CCEC_GT2', 'DDPEC_GT1'}


"""
Pre-processes the summary CSV data into a nested dictionary in order to accelerate the 
searching & aggregation process. 

Input:
    - data_path: A relative path to the summary data.

Output:
    - A pre-processed dictionary.
"""
def pre_process(data_path: str) -> Dict[str, Dict[str, List[Tuple[str, str, str, float]]]]:
    raw_data = pd.read_csv(data_path)
    raw_data = raw_data[raw_data['Settlement_Point'].isin(unique_nodes)]

    res = defaultdict(lambda: defaultdict(list))

    for _, row in raw_data.iterrows():
        settlement = row['Settlement_Point']
        full_date = row['SCED_Time_Stamp']
        peak_type = row['PeakType']
        constraint = row['Constraint_Name']
        contingency = row['Contingency_Name']
        shadowShift = float(row['Shadow_Price']) * float(row['Shift_Factor'])

        res[settlement][full_date].append((contingency, constraint, peak_type, shadowShift))
    
    with open(json_processed, "w") as file:
        json.dump(res, file)
        
    return res


"""


"""
def accumulate_data(mapping: Dict, source: str, sink: str) -> Union[pd.DataFrame, None]:
    if source not in mapping or sink not in mapping:
        return None

    columns = ['Date', 'PeakType', 'Constraint', 'Contingency', 'Path', 'Source SF', 'Sink SF', 'Delta']
    data = [[] for _ in columns]

    source_map = mapping[source]
    sink_map = mapping[sink]

    for date, source_list in source_map.items():
        if date in sink_map:
            sink_list = sink_map[date]

            for source_item, sink_item in product(source_list, sink_list):
                contin, constr, peak, ss = source_item
                sink_contin, sink_constr, sink_peak, sink_ss = sink_item

                if contin == sink_contin and constr == sink_constr and peak == sink_peak:
                    data[0].append(date)
                    data[1].append(peak)
                    data[2].append(constr)
                    data[3].append(contin)
                    data[4].append(f"{source} {sink}")
                    data[5].append(source)
                    data[6].append(sink)
                    data[7].append(ss - sink_ss)

    res = pd.DataFrame(dict(zip(columns, data)))
    return res


with open(json_processed, "r") as map_file:
    mapping = json.load(map_file)

final_merge = []
for _, row in df.iterrows():
    source = row['SOURCE']
    sink = row['SINK']
    
    df_path = accumulate_data(mapping, source, sink)
    
    if df_path is not None:
        final_merge.append(df_path)
        print("yay!")
    
df_merged = pd.concat(final_merge, axis=0)
df_merged.to_csv(output_path, index=False)
    
# Output summary statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
