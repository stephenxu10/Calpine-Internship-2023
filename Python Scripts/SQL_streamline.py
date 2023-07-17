import pandas as pd

# Data preparation
caseinfo_df = pd.DataFrame({
    'scenarioid': [1, 2],
    'name': ['P220929_2022_R11O_C220928', 'P220929_2023_R15O_C220928'],
    'modifieddttm': ['2022-01-01', '2023-01-01'],
    'start_date': ['2022-01-01', '2023-01-01'],
    'end_date': ['2022-12-31', '2023-12-31'],
    'zonescenario': [100, 101]
})

outpricehublmp_30_df = pd.DataFrame({
    'YEAR': [2023, 2023],
    'MONTH': [1, 1],
    'hubname': ['HUB_PJMW', 'MP_CPN BETHL230'],
    'price': [50.25, 47.80],
    'energy': [20.5, 18.2],
    'congestion': [5.1, 4.8],
    'marginal': [10.2, 9.5],
    'peak_offpeak_flag': [0, 1]
})

outpriceoutputzonallmp_30_df = pd.DataFrame({
    'YEAR': [2023, 2023],
    'MONTH': [1, 1],
    'zone_name': ['PJM_PECO', 'PJM_DPL'],
    'buyer_price': [48.5, 52.0],
    'lmp_energy': [18.5, 21.0],
    'lmp_congestion': [4.2, 5.5],
    'lmp_marginal': [9.8, 10.5],
    'peak_offpeak_flag': [0, 3]
})

# Update scenario names and IDs
scenario_names_to_update = ['P220929_2022_R11O_C220928', 'P220929_2023_R15O_C220928']
outpricehublmp_30_scenario_id = 1  # Update with the actual scenario ID for 'outpricehublmp_30'
outpriceoutputzonallmp_30_scenario_id = 2  # Update with the actual scenario ID for 'outpriceoutputzonallmp_30'

# Filter data based on scenario names and IDs
caseinfo_filtered = caseinfo_df[caseinfo_df['name'].isin(scenario_names_to_update)]
hub_prices_filtered = outpricehublmp_30_df[outpricehublmp_30_df['hubname'].isin(['HUB_PJMW', 'MP_CPN BETHL230', 'MP_CPN BETHL500', 'MP_CPN HAYRD1', 'MP_CPN HAYRD5', 'MP_CPN HR2', 'MP_CPN HR4', 'MP_CPN YORK'])]
zone_prices_filtered = outpriceoutputzonallmp_30_df[outpriceoutputzonallmp_30_df['zone_name'].isin(['PJM_PECO', 'PJM_PPL', 'PJM_DPL'])]

# Calculate TOU based on peak_offpeak_flag
def map_peak_offpeak_flag(peak_offpeak_flag):
    if peak_offpeak_flag == 0:
        return 'DailyOffPeak'
    elif peak_offpeak_flag == 1:
        return 'WkndOnPeak'
    elif peak_offpeak_flag == 3:
        return 'OnPeak'

hub_prices_filtered['TOU'] = hub_prices_filtered['peak_offpeak_flag'].apply(map_peak_offpeak_flag)
zone_prices_filtered['TOU'] = zone_prices_filtered['peak_offpeak_flag'].apply(map_peak_offpeak_flag)

# Calculate the average LMP and MCC for hub_prices and zone_prices DataFrames
hub_prices_avg_df = hub_prices_filtered.groupby(['YEAR', 'MONTH', 'hubname', 'TOU']).mean().reset_index()
zone_prices_avg_df = zone_prices_filtered.groupby(['YEAR', 'MONTH', 'zone_name', 'TOU']).mean().reset_index()

# Pivot the DataFrames to get desired format
hub_prices_pivot_df = hub_prices_avg_df.pivot_table(index=['PRICE', 'hubname', 'TOU'], columns='MONTH', values='price', aggfunc='mean')
zone_prices_pivot_df = zone_prices_avg_df.pivot_table(index=['PRICE', 'zone_name', 'TOU'], columns='MONTH', values='buyer_price', aggfunc='mean')

# Combine the pivoted DataFrames
combined_df = pd.concat([hub_prices_pivot_df, zone_prices_pivot_df], axis=0)

# Calculate the 'uplan_basis' DataFrame
uplan_basis_df = combined_df.copy()
uplan_basis_df['PECO_BS'] = uplan_basis_df['PJM_PECO'] - uplan_basis_df['HUB_PJMW']
uplan_basis_df['PPL_BS'] = uplan_basis_df['PJM_PPL'] - uplan_basis_df['HUB_PJMW']
uplan_basis_df['DPL_BS'] = uplan_basis_df['PJM_DPL'] - uplan_basis_df['HUB_PJMW']
uplan_basis_df['PECO_PPL_BS'] = uplan_basis_df['PJM_PECO'] - uplan_basis_df['PJM_PPL']
uplan_basis_df['HR13_BS'] = uplan_basis_df['MP_CPN HAYRD1'] - uplan_basis_df['PJM_PECO']
uplan_basis_df['HR2_BS'] = uplan_basis_df['MP_CPN HR2'] - uplan_basis_df['PJM_PECO']
uplan_basis_df['HR4_BS'] = uplan_basis_df['MP_CPN HR4'] - uplan_basis_df['PJM_PECO']
uplan_basis_df['HR5T8_BS'] = uplan_basis_df['MP_CPN HAYRD5'] - uplan_basis_df['PJM_PECO']
uplan_basis_df['York_BS'] = uplan_basis_df['MP_CPN YORK'] - uplan_basis_df['PJM_PECO']
uplan_basis_df['BETH1T7_BS'] = uplan_basis_df['MP_CPN BETHL500'] - uplan_basis_df['PJM_PPL']
uplan_basis_df['BETH8_BS'] = uplan_basis_df['MP_CPN BETHL230'] - uplan_basis_df['PJM_PPL']

# Output the result to a CSV file
output_csv_file = 'result.csv'
uplan_basis_df.to_csv(output_csv_file, index=True)

print(f"Output saved to {output_csv_file}")
