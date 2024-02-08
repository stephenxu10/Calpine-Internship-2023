import requests
import pandas as pd
from io import StringIO
import concurrent.futures
from datetime import datetime, timedelta
import time
import logging
from requests.exceptions import HTTPError
import warnings

warnings.simplefilter("ignore")

AUTH_CREDENTIALS = ('transmission.yesapi@calpine.com', 'texasave717')
start_time = time.time()

SLEEP_TIME = 200
outputroot = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\01 - General\\Extracts\\DAM_ENERGY_SOLD_DELTA.csv'
outputroot_hr = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\01 - General\\Extracts\\DAM_ENERGY_SOLD_DELTA_HOURLY.csv'
MAPPING_FILE = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/06 - CRR/02 - Summary/MappingDocument/MapResources_24.01.30.xlsx"
BASE_URL = "https://services.yesenergy.com/PS/rest/timeseries/"
iso = 'ERCOT'
PEAKTYPE = ('WDPEAK', 'WEPEAK', 'OFFPEAK')
ITEMSIZE = 75

startdate = 'today-120'
enddate = 'today+1'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Read in the initial OBJECTIDs into a DataFrame
call1 = "https://services.yesenergy.com/PS/rest/timeseries/TOTAL_DAM_ENERGY_SOLD.csv?ISO="+iso+""
print(call1)
call_one = requests.get(call1, auth=AUTH_CREDENTIALS)
df1 = pd.read_csv(StringIO(call_one.text))

# Elliminates Load Zones and Hubs
df1 = df1[~df1['NAME'].str.startswith(('LZ_', 'HB_', 'BRP_'))]
print(df1.info())


def generate_node_list(key: str, df):
    """
    Basic helper method that generates a list of nodes, with each entry having
    CHUNKSIZE nodes

    Parameters
    ----------
    key : str
        label at the beginning of each OBJECTID.
    df : DataFrame
        A DataFrame with an OBJECTID column.

    Returns
    -------
    list
        the list of nodes as specified above.
    """
    url_string = [f"{key}:{i}" for i in df.OBJECTID]
    return [','.join(url_string[i:i + ITEMSIZE]) for i in range(0, len(url_string), ITEMSIZE)]


node_list = generate_node_list("TOTAL_DAM_ENERGY_SOLD", df1)

def get_data_from_api(url: str, params=None):
    """
    Makes a GET request to the specified URL and return the data as a DataFrame.

    Parameters
    ----------
    url : str
        Base URL to query to.
    params : dict, optional
        Additional parameters to the query URL. The default is None.

    Returns
    -------
    DataFrame
        A DataFrame containing the queried data. Any invalid and/or unsuccessful
        queries are logged to the console, and an empty DataFrame is returned
        as a result.
    """
    try:
        response = requests.get(url, auth=AUTH_CREDENTIALS, params=params)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text))
    except HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.error(f'Other error occurred: {err}')
    return pd.DataFrame()

def fetch_data(params):
    """
    Wrapper method around get_data_from_api to be used for
    parallelized processes. Also drops DATETIME, MONTH, and YEAR
    columns.
    """
    call_url = f"{BASE_URL}multiple.csv?"
    df = get_data_from_api(call_url, params)
    if not df.empty:
        df = df.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)
        print(df.info())
        return df
    else:
        return None
    
def collect_data(node_list) -> pd.DataFrame:
    """
    Aggregates data for multiple nodes after a specified start date assisted through concurrent futures.
    """
    dataframes = []
    params_list = [
        {
            'agglevel': 'hour',
            'startdate': startdate,
            'enddate': enddate,
            'items': nodes
        }
        for nodes in node_list
    ]

    # IMPORTANT: Yes Energy enforces a limit of seven concurrent requests per user. max_workers should
    # never be greater than 7.
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_df = {executor.submit(fetch_data, params): params for params in params_list}
        for future in concurrent.futures.as_completed(future_to_df):
            df = future.result()
            if df is not None:
                dataframes.append(df)
            else:
                params = future_to_df[future]
                logging.error(f"Failed to fetch data for params: {params}")

    return pd.concat(dataframes, axis=1)

#%%
df2 = collect_data(node_list)
time.sleep(SLEEP_TIME)
df2 = df2.loc[:, ~df2.columns.duplicated()]

# Removing ' (TOTAL_DAM_ENERGY_SOLD)' from the column headers
df5 = df2.rename(columns={col: col.split(
    ' (TOTAL_DAM_ENERGY_SOLD)')[0] for col in df2.columns})
df5 = df5.fillna(0)

df6 = pd.melt(df5, id_vars=["HOURENDING", "MARKETDAY", "PEAKTYPE"], var_name="NODENAME",
              value_name="MW")
df6['MARKETDAY'] = pd.to_datetime(df6['MARKETDAY'])

# Calculate the date 30 days before today
thirty_days_ago = datetime.now() - timedelta(days=30)

# Filter the DataFrame
dam_df = df6[df6['MARKETDAY'] >= thirty_days_ago]

dam_df['Delta'] = dam_df.groupby(['NODENAME', 'PEAKTYPE'])[
    'MW'].diff().fillna(0)
dam_df.sort_index(inplace=True)

df6_daily = df6.groupby(['MARKETDAY', 'PEAKTYPE', 'NODENAME'], as_index=False)[
    'MW'].mean()

# This modified version computes the delta column by grouping together each (Node, Peaktype) combination
# and subtracting the current day MW from the next day MW.
df6_daily = df6_daily.sort_values(by=['NODENAME', 'PEAKTYPE', 'MARKETDAY'])
df6_daily['Delta'] = df6_daily.groupby(['NODENAME', 'PEAKTYPE'])[
    'MW'].diff().fillna(0)
df6_daily.sort_index(inplace=True)

# Obtain the DA LMP values for each NODENAME and merge into the other tables
node_list_dalmp = generate_node_list("DALMP", df1)
df_DALMP = collect_data(node_list_dalmp)
df_DALMP = df_DALMP.loc[:, ~df_DALMP.columns.duplicated()]

df_DALMP = df_DALMP.rename(columns={col: col.split(
    ' (DALMP)')[0] for col in df_DALMP.columns})

df_DALMP = pd.melt(df_DALMP, id_vars=["HOURENDING", "MARKETDAY", "PEAKTYPE"], var_name="NODENAME",
              value_name="DALMP")

#%%
df_DALMP_daily = df_DALMP.groupby(['MARKETDAY', 'PEAKTYPE', 'NODENAME'], as_index=False)["DALMP"].mean()
df_DALMP_daily['MARKETDAY'] = pd.to_datetime(df_DALMP_daily['MARKETDAY'])
df_DALMP['MARKETDAY'] = pd.to_datetime(df_DALMP['MARKETDAY'])

df_merged_daily = pd.merge(df_DALMP_daily, df6_daily, on=["MARKETDAY", "PEAKTYPE", "NODENAME"], how='inner')

df_DALMP_30 = df_DALMP[df_DALMP['MARKETDAY'] >= thirty_days_ago]
df_merged_hourly = pd.merge(df_DALMP_30, dam_df, on=["MARKETDAY", "PEAKTYPE", "HOURENDING", "NODENAME"], how='inner')

df_merged_daily = df_merged_daily.sort_values(by=["MARKETDAY", "NODENAME"]).dropna()
df_merged_hourly = df_merged_hourly.sort_values(by=["MARKETDAY", "HOURENDING", "NODENAME"]).dropna()

#%%
# Add in the node mapping data
mapping_df = pd.read_excel(MAPPING_FILE, sheet_name="Sheet1")
mapping_alt = mapping_df.drop(columns=["CODE_COMBINED"]).dropna(subset=["CC Node"])
mapping_df = mapping_df.drop(columns=["CC Node"])

mapping_alt['INSTALLED_CAPACITY_RATING_(MW)'] = mapping_alt.groupby("CC Node")['INSTALLED_CAPACITY_RATING_(MW)'].transform("sum")
mapping_alt = mapping_alt.drop_duplicates(subset=["CC Node"]).sort_values(by=['CC Node'])

def post_process(raw_df: pd.DataFrame, mapping: pd.DataFrame, mapping_alt: pd.DataFrame, hourly=True) -> pd.DataFrame:
    main_merge = pd.merge(raw_df, mapping, left_on="NODENAME", right_on="CODE_COMBINED", how="left")
    alt_merge = pd.merge(raw_df, mapping_alt, left_on="NODENAME", right_on="CC Node", how="left")
    combined_df = main_merge.combine_first(alt_merge)
    
    post_df = combined_df.drop_duplicates(subset=["MARKETDAY", "NODENAME", "PEAKTYPE", "DALMP", "MW", "Delta"])
    post_df = post_df.drop(columns=['CC Node', 'CODE_COMBINED', 'LINK'], errors='ignore')
    
    base_order = [
    'MARKETDAY', 'PEAKTYPE', 'NODENAME', 'DALMP', 'MW', 'Delta', 
    'NAME_COMBINED', 'OWNER', 'FUEL', 'COUNTY', 'ZONE', 'IN_SERVICE', 
    'INSTALLED_CAPACITY_RATING_(MW)'
    ] 
    hourly_order = [
    'MARKETDAY', 'HOURENDING', 'PEAKTYPE', 'NODENAME', 'DALMP', 'MW', 'Delta', 
    'NAME_COMBINED', 'OWNER', 'FUEL', 'COUNTY', 'ZONE', 'IN_SERVICE', 
    'INSTALLED_CAPACITY_RATING_(MW)'
    ] 
    
    post_df = post_df.reindex(columns=hourly_order if hourly else base_order)
    return post_df

post_daily = post_process(df_merged_daily, mapping_df, mapping_alt, False)
post_hourly = post_process(df_merged_hourly, mapping_df, mapping_alt)

post_daily.to_csv(outputroot, index=False)
post_hourly.to_csv(outputroot_hr, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
