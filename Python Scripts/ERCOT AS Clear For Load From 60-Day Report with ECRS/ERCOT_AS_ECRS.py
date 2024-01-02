#%%
import requests
import pandas as pd
from io import StringIO
import time
import pyodbc
import datetime
import logging
from requests.exceptions import HTTPError
import concurrent.futures
import warnings

pd.set_option('display.max_colwidth', None)
warnings.simplefilter("ignore")

"""
Gets Load Participation from the AS market from YES Energy. If using Spyder, you can optionally
import the 'data.spydata' file to the variable explorer to save same time.

NEW: This program handles potential YES Energy throttles through time.sleep(). No throttles are antipicated
currently, but consider adjusting the sleeping time if you wish for faster execution time.

As of now, we sleep for 280 seconds in between large data queries.
"""
# Constants
CHUNKSIZE = 75
SLEEP_TIME = 160
OUTPUT_ROOT = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\01 - General\\Extracts\\'
ISO = 'ERCOT'
BASE_URL = "https://services.yesenergy.com/PS/rest/timeseries/"
AUTH_CREDENTIALS = ('transmission.yesapi@calpine.com', 'texasave717') 

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

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
    return [','.join(url_string[i:i + CHUNKSIZE]) for i in range(0, len(url_string), CHUNKSIZE)]


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
    call_url = f"{BASE_URL}multiple.csv"
    df = get_data_from_api(call_url, params)
    if not df.empty:
        print(df.head())
        return df.drop(columns=['DATETIME', 'MONTH', 'YEAR'])
    else:
        return None

def collect_data(node_list, startdate, enddate):
    """
    Aggregates data from multiple nodes over a specified date range using concurrent futures.
    The function fetches data for each node in the node_list for the period between startdate and enddate.
    It concurrently processes these fetch requests and compiles the resulting data into a single DataFrame.

    Parameters
    ----------
    node_list : list
        A list of nodes genreated through generate_node_list
    startdate : str or datetime-like
        The starting date of the data fetching period
    enddate : str or datetime-like
        The ending date of the data fetching period

    Returns
    -------
    DataFrame
        A DataFrame containing the aggregated data from all the nodes. Each node's data is concatenated along the columns.
        If data fetching for a node fails, that node's data will be missing in the final DataFrame, and an error will be logged.
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

def post_process(df, new_column, drop_column):
    """
    Simple helper method that post-processes a queried DataFrame obtained
    from collect_data
    """
    df = df.loc[:,~df.columns.duplicated()]
    df[new_column] = df[[col for col in df.columns if col.endswith(drop_column)]].sum(axis=1)

    ## Dropping all the column containing indiviual load
    df = df.drop([col for col in df.columns if col.endswith(drop_column)], axis=1)
    return df


# Reading initial data
df3a = pd.read_csv(OUTPUT_ROOT + 'AS_ClearfromLoad_withquantity.csv')

startdate = (pd.to_datetime(df3a['MARKETDAY']).max() + datetime.timedelta(days=1)).strftime('%m/%d/%Y')
enddate = (datetime.datetime.now() + datetime.timedelta(days=-60)).date().strftime("%m/%d/%Y")

# Get RRS Clears for Loads
call1_url = f"{BASE_URL}ERCOT_DAM_LOAD_RRS_AWARDED.csv?ISO={ISO}"
df1 = get_data_from_api(call1_url)

#%% 
node_list_S = generate_node_list("ERCOT_DAM_LOAD_RRS_AWARDED", df1)
df2_S = collect_data(node_list_S, startdate, enddate)
df2_S = post_process(df2_S, 'DAM RRS Awarded', '(ERCOT_DAM_LOAD_RRS_AWARDED)')
print(df2_S.info())

logging.info("Sleeping to avoid throttling...")
time.sleep(SLEEP_TIME)

#%%
## Load which cleared NON SPIN
node_list_NS = generate_node_list("ERCOT_DAM_LOAD_NONSPIN_AWARDED", df1)
df2_NS = collect_data(node_list_NS, startdate, enddate)
df2_NS = post_process(df2_NS, 'DAM NS Awarded', '(ERCOT_DAM_LOAD_NONSPIN_AWARDED)')
print(df2_NS.info())

## Merge Spin and NonSpin Quantities
df2_S_NS = pd.merge(df2_S,df2_NS,how='inner',left_on=['HOURENDING','MARKETDAY','PEAKTYPE'],right_on=['HOURENDING','MARKETDAY','PEAKTYPE'])
logging.info("Sleeping to avoid throttling...")
time.sleep(SLEEP_TIME)

#%%
## Load which cleared Regulation DOWN
node_list_RD = generate_node_list("ERCOT_DAM_LOAD_REGDOWN_AWARDED", df1)
df2_RD = collect_data(node_list_RD, startdate, enddate)
df2_RD = post_process(df2_RD, 'DAM RD Awarded', '(ERCOT_DAM_LOAD_REGDOWN_AWARDED)')
print(df2_RD.info())

## Merge Spin and NonSpin and Regulation Down Quantities
df2_S_NS_RD = pd.merge(df2_S_NS,df2_RD,how='inner',left_on=['HOURENDING','MARKETDAY','PEAKTYPE'],right_on=['HOURENDING','MARKETDAY','PEAKTYPE'])

logging.info("Sleeping to avoid throttling...")
time.sleep(SLEEP_TIME)

#%%
## Load which cleared Regulation UP
node_list_RU = generate_node_list("ERCOT_DAM_LOAD_REGUP_AWARDED", df1)
df2_RU = collect_data(node_list_RU, startdate, enddate)
df2_RU = post_process(df2_RU, 'DAM REGUP Awarded', '(ERCOT_DAM_LOAD_REGUP_AWARDED)')

print(df2_RU.info())
logging.info("Sleeping to avoid throttling...")
time.sleep(SLEEP_TIME)


#%%
startdate = '06/10/2023'
node_list_ECRSSD = generate_node_list("ERCOT_DAM_LOAD_ECRSSD_AWARDED", df1)
df2_ECRSSD = collect_data(node_list_ECRSSD, startdate, enddate)
df2_ECRSSD = post_process(df2_ECRSSD, 'DAM ECRSSD Awarded', '(ERCOT_DAM_LOAD_ECRSSD_AWARDED)')

print(df2_ECRSSD.info())
logging.info("Sleeping to avoid throttling...")
time.sleep(SLEEP_TIME)

#%%
## Load which cleared ECRS Manually Dispatched Awards #####
node_list_ECRSMD = generate_node_list("ERCOT_DAM_LOAD_ECRSMD_AWARDED", df1)
df2_ECRSMD = collect_data(node_list_ECRSMD, startdate, enddate)
df2_ECRSMD = post_process(df2_ECRSMD, 'DAM ECRSMD Awarded', '(ERCOT_DAM_LOAD_ECRSMD_AWARDED)')

print(df2_ECRSMD.info())

df2_ECRS = pd.concat([df2_ECRSSD, df2_ECRSMD]).groupby(['HOURENDING','MARKETDAY','PEAKTYPE']).sum().reset_index()
df2_ECRS['DAM ECRS Awarded'] = df2_ECRS['DAM ECRSSD Awarded'] + df2_ECRS['DAM ECRSMD Awarded']
df2_ECRS = df2_ECRS.drop(labels=['DAM ECRSSD Awarded','DAM ECRSMD Awarded'],axis=1)


#%%
## Merge Spin and NonSpin and Regulation Down Quantities
df2_S_NS_RD_RU = pd.merge(df2_S_NS_RD,df2_RU,how='inner',left_on=['HOURENDING','MARKETDAY','PEAKTYPE'],right_on=['HOURENDING','MARKETDAY','PEAKTYPE'])

#%%
# Currently gives connection error? As of 12/18
cnxn = pyodbc.connect(driver = '{SQL Server Native Client 11.0}',server = 'pzpwbidb01',database = 'Accord',trusted_connection='yes')
## Get AS Quantities from ACCORD Database
query = ' Select * from [fact_ERCOTBI_Traders].[dbo].[vw_ERCOT_DAM_Ancillary_Service_Plan] '\
        ' PIVOT (AVG([Quantity]) FOR [Ancillary_Type] IN (REGUP,REGDN,RRS,NSPIN,ECRS)) AS PivotTable '
        
df_ASQ = pd.read_sql_query(query,cnxn)         
df_ASQ = df_ASQ.drop(columns=['DST','OriginalSourceFileName'])

df_ASQ['Hour_Ending'] = df_ASQ['Hour_Ending'].str.replace(":","").astype(int)/100

df_ASQ['Delivery_date'] = pd.to_datetime(df_ASQ['Delivery_date']).dt.strftime('%m/%d/%Y')

## Rename the table before merge
df_ASQ=df_ASQ.rename(columns = {'Delivery_date':'MARKETDAY'})
df_ASQ=df_ASQ.rename(columns = {'Hour_Ending':'HOURENDING'})
