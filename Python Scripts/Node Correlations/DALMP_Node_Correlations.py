#%%
import requests
import pandas as pd
from io import StringIO
import concurrent.futures
import time
import logging
from requests.exceptions import HTTPError
import warnings
import numpy as np

warnings.simplefilter("ignore")

AUTH_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"
OUTPUT_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Node Correlation Comparisons"
BASE_URL = "https://services.yesenergy.com/PS/rest/timeseries/"
STARTDATE = "2023-06-01"
ENDDATE = "2023-09-01"
ITEMSIZE = 75

start_time = time.time()

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
    url_string = [f"{key}:{i}" for i in df.POINTID]
    return [','.join(url_string[i:i + ITEMSIZE]) for i in range(0, len(url_string), ITEMSIZE)]

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
        df.columns = df.columns.str.strip()
        df = df.drop(labels=['MARKETDAY','HOURENDING','PEAKTYPE', 'DATETIME', 'MONTH', 'YEAR'],axis=1)
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
            'startdate': STARTDATE,
            'enddate': ENDDATE,
            'PEAKTYPE': 'WDPEAK',
            'items': nodes
        }
        for nodes in node_list
    ]

    # IMPORTANT: Yes Energy enforces a limit of seven concurrent requests per user. max_workers should
    # never be greater than 7.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_df = {executor.submit(fetch_data, params): params for params in params_list}
        for future in concurrent.futures.as_completed(future_to_df):
            df = future.result()
            if df is not None:
                dataframes.append(df)
            else:
                params = future_to_df[future]
                logging.error(f"Failed to fetch data for params: {params}")

    return pd.concat(dataframes, axis=1)


with open(AUTH_PATH, "r") as auth:
    AUTH_CREDENTIALS = tuple(auth.read().split(" "))


# Read in the initial OBJECTIDs into a DataFrame
call1 = "https://services.yesenergy.com/PS/rest/collection/node/ERCOT Valid Prompt Month CRR Nodes.csv"
print(call1)
call_one = requests.get(call1, auth=AUTH_CREDENTIALS)
df1 = pd.read_csv(StringIO(call_one.text))

# Read in the desired SOUTH nodes
call2 = "https://services.yesenergy.com/PS/rest/collection/node/ERCOT_Calpine.csv"
print(call2)
call_two = requests.get(call2, auth=AUTH_CREDENTIALS)
df2 = pd.read_csv(StringIO(call_two.text))

# Filter on ZONE = SOUTH
south_nodes = list(df2[df2['ZONE'] == 'SOUTH']['PNODENAME'])

node_list = generate_node_list("DALMP", df1)
raw_dataset = collect_data(node_list)
raw_df = raw_dataset.dropna(axis=1, how='all')

raw_df.columns = raw_df.columns.str.replace(r'\(DALMP\)$', '', regex=True)

#%%
def find_correlation(raw_df: pd.DataFrame, node1: str, node2: str) -> float:
    """
    Method that finds the correlation between two columns in a DataFrame. Assumes
    the columns contain the same length of data.

    Parameters
    ----------
    raw_df : pd.DataFrame
        a DataFrame of raw data
    node1 : str
        The name of the first node
    node2 : str
        The name of the second node

    Returns
    -------
    The correlation coefficient, ranging from -1 to 1 between node1 and node2.
    """
    x = np.array(raw_df[node1])
    y = np.array(raw_df[node2])
    
    if len(x) != len(y):
        print("ERROR: lengths do not match")
        return np.nan
    
    correlation_matrix = np.corrcoef(x, y)
    return correlation_matrix[0, 1]

def find_top_50(raw_df: pd.DataFrame, south_node: str):
    correlations = []
    for col in raw_df.columns: 
        if col != south_node:
            correlation = find_correlation(raw_df, south_node, col)
            if correlation == correlation and correlation < 0.98:  # Check for NaN and filter out self-correlation
                correlations.append((south_node, col, correlation))  
    
    correlations_df = pd.DataFrame(correlations, columns=['South_Node', 'Connected_Node', 'Correlation'])
    correlations_df.sort_values(by='Correlation', ascending=False, inplace=True)
    return correlations_df.head(50)

raw_df.columns = raw_df.columns.str.strip()
all_top_50_df = pd.DataFrame()

# Assume south_nodes is a list of node names
for node in south_nodes:
    top_50_df = find_top_50(raw_df, node)
    all_top_50_df = pd.concat([all_top_50_df, top_50_df], ignore_index=True)

# Display or process all_top_50_df as needed
all_top_50_df['Rank'] = all_top_50_df.groupby('South_Node')['Correlation'].rank(method='first', ascending=False)
raw_df.to_csv(OUTPUT_PATH + "/raw_dataset.csv", index=False)
all_top_50_df.to_csv(OUTPUT_PATH + "/south_top_50.csv", index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")
