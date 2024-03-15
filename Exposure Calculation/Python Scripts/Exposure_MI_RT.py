import requests
import pandas as pd
from io import StringIO
import zipfile
import logging
import numpy as np
import time
import os
from requests.exceptions import HTTPError
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global Variables and Parameters.
start_time = time.time()

CREDENTIALS_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"
OUTPUT_ROOT = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Exposure Calculation/Outputs"
YEAR = 2022
MONTH = 2
THRESHOLD = 0.1
BASE_URL = "https://services.yesenergy.com/PS/rest/ftr/awards/netinventory.csv"
mis_path = "//Pzpwuplancli01/Uplan/ERCOT"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
with open(CREDENTIALS_PATH, "r") as cred_file:
    AUTH_CREDENTIALS = tuple(cred_file.read().split(" "))

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

def grab_net_inventory(year: int, month: int) -> pd.DataFrame:
    """
    Grabs Raw NetInventory data for a certain year and month combination
    """
    query_date = datetime(year, month, 1)
    next_month_date = (query_date + timedelta(days=31)).strftime("%m/%d/%Y")
    
    params = {
            'iso': 'ERCOT',
            'startdate': query_date.strftime("%m/%d/%Y"),
            'enddate': next_month_date,
            'columns': '2,3,5,7,8,9,13,31,32'
        }
    
    return get_data_from_api(BASE_URL, params)
