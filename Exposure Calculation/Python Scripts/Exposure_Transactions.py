import requests
import pandas as pd
from io import StringIO
import logging
from requests.exceptions import HTTPError

CONTRACT_SIZE_FILTER = 10
CREDENTIALS_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/credentials.txt"
OUTPUT_ROOT = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Exposure Calculation/Outputs"
YEAR = 2024
MONTH = 2
BASE_URL = "https://services.yesenergy.com/PS/rest/ftr/awards/transactions.csv"


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


def grab_raw_data(year: int, month: int) -> pd.DataFrame:
    """
    Grabs raw transaction data from Yes Energy for a given year and month. Returns
    a DataFrame containing this queried data.
    """
    query_date = f"{month:02d}/01/{year}"
    params = {
            'iso': 'ERCOT',
            'AUCTIONTYPE': 'M',
            'startdate': query_date,
            'enddate': query_date,
            'TRADETYPE': "BUY",
            'columns': '1,3,4,6,8,9,10,11,12,13,14,15,18,27,29,46'
        }
    return get_data_from_api(BASE_URL, params)

def post_process(df: pd.DataFrame) -> pd.DataFrame:
    relevant_columns = [
        'PARTICIPANTSHORTNAME', 'FTRPARTICIPANT', 'TRADETYPE', 'PEAKTYPE',
        'SOURCENAME', 'SINKNAME', 'CONTRACTSIZE', 'COST_PMWH', 'REVENUE_PMWH'
    ]
    df = df[relevant_columns]
    
    # Group by the relevant columns and aggregate the data
    df = df.groupby(
        by=['PARTICIPANTSHORTNAME', 'FTRPARTICIPANT', 'TRADETYPE', 'PEAKTYPE', 'SOURCENAME', 'SINKNAME']
    ).agg(
        CONTRACTSIZE_SUM=('CONTRACTSIZE', 'sum'),
        COST_PMWH_AVG=('COST_PMWH', 'mean'),
        REVENUE_PMWH_AVG=('REVENUE_PMWH', 'mean')
    ).reset_index()

    df.columns = [
        'PARTICIPANTSHORTNAME', 'FTRPARTICIPANT', 'TRADETYPE', 'PEAKTYPE',
        'SOURCENAME', 'SINKNAME', 'CONTRACTSIZE=SUM', 'COST_PMWH=avg', 'REVENUE_PMWH=avg'
    ]
    
    df = df[df['CONTRACTSIZE=SUM'] > CONTRACT_SIZE_FILTER]
    return df

test_df = post_process(grab_raw_data(YEAR, MONTH))
test_df.to_csv(OUTPUT_ROOT + f"/CRR Exposure_Transactions_{MONTH:02d}_{YEAR}.csv", index=False)

