import pandas as pd 
import requests
from io import StringIO


AUTH_PATH = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/Amperon Data Analysis/auth_amperon_sx.txt"
CLIENT_ID = ""
CLIENT_PW = ""
DAYS_BACK = 3
DAYS_FORWARD = 14
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/03 - UPLAN/02 - Setup/Temporalcsv/ERCOT_14day_WZ_UPLAN_Amepron.csv"

WZ_NAMES = ["COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST"]
DEDUCTIONS = {
    "WZ_COAST": 3899,
    "WZ_EAST": 201,
    "WZ_FARWEST": 761,
    "WZ_NORTH": 2,
    "WZ_NTHCEN": 182,
    "WZ_STHCEN": 198,
    "WZ_SOUTH": 557,
    "WZ_WEST": 9,
    "Z_ERCOT": 5508
}
rename_mapping = {"COAST": "WZ_COAST", "EAST": "WZ_EAST", "FWEST": "WZ_FARWEST", "NCENT": "WZ_NTHCEN", "NORTH": "WZ_NORTH", "SCENT": "WZ_STHCEN", "SOUTH": "WZ_SOUTH", "WEST": "WZ_WEST"}

with open(AUTH_PATH, "r") as auth_file:
    auth = auth_file.read()
    CLIENT_ID = auth.split("\n")[0]
    CLIENT_PW = auth.split("\n")[1]

MY_AUTH = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_PW)

def post_process(load_df: pd.DataFrame) -> pd.DataFrame:
    """
    Post-processes a raw DataFrame queried directly from Amperon. Returns the new DataFrame 
    after the computations.
    """
    res = pd.DataFrame()
    load_df["Date"] = pd.to_datetime(load_df['date'], format="%Y-%m-%d")
    res['YEAR'] = load_df["Date"].dt.year
    res["MONTH"] = load_df["Date"].dt.month
    res["DAY"] = load_df["Date"].dt.day
    res['HOUR'] = load_df['hour']

    load_df = load_df.dropna(axis=1, how="all").fillna(0)
    
    for weather_zone in WZ_NAMES:
        res[weather_zone] = load_df[weather_zone + " Current Forecast"] + load_df[weather_zone + " Estimated Actual"] + load_df[weather_zone + " Actual"]
        res[weather_zone] = res[weather_zone] * 1000 - DEDUCTIONS[rename_mapping[weather_zone]]
    
    res = res.rename(columns=rename_mapping)
    return res

# Base URL
url = "https://platform.amperon.co/export/iso/ercot/short-term/load"


params = {
    'scenarioValues': '0,0,0,0,0',
    'scenarioAdjustmentValue': 'high',
    'mode': 'demand',
    'startDate': f'today-{DAYS_BACK}',
    'endDate': f'today+{DAYS_FORWARD}',
    'analysisBreakdown': 'weather_zone',
    'showTemps': 'false',
    'showActuals': 'true',
    'showHourAhead': 'true',
    'weatherModel': 'ag2',
}

try:
    response = requests.get(url, params=params, auth=MY_AUTH)

    # Check if the request was successful
    if response.status_code == 200 and response.text.strip():
        df_csv = pd.read_csv(StringIO(response.text))
        print("DataFrame loaded successfully.")
    else:
        print(f"Request failed or returned no data. Status code: {response.status_code}")

except Exception as e:
    # Handle any unexpected errors
    print(f"An error occurred: {e}")

# Post-process the raw DataFrame
df_csv = post_process(df_csv)

try:
    assert(len(df_csv) > (DAYS_FORWARD - DAYS_BACK + 1) * 24), "Data length does not meet the expected criteria"
except AssertionError as error:
    print(f"Assertion Error: {error}")
    raise

df_csv.to_csv(output_path, index=False)