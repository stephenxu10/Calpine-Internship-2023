from pathlib import Path
import pandas as pd
from datetime import date, timedelta, datetime
from MIS_Download_Scheduler import download_folder, destination_folder
from concurrent.futures import ThreadPoolExecutor
import os

# Constants
EXCEL_PATH = Path(r"\\Pzpwuplancli01\APP-DATA\Task Scheduler\MIS_Download_210125a_v3_via_API.xlsm")
DESTINATION_FOLDER = Path(destination_folder)

# Function to load data from Excel
def load_data_from_excel(sheet_name, columns):
    return pd.read_excel(EXCEL_PATH, sheet_name=sheet_name, usecols=columns)

def extract_date(path_name: str, year: int) -> str:
    year_str = str(year)
    try:
        year_idx = path_name.index(year_str)
        raw_date = path_name[year_idx: year_idx + 8]
        return pd.to_datetime(raw_date, format='%Y%m%d').strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Year {year} not in {path_name}")


def find_missing_chunks(folder_name: str, frequency=24):
    sub_folder = DESTINATION_FOLDER / folder_name
    lower_bound_date = pd.to_datetime(date.today() - timedelta(days=30))

    if not sub_folder.exists():
        raise FileNotFoundError(f"The system cannot find the path {sub_folder}")
    
    frequencies = {}
    for zip_folder in sub_folder.iterdir():
        folder_date = pd.to_datetime(extract_date(zip_folder.name, date.today().year))
        if folder_date >= lower_bound_date and folder_date < date.today():
            frequencies[folder_date] = frequencies.get(folder_date, 0) + 1
    
    print(frequencies)
    return [d.strftime("%Y-%m-%d") for d in frequencies if frequencies[d] < frequency * 3 // 4]

def patch_folder(folder_name: str, full_mapping: dict):
    missing_dates = find_missing_chunks(folder_name)

    # Function to execute in parallel
    def download_for_date(date):
        next_date = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        download_folder(full_mapping, folder_name, date, "06", next_date, "06", False)

    # Using ThreadPoolExecutor to parallelize downloads
    with ThreadPoolExecutor(max_workers=2) as executor:  
        executor.map(download_for_date, missing_dates)

# Main execution
excel_path = "\\\\Pzpwuplancli01\\APP-DATA\\Task Scheduler\\MIS_Download_210125a_v3_via_API.xlsm"

# Create the folder mapping by reading the Excel sheet.
webpage_partial = pd.read_excel(excel_path, sheet_name="List of Webpage", usecols=['Folder Name', 'Type Id', 'New Table Name'])
webpage_complete = pd.read_excel(excel_path, sheet_name="List of Webpage_complete", usecols=['Folder Name', 'Type of file'])

condition = webpage_partial['New Table Name'] != ""
dict_partial = dict(zip(webpage_partial.loc[condition, 'Folder Name'], webpage_partial.loc[condition, 'Type Id']))
dict_complete = dict(zip(webpage_complete['Folder Name'], webpage_complete['Type of file']))

full_mapping = {a: (dict_partial[a], dict_complete[a]) for a in dict_partial.keys() & dict_complete.keys()}
full_mapping.pop("48_3MRCR")

print(find_missing_chunks("130_SSPSF"))
patch_folder("130_SSPSF", full_mapping)
