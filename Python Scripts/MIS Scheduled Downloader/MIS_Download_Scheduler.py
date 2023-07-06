import requests
import warnings
import threading
import concurrent.futures
import time
import zipfile
import os
from typing import Dict, Tuple
import pandas as pd
from io import BytesIO

"""
This script aims to automate the MIS downloading process from the ERCOT API.

Currently, the process is sped up through futures and thread-lock synchronization. Running
the script will extract all new files for the current day into the targeted directory
in less than two minutes (almost 7 times speedup compared to a sequential implementation!).
"""

# Ignore warnings. Whatever.
warnings.simplefilter("ignore")

# Global Parameters and Variables
start_time = time.time()

# Maximum number of concurrent operations.
max_workers = 10

file_lock = threading.Lock()

# End goal for all downloaded files to be. Redirect here once testing complete.
# destination_folder = "//Pzpwuplancli01/Uplan/ERCOT/MIS 2023"

# Current temporary storage for downloaded files
destination_folder = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/MIS Scheduled Downloads/"

# Reference Excel Sheet for all the web data and requirements
excel_path = "\\\\Pzpwuplancli01\\APP-DATA\\Task Scheduler\\MIS_Download_210125a_v3_via_API.xlsm"

def download_folder(mapping: Dict[str, Tuple[str, str]], folder_name: str, date: str):
    """
    Given a mapping of folder names to (reportID, Type) tuples, a requested folder name,
    and an input date in YYYY-MM-DD or today - x format, this helper method
    queries ERCOT API and extracts the contents in the received ZIP file for the input
    date into the destination folder.

    Inputs:
        - mapping: A dictionary mapping folder names to an ordered pair of corresponding
          report IDs and Types
        - folder_name: The requested folder name to extract.
        - date: The date to query for in the ERCOT API.
    
    Output:
        - Returns nothing.
    """
    sub_folder = f"{destination_folder}{folder_name}"
    current_file_names = os.listdir(sub_folder)
    reportID, file_type = mapping[folder_name]
    ercot_url = f"https://ercotapi.app.calpine.com/reports?reportId={reportID}&marketParticipantId=CRRAH&startTime={date}T00:00:00&endTime={date}T00:00:00&unzipFiles=false"
    print(ercot_url)

    response = requests.get(ercot_url, verify=False)

    if response.status_code == 200:
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        
        filtered_files = [filename for filename in zip_file.namelist() if file_type == 'all' or file_type in filename]

        with file_lock:
            for filename in filtered_files:
                if filename not in current_file_names:
                    zip_file.extract(filename, sub_folder)
                    print(filename + " " + folder_name)
        
    else:
        print("Invalid query to ERCOT. Check the validity of the request URL.")
    
    print("")

    return

# Create the folder mapping by reading the Excel sheet.
webpage_partial = pd.read_excel(excel_path, sheet_name="List of Webpage", usecols=['Folder Name', 'Type Id', 'New Table Name'])
webpage_complete = pd.read_excel(excel_path, sheet_name="List of Webpage_complete", usecols=['Folder Name', 'Type of file'])

condition = webpage_partial['New Table Name'] != ""
dict_partial = dict(zip(webpage_partial.loc[condition, 'Folder Name'], webpage_partial.loc[condition, 'Type Id']))
dict_complete = dict(zip(webpage_complete['Folder Name'], webpage_complete['Type of file']))

full_mapping = {a: (dict_partial[a], dict_complete[a]) for a in dict_partial.keys() & dict_complete.keys()}

# List of folders to process
folders = full_mapping.keys()

# First, create the subfolders if necessary.
for folder in folders:
    sub_folder = f"{destination_folder}{folder}"

    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)

# Create a ThreadPoolExecutor with the specified number of workers
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit each folder for processing
    futures = [executor.submit(download_folder, full_mapping, folder, "today") for folder in folders]

    # Wait for all futures to complete
    concurrent.futures.wait(futures)

# Output Summary Statistics
end_time = time.time()
execution_time = (end_time - start_time)
print("Downloading Complete")
print(f"The script took {execution_time:.2f} seconds to run.")