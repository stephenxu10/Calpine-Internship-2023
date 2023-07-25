import requests
import warnings
import concurrent.futures
import time
import sys
import zipfile
import os
from typing import Dict, Tuple
import pandas as pd
from io import BytesIO
from datetime import date, datetime, timedelta

"""
This script aims to automate the MIS downloading process via the ERCOT API.

Currently, the process is sped up through futures and thread-lock synchronization. Running
the script will extract all new files for the current day into the targeted directory
in about nine minutes on a 4-core CPU.
"""
# Ignore warnings. Whatever.
warnings.simplefilter("ignore")

# Global Parameters and Variables
start_time = time.time()

# Maximum number of concurrent operations.
max_workers = 8

# How many days we look back for data collection.
days_back = 1

# How many hours to look back for OOM Exception Handling.
chunk_size = 6

# Yesterday and today's date
offset = 0
yesterday = (date.today() - timedelta(days=days_back+offset)).strftime('%Y-%m-%d')
today = (date.today() - timedelta(days=offset)).strftime('%Y-%m-%d')

# Current temporary storage for downloaded files
destination_folder = "\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS 2023\\"

# destination_folder = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\Data\\MIS Scheduled Downloads\\"

# Text file for invalid request numbers
invalid_rid = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/MIS Scheduled Downloader/request_summary.txt"

# Reference Excel Sheet for all the web data and requirements
excel_path = "\\\\Pzpwuplancli01\\APP-DATA\\Task Scheduler\\MIS_Download_210125a_v3_via_API.xlsm"

# Assume these folders will throw OOM error when querying back a day. More optimal to give up immediately.
if days_back == 2:
    give_up = ["34_TC", "83_CTOR", "20_RTDRD", "58_RILBRNLZAH", "16_LBEB"]

else:
    give_up = ["34_TC", "83_CTOR"]

give_up.append("03_ASC")
give_up.append("02_AAFRWP")
give_up.append("130_SSPSF")

invalid_rid = open(invalid_rid, "w")

def convert(hour: int) -> str:
    """
    Simple helper method to convert an input hour.
        - 9 -> "09"
        - 13 -> "13"
    """
    if hour < 10:
        return "0" + str(hour)
    else:
        return str(hour)

def handle_oom_error(mapping: Dict, folder: str, request: str, start_date: str, start_hour: int, hours_left: int, back: int):
    """
    This method handles a caught 500 HTTP response. In practice, this is almost guaranteed to be
    a 'System.OutOfMemory' Exception from the ERCOT Calpine API. To address this, we split up 
    the original queried time into smaller chunks, typically 6 or 8 hours at a time. The
    received contents are then downloaded into the desired folder.

    This method is purely sequential. Not really worth the hassle to parallelize.

    Inputs:
        - folder: The name of the folder to download to, i.e. '83_CTOR'
        - request: The 5-digit ID of the request for that folder
        - start_date: The starting date in YYYY-MM-DD format.
        - start_hour: The starting hour in [0, 24]
        - hours_left: How many hours left to query.
        - back: How many hours we look back at a time. 6-8 hours is reasonable.
    
    Output:
        Returns nothing, but downloads to the appropriate folder.
    """
    if hours_left == 0:
        return
    
    # Calculate the new upper and lower bounds
    upper_date = start_date
    upper_hour = start_hour
    lower_date = start_date
    lower_hour = int(start_hour) - back

    if int(lower_hour) < 0:
        lower_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        lower_hour += 24

    lower_hour = convert(lower_hour)
    
    # Download the contents for this new bound.
    download_folder(mapping, folder, lower_date, lower_hour, upper_date, upper_hour, handle=False)
    handle_oom_error(mapping, folder, request, lower_date, lower_hour, hours_left - back, back)


def download_folder(mapping: Dict[str, Tuple[str, str]], folder_name: str, l_d: str, l_h: str, u_d: str, u_h: str, handle=True):
    """
    Given a mapping of folder names to (reportID, Type) tuples, a requested folder name,
    and an input date in YYYY-MM-DD or today - x format, this helper method
    queries ERCOT API and extracts the contents in the received ZIP file for the input
    date into the destination folder.

    Inputs:
        - mapping: A dictionary mapping folder names to an ordered pair of corresponding
          report IDs and Types
        - folder_name: The requested folder name to extract.
        - lower_date, lower_hour: The lower-bound date in YYYY-MM-DD format and hour in '05' or '23' format.
        - upper_date, upper_hour: Analogous to above.
        - handle: Boolean flag that determines if invalid requests should be handled. True by default.

    Output:
        - Returns nothing, but downloads files to appropriate folder. 
    """
    sub_folder = f"{destination_folder}{folder_name}"
    reportID, file_type = mapping[folder_name]

    # Give up on some folders and immediately handle an OOM error.
    if folder_name in give_up and handle:
        print(f"Handling assumed Exception for folder {folder_name}...")
        invalid_rid.write(f"{reportID} {folder_name} 500\n")
        handle_oom_error(mapping, folder_name, reportID, u_d, u_h, 24 * days_back, chunk_size)
    
    else:
        ercot_url = f"https://ercotapi.app.calpine.com/reports?reportId={reportID}&marketParticipantId=CRRAH&startTime={l_d}T{l_h}:00:00&endTime={u_d}T{u_h}:00:00&unzipFiles=false"
        response = requests.get(ercot_url, verify=False)

        # If the request was successful, extract the received files
        if response.status_code == 200:
            # This statement will execute if an OOM Handler successfully worked on a folder.
            if u_h != l_h:
                invalid_rid.write(f"Folder {folder_name} written to successfully from {l_d} Hour {l_h} to {u_d} Hour {u_h}.\n")
            
            else:
                invalid_rid.write(f"{reportID} {folder_name} {response.status_code}\n")

            zip_file = zipfile.ZipFile(BytesIO(response.content))
            filtered_files = [filename for filename in zip_file.namelist() if file_type == 'all' or file_type in filename]

            for filename in filtered_files:
                if filename != "errorLog.txt":
                    zip_file.extract(filename, sub_folder)

        # Handle the Internal Server Error Exception here. Most likely an OutOfMemory issue.
        elif response.status_code == 500:
            invalid_rid.write(f"{reportID} {folder_name} {response.status_code}\n")

            if handle:
                print(f"Handling Exception for folder {folder_name}...")
                handle_oom_error(mapping, folder_name, reportID, u_d, u_h, 24 * days_back, chunk_size)

        # Most likely a 404 error code - no data is available for today. 
        else:
            invalid_rid.write(f"{reportID} {folder_name} {response.status_code}\n")
        
    return


# Create the folder mapping by reading the Excel sheet.
webpage_partial = pd.read_excel(excel_path, sheet_name="List of Webpage", usecols=['Folder Name', 'Type Id', 'New Table Name'])
webpage_complete = pd.read_excel(excel_path, sheet_name="List of Webpage_complete", usecols=['Folder Name', 'Type of file'])

condition = webpage_partial['New Table Name'] != ""
dict_partial = dict(zip(webpage_partial.loc[condition, 'Folder Name'], webpage_partial.loc[condition, 'Type Id']))
dict_complete = dict(zip(webpage_complete['Folder Name'], webpage_complete['Type of file']))

full_mapping = {a: (dict_partial[a], dict_complete[a]) for a in dict_partial.keys() & dict_complete.keys()}
full_mapping.pop("48_3MRCR")

# List of folders to process
folders = full_mapping.keys()

# First, create the subfolders if necessary.
for folder in folders:
    sub_folder = f"{destination_folder}{folder}"

    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)


# Parse the command-line arguments to determine what actions should be taken.
arguments = sys.argv
valid_flag = True

if "-r" in arguments:
    if arguments[-1] != "-r":
        # Set the chunk size based on if it is given or not.
        if "-c" in arguments:
            c_idx = arguments.index("-c")
            c_size = int(arguments[c_idx + 1])

        else:
            c_size = chunk_size

        # Parse all the requested folders.
        folders_to_download = []
        index = arguments.index("-r")

        while index < len(arguments) - 1:
            if arguments[index + 1] == "-c":
                break
            
            folders_to_download.append(arguments[index+1])
            index += 1

        # Submit the folders for processing.
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(handle_oom_error, full_mapping, folder, full_mapping[folder][0], today, "06", 24, c_size) for folder in folders_to_download]

            # Wait for all futures to complete
            concurrent.futures.wait(futures)

    else:
        sys.stderr.write(f"usage: {sys.argv[0]} [-c <integer>] [-r] <input folder-names>\n")
        valid_flag = False

else:
    # Create a ThreadPoolExecutor with the specified number of workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit each folder for processing
        futures = [executor.submit(download_folder, full_mapping, folder, yesterday, "06", today, "06") for folder in folders]

        # Wait for all futures to complete
        concurrent.futures.wait(futures)


# Output Summary Statistics
if valid_flag:
    end_time = time.time()
    execution_time = (end_time - start_time)
    print("Downloading Complete")
    invalid_rid.write(f"The script took {execution_time:.2f} seconds to run.")

invalid_rid.close()
