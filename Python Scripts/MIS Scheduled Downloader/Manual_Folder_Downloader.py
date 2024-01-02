from MIS_Download_Scheduler import download_folder
import pandas as pd
import os

# Reference Excel Sheet for all the web data and requirements
excel_path = "\\\\Pzpwuplancli01\\APP-DATA\\Task Scheduler\\MIS_Download_210125a_v3_via_API.xlsm"
# d_folder = "\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\101 - Misc\\CRR Limit Aggregates\\Data\\MIS Scheduled Downloads\\"

# Create the folder mapping by reading the Excel sheet.
webpage_partial = pd.read_excel(excel_path, sheet_name="List of Webpage", usecols=['Folder Name', 'Type Id', 'New Table Name'])
webpage_complete = pd.read_excel(excel_path, sheet_name="List of Webpage_complete", usecols=['Folder Name', 'Type of file'])

condition = webpage_partial['New Table Name'] != ""
dict_partial = dict(zip(webpage_partial.loc[condition, 'Folder Name'], webpage_partial.loc[condition, 'Type Id']))
dict_complete = dict(zip(webpage_complete['Folder Name'], webpage_complete['Type of file']))

full_mapping = {a: (dict_partial[a], dict_complete[a]) for a in dict_partial.keys() & dict_complete.keys()}
full_mapping.pop("48_3MRCR")

folders = full_mapping.keys()

#%%
download_folder(full_mapping, "130_SSPSF", "2023-11-23", "06", "2023-11-24", "06", False)
