from io import StringIO
import zipfile
import json
import requests
import warnings
from typing import Dict, Tuple, List, Union
import pandas as pd
from collections import defaultdict
import time
from datetime import timedelta, datetime
import os

warnings.simplefilter("ignore")

# Global Variables and Parameters.
start_time = time.time()
year = 2023

# How many days we look back
days_back = 30

zip_base = f"\\\\Pzpwuplancli01\\Uplan\\ERCOT\\MIS {year}\\130_SSPSF"
output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Aggregated RT Constraint Data/RT_Summary_" + str(year) + ".csv"

yearly_zip_files = os.listdir(zip_base)

today = datetime.now().date
for zip_file in yearly_zip_files:
    zip_date = datetime.strptime(zip_file[34:36] + "/" + zip_file[36:38] + "/" + str(year), "%m/%d/%Y")

    # Convert all newly added CSVs since the last aggregation
    if zip_date >= today - timedelta(days=days_back):
        with zipfile.ZipFile(os.path.join(zip_base, zip_file), "r") as zip_path:
            print(zip_file)
