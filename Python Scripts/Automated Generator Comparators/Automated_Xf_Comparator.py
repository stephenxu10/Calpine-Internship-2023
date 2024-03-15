
from io import BytesIO
import requests
import zipfile
import warnings
import pandas as pd
from datetime import date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import DAM_Ln_Xf_Comparator

# Ignore warnings. Whatever.
warnings.simplefilter("ignore")

# Build the request URL.
url_header = "https://ercotapi.app.calpine.com/reports?reportId=13070&marketParticipantId=CRRAH&"

lower = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
upper = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')

# Should return the MIS DAM Data for today and tomorrow.
request_url = url_header + f"startTime={lower}T00:00:00&endTime={upper}T00:00:00&unzipFiles=false"
print(request_url)

# Get today and tomorrow's dates in MM/DD/YYYY
today = date.today().strftime('%m/%d/%Y')
tomorrow = (date.today() + timedelta(days=1)).strftime('%m/%d/%Y')

# Work on pulling tomorrow's data via the website.
r = requests.get(request_url, verify=False)
content = r.content

# zip_data is a nested zip file - it should contain a list of historical MIS ZIP files in [lower, upper]
zip_data = BytesIO(content)

with zipfile.ZipFile(zip_data, 'r') as zip_file:
    # We intend for zip_file to hold exactly two ZIPs - one for today and one for tomorrow.
    today_zip = zip_file.namelist()[0]
    tomo_zip = zip_file.namelist()[1]

    today_data = BytesIO(zip_file.read(today_zip))
    tomo_data = BytesIO(zip_file.read(tomo_zip))

    # Locate the generator CSV for Hour 16 today
    with zipfile.ZipFile(today_data, 'r') as first_zip_file:
        csv_now = [x for x in first_zip_file.namelist() if "_Xf_016" in x][0]
        
        with first_zip_file.open(csv_now) as csv_today:
            df_today = pd.read_csv(csv_today)

    # Locate the generator CSV for Hour 16 tomorrow.
    with zipfile.ZipFile(tomo_data, 'r') as second_zip_file:
        csv_tomorrow = [x for x in second_zip_file.namelist() if "_Xf_016" in x][0]

        with second_zip_file.open(csv_tomorrow) as csv_tomo:
            df_tomo = pd.read_csv(csv_tomo)

output_path = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/Line and Transformer Comparisons/Xf"
df_status_comp = DAM_Ln_Xf_Comparator.compare_statuses(df_today, df_tomo, today, tomorrow, False)
df_status_comp.to_csv(output_path + "./status.csv", index=False)
df_rate_a = DAM_Ln_Xf_Comparator.compare_rates(df_today, df_tomo, today, tomorrow, 'RATEA')
df_rate_a.to_csv(output_path + "./RATEA.csv", index=False)
df_rate_b = DAM_Ln_Xf_Comparator.compare_rates(df_today, df_tomo, today, tomorrow, 'RATEB')
df_rate_b.to_csv(output_path + "./RATEB.csv", index=False)

