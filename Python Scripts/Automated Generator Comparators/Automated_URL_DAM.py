# type: ignore

from io import BytesIO
import requests
import zipfile
import warnings
import pandas as pd
from datetime import date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import DAM_Gn_Comparator

"""
This Python tool aims to automate the comparison between today and yesterday's DAM Generator data 
by sending an email through Outlook storing the CSV that summarizes the comparison. Will send every day
at 7 AM.

Here, we extract the DAM data via the ERCOT API. More testing is needed to confirm correctness. If an email
does not send at 7 AM, here are some possible explanations.
    - The data has been published yet on ERCOT. 
    Check https://mis.ercot.com/secure/data-products/markets/day-ahead-market?id=NP4-500-SG to see if it is
    there or not.
    
    - If the data is there on the ERCOT website, the script either failed to grab the data, or
      a runtime error occurred during the processing.
"""
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
current_month = datetime.datetime.now().month

if 4 <= current_month <= 10:  # April to October
    gn_key = "_Gn_016"
else:
    gn_key = "_Gn_008"

with zipfile.ZipFile(zip_data, 'r') as zip_file:
    # We intend for zip_file to hold exactly two ZIPs - one for today and one for tomorrow.
    today_zip = zip_file.namelist()[0]
    tomo_zip = zip_file.namelist()[1]

    today_data = BytesIO(zip_file.read(today_zip))
    tomo_data = BytesIO(zip_file.read(tomo_zip))

    # Locate the generator CSV for Hour 16 today
    with zipfile.ZipFile(today_data, 'r') as first_zip_file:
        csv_now = [x for x in first_zip_file.namelist() if gn_key in x][0]
        
        with first_zip_file.open(csv_now) as csv_today:
            df_today = pd.read_csv(csv_today)

    # Locate the generator CSV for Hour 16 tomorrow.
    with zipfile.ZipFile(tomo_data, 'r') as second_zip_file:
        csv_tomorrow = [x for x in second_zip_file.namelist() if gn_key in x][0]

        with second_zip_file.open(csv_tomorrow) as csv_tomo:
            df_tomo = pd.read_csv(csv_tomo)
            
# Use previous helper methods to compare the generator data between the two DataFrames
df_csv = DAM_Gn_Comparator.compare_statuses(df_today, df_tomo, today, tomorrow)
change = df_csv[df_csv['Description'] == 'Changed']

unknown, known = DAM_Gn_Comparator.merge_with_mapping(df_csv)

body = ""
if len(change) == 0:
    body += '<html><p> No generator statuses have changed from yesterday to today. <br> </p></html>'

body += '<html><body>' + known.to_html(index=False) + '</body></html>'

msg = MIMEMultipart('alternative')
msg['Subject'] = "Daily DAM Generator Comparison Results"
sender = 'transmission.yesapi@calpine.com'

"""
Update the unknown mapping text file.
"""
output_root = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/MIS Gen_Ln_Xf Comparisons/Unknown Mappings"
if len(unknown) == 0:
    body += '<html><p> No unknown mappings were found. <br> </p></html>'

else:
    body += '<html><p> Unknown mappings were found. Go to <strong> //pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Data/MIS Gen_Ln_Xf Comparisons/Unknown Mappings </strong> for the latest unmatched records. <br> </p></html>'
    df_curr = pd.read_csv(output_root + "/output.csv")
    combined = pd.concat([unknown, df_curr]).drop_duplicates()
    combined.to_csv(output_root + "/output.csv", index=False)

# Edit this line to determine who receives the email.
receivers = ['Stephen.Xu@calpine.com']

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")

smtpObj.sendmail(sender, receivers, msg.as_string())
smtpObj.quit()
