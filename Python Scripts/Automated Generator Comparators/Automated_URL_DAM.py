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

with zipfile.ZipFile(zip_data, 'r') as zip_file:
    # We intend for zip_file to hold exactly two ZIPs - one for today and one for tomorrow.
    today_zip = zip_file.namelist()[0]
    tomo_zip = zip_file.namelist()[1]

    today_data = BytesIO(zip_file.read(today_zip))
    tomo_data = BytesIO(zip_file.read(tomo_zip))

    # Locate the generator CSV for Hour 16 today
    with zipfile.ZipFile(today_data, 'r') as first_zip_file:
        csv_now = [x for x in first_zip_file.namelist() if "_Gn_016" in x][0]
        
        with first_zip_file.open(csv_now) as csv_today:
            df_today = pd.read_csv(csv_today)

    # Locate the generator CSV for Hour 16 tomorrow.
    with zipfile.ZipFile(tomo_data, 'r') as second_zip_file:
        csv_tomorrow = [x for x in second_zip_file.namelist() if "_Gn_016" in x][0]

        with second_zip_file.open(csv_tomorrow) as csv_tomo:
            df_tomo = pd.read_csv(csv_tomo)

# Use previous helper methods to compare the generator data between the two DataFrames
shared, first, second, change = DAM_Gn_Comparator.compare_data(df_today, df_tomo)
df_csv = DAM_Gn_Comparator.write_results(shared, first, second, change, today, tomorrow)

body = ""
if len(change) == 0:
    body += '<html><p> No generator statuses have changed from yesterday to today. <br> </p></html>'

body += '<html><body>' + df_csv.to_html(index=False) + '</body></html>'

msg = MIMEMultipart('alternative')
msg['Subject'] = "Daily DAM Generator Comparison Results"
sender = 'Transmission.Yesapi@calpine.com'

# Edit this line to determine who receives the email.
receivers = ['Stephen.Xu@calpine.com', 'Pranil.Walke@calpine.com']

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")
smtpObj.sendmail(sender, receivers, msg.as_string())

smtpObj.quit()
