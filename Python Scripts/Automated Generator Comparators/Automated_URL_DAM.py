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

Currently, there are two versions of Python scripts that accomplish this task. In this newer version, some of
the data is extracted from the ERCOT API, which is advantageous since we no longer require all data
to already exist on the disk. 

More testing still needed!
"""

# Ignore warnings. Whatever.
warnings.simplefilter("ignore")

# Build the request URL.
url_header = "https://ercotapi.app.calpine.com/reports?reportId=13070&marketParticipantId=CRRAH&"

lower = (date.today() - timedelta(days=2)).strftime('%m-%d-%Y')
upper = (date.today() + timedelta(days=2)).strftime('%m-%d-%Y')

request_url = url_header + f"startTime={lower}T00:00:00&endTime={upper}T00:00:00&unzipFiles=false"

# Get today and tomorrow's dates in MM/DD/YYYY
today = date.today().strftime('%m/%d/%Y')
tomorrow = (date.today() + timedelta(days=1)).strftime('%m/%d/%Y')

# Assume that today's DAM data is already on the disk.
df_today = DAM_Gn_Comparator.find_generator_data(today, 16)

# Work on pulling tomorrow's data via the website.
r = requests.get(request_url, verify=False)
content = r.content

# zip_data is a nested zip file - it should contain a list of historical MIS ZIP files in [lower, upper]
zip_data = BytesIO(content)
with zipfile.ZipFile(zip_data, 'r') as zip_file:

    # Hopefully, first_zip is the zip file for tomorrow's generator data. More testing is needed to confirm this.
    first_zip = zip_file.namelist()[0]
    first_data = BytesIO(zip_file.read(first_zip))

    # Locate the generator CSV for Hour 16.
    with zipfile.ZipFile(first_data, 'r') as first_zip_file:
        csv_tomorrow = [x for x in first_zip_file.namelist() if "_Gn_016" in x][0]
        
        with first_zip_file.open(csv_tomorrow) as csv_tomo:
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
receivers = ['Stephen.Xu@calpine.com', 'Pranil.Walke@calpine.com']

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")
smtpObj.sendmail(sender, receivers, msg.as_string())

smtpObj.quit()
