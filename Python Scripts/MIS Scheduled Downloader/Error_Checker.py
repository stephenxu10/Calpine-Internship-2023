from datetime import date, datetime, timedelta
import os
import pandas as pd
import requests
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import warnings

# Ignore warnings
warnings.simplefilter("ignore")

# Absolute paths to Python executable and the downloader script path.
pythonPath = "C:/ProgramData/Anaconda3/python.exe"
scriptPath = "\"//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/MIS Scheduled Downloader/MIS_Download_Scheduler.py\""

missing_folders = []
days_back = 2
chunk_size = 6

log_file = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/MIS Scheduled Downloader/request_summary.txt"
offset = 0
yesterday = (date.today() - timedelta(days=days_back+offset)).strftime('%Y-%m-%d')
today = (date.today() - timedelta(days=offset)).strftime('%Y-%m-%d')

# Reference Excel Sheet for all the web data and requirements
excel_path = "\\\\Pzpwuplancli01\\APP-DATA\\Task Scheduler\\MIS_Download_210125a_v3_via_API.xlsm"
df_excel = pd.read_excel(excel_path, sheet_name="List of Webpage", usecols=['Folder Name', 'Type Id', 'New Table Name'])
full_mapping = dict(zip(df_excel['Folder Name'], zip(df_excel['Type Id'], df_excel['New Table Name'])))

# Read from the invalid requests error log
with open(log_file, "r") as log:
    rows = log.read().split("\n")

hour = "06"
intended_chunks = days_back * 24 / chunk_size

# Build the result table by parsing the request summary text file
result_df = pd.DataFrame()
runtime = 0.0

folders = []
reportIDs = []
descriptions = []
codes = []
handled_500 = {}
successes = 0

for row in rows:
    row_data = row.split(" ")
    if not "to" in row and row != "":
        folders.append(row_data[1])
        reportIDs.append(row_data[0])
        codes.append(row_data[2])

        if row_data[2] == "200":
            successes += 1

        descriptions.append(full_mapping[row_data[1]][1])
    
    if "seconds" in row:
        for i in range(len(row_data)):
            if row_data[i] == "seconds":
                runtime = float(row_data[i-1])
    
    if "Folder" in row_data:
        f_name = row_data[1]
        if f_name not in handled_500:
            handled_500[f_name] = 1
        else:
            handled_500[f_name] += 1

result_df['Folder Name'] = folders
result_df['Report ID'] = reportIDs
result_df['Description'] = descriptions
result_df['Status Code'] = codes

result_df = result_df.drop_duplicates()
result_df['sort_key'] = result_df[result_df.columns[0]].str.split('_', expand=True)[0].astype(int)
result_df = result_df.sort_values(by='sort_key')
result_df = result_df.drop(columns='sort_key')

status_count = {}
for _, row in result_df.iterrows():
    if row['Status Code'] not in status_count:
        status_count[row['Status Code']] = 1
    else:
        status_count[row['Status Code']] += 1

caught_404 = []
folders_502 = []
# Do the Double-Checking.
for _, row in result_df.iterrows():
    if row['Status Code'] == "404":
        req_url = f"https://ercotapi.app.calpine.com/reports?reportId={row['Report ID']}&marketParticipantId=CRRAH&startTime={yesterday}T{hour}:00:00&endTime={today}T{hour}:00:00&unzipFiles=false"
        response = requests.get(req_url, verify=False)
        if response.status_code == 200:
            caught_404.append(row['Folder Name'])
    
    if row['Status Code'] == "502":
        folders_502.append(row['Folder Name'])

missing_folder_strings = []
html_result = "<ul>\n"

if len(caught_404) == 0 or (len(caught_404) == 1 and caught_404[0] == "51_4DASECR"):
    html_result += "<li>No folders with error code 404 were found to have uncaught data.</li>\n"

for folder_name in folders_502:
    missing_folders.append(folder_name)
    html_result += f"<li><strong>A 502 Bad Gateway Error was encountered for folder {folder_name}. See below for further instructions.</strong></li>\n"

for folder_name in caught_404:
    if folder_name != "51_4DASECR":
        html_result += f"<li><strong>{folder_name} was found to have uncaught data. Please add it manually.</strong></li>\n"

for folder_name in handled_500:
    if handled_500[folder_name] == intended_chunks or (folder_name == "51_4DASECR" and handled_500[folder_name] == 2):
        html_result += f"<li>{folder_name} had an OutOfMemory Exception that was handled successfully.</li>\n"
    else:
        missing_folders.append(folder_name)
        missing_folder_strings.append(f"<li><strong>{folder_name} had an OutOfMemory Exception that was NOT handled successfully. {int(intended_chunks)} of {chunk_size}-hour data chunks were expected and only {handled_500[folder_name]} were found.</strong></li>\n")

html_result += "".join(missing_folder_strings)
html_result += "</ul>"
folder_string = " ".join(missing_folders)

cmd_string = f"{pythonPath} {scriptPath} -r {folder_string} -c 4"
instructions = f"""
<h4 style="font-weight: normal;">To resolve this issue, follow these steps:</h4>
<ol>
    <li style="font-weight: normal;">Search for 'Anaconda Prompt' in the start menu. If it's not available, request IT to install 'Anaconda'.</li>
    <li style="font-weight: normal;">Launch 'Anaconda Prompt' to open a command prompt window.</li>
    <li style="font-weight: normal;">Copy and paste the following command into the prompt:</li>
</ol>
<pre style="font-size: smaller; margin: 0; padding: 0;">
<strong>{cmd_string}</strong>
</pre>
<p style="font-weight: normal;">Press Enter to execute the command. This will attempt to re-download all missing folder data. Expect to wait about 4-5 minutes.</p>

<h4 style="font-weight: normal;">Troubleshooting Tips and Tricks:</h4>
<ol>
    <li style="font-weight: normal;">If you believe Anaconda3 is installed properly, but the path to the Python executable in the command line argument is invalid, 
    type 'where python' in the Anaconda Prompt and make the appropriate substitutions. For future convienience, consider changing line
    15 of the error checker script with this updated path.</li>
    <li style="font-weight: normal;">After running the provided command, you can verify if the files were successfully downloaded by checking the contents of the log file located at <strong>{log_file}</strong></li>
    <li style="font-weight: normal;">If running the command results in a 'missing package/import' error, take note of the package(s) mentioned in the error prompt. You can install all missing packages by running 'conda install (package_name)' in the Anaconda Prompt.
    <li style="font-weight: normal;">If you continue to encounter Out-of-Memory issues even after running the provided command, you can try reducing the chunk size by modifying the end of the command to "-c 1". This will query the API one hour at a time for each missing folder. If the issue persists, there may not be a realistic solution available.</li>
</ol>"""

# Send the email!
body = f"""
<html>
  <body>
    <h3 style="font-weight: normal;">The following data gives the error checking results for today's MIS Download.</h3>
    
    <ol>
      <li style="font-weight: normal;">Queried Lower Bound Date: {yesterday} {hour}:00:00</li>
      <li style="font-weight: normal;">Queried Upper Bound Date: {today} {hour}:00:00</li>
      <li style="font-weight: normal;">Total Download Time: {runtime} seconds</li>
      <li style="font-weight: normal;">Request URL template: <a href="https://ercotapi.app.calpine.com/reports?reportId=(reportID)&marketParticipantId=CRRAH&startTime={yesterday}T{hour}:00:00&endTime={today}T{hour}:00:00&unzipFiles=false">https://ercotapi.app.calpine.com/reports?reportId=(reportID)&marketParticipantId=CRRAH&startTime={yesterday}T{hour}:00:00&endTime={today}T{hour}:00:00&unzipFiles=false</a></li>
      <li style="font-weight: normal;">A total of {len(result_df) - successes} folders out of 115 were not written to:
        <ul style="list-style-type: disc; margin-top: 0; padding-left: 2em;">
          {"".join(["<li>{0} folders were status code {1}</li>".format(status_count[code], code) for code in status_count])}
        </ul>
      </li>
    </ol>

    <h4 style="font-weight: normal;">Upon double checking, the following was discovered:</h4>
    {html_result}
    
"""

if len(missing_folders) > 0:
    body += instructions

body += f"""<h3 style="font-weight: normal;">Table Summary of Findings:</h3>
    <p style="margin-top: 10px;">{result_df.to_html(index=False, justify='center')}</p>
  </body>
</html>"""

msg = MIMEMultipart('alternative')
msg['Subject'] = "MIS Scheduled Download Error Checking Report"
sender = 'Stephen.Xu@calpine.com'

# Edit this line to determine who receives the email.
receivers = ['Stephen.Xu@calpine.com', 'Pranil.Walke@calpine.com']

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")
smtpObj.sendmail(sender, receivers, msg.as_string())
smtpObj.quit()
