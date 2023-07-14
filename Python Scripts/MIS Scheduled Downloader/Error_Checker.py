from datetime import date, datetime, timedelta
import os
import pandas as pd
import requests
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import warnings

"""
This Python script performs error checking to ensure that all the available MIS data for the current day has been downloaded correctly.

Sends an email that contains the following information:
    - The lower and upper bound for the queried datetimes.
    - A table storing Folder Name, Report ID, Description, and Status Code
    - A URL template for manual checking, i.e.
        https://ercotapi.app.calpine.com/reports?reportId={reportID}&marketParticipantId=CRRAH&startTime=2023-07-12T09:00:00&endTime=2023-07-13T09:00:00&unzipFiles=false
    - A quick double-checking report:
        - Verifies that there is indeed no data available for all folders with status code 404.
        - Verifies that all 500 error code folders were properly handled.
        - Writes bolded alerts for all other exceptions, or if there is any uncaught data.

Reads from 'invalid_requests.txt', an error log file from the Python MIS downloader.
"""
# Ignore warnings. Whatever.
warnings.simplefilter("ignore")

log_file = "//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/MIS Scheduled Downloader/request_summary.txt"
yesterday = (date.today() - timedelta(days=2)).strftime('%Y-%m-%d')
today = (date.today() - timedelta(days=0)).strftime('%Y-%m-%d')

# Reference Excel Sheet for all the web data and requirements
excel_path = "\\\\Pzpwuplancli01\\APP-DATA\\Task Scheduler\\MIS_Download_210125a_v3_via_API.xlsm"

df_excel = pd.read_excel(excel_path, sheet_name="List of Webpage", usecols=['Folder Name', 'Type Id', 'New Table Name'])
full_mapping = dict(zip(df_excel['Folder Name'], zip(df_excel['Type Id'], df_excel['New Table Name'])))

# Read from the invalid requests error log
with open(log_file, "r") as log:
    rows = log.read().split("\n")

hour = "06"
intended_chunks = 8

# Build the result table by parsing the request summary text file
result_df = pd.DataFrame()
runtime = 0.0

folders = []
reportIDs = []
descriptions = []
codes = []
handled_500 = {}
for row in rows:
    row_data = row.split(" ")
    if not "to" in row and row != "":
        folders.append(row_data[1])
        reportIDs.append(row_data[0])
        codes.append(row_data[2])
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
result_df = result_df.sort_values(by=result_df.columns[0], key=lambda x: x.str.split('_', expand=True)[0].astype(int))

status_count = {}
for _, row in result_df.iterrows():
    if row['Status Code'] not in status_count:
        status_count[row['Status Code']] = 1
    
    else:
        status_count[row['Status Code']] += 1

caught_404 = []

# Do the Double-Checking.
for _, row in result_df.iterrows():
    if row['Status Code'] == "404":
        req_url = f"https://ercotapi.app.calpine.com/reports?reportId={row['Report ID']}&marketParticipantId=CRRAH&startTime={yesterday}T{hour}:00:00&endTime={today}T{hour}:00:00&unzipFiles=false"
        response = requests.get(req_url, verify=False)

        if response.status_code == 200:
            print(req_url)
            caught_404.append(row['Folder Name'])
    

html_result = ""

if len(caught_404) == 0:
    html_result = "<ul>\n<li><strong>No folders with error code 404 were found to have uncaught data.</strong></li>\n</ul>"
    for folder_name in handled_500:
        if handled_500[folder_name] == intended_chunks:
            html_result += f"<li><strong>{folder_name} had an OutOfMemory Exception that was handled successfully.</strong></li>\n"
        
        else:
            html_result += f"<li><strong>{folder_name} had an OutOfMemory Exception that was not handled successfully. {intended_chunks} of 8-hour data were expected and only {handled_500[folder_name]} were found. Consider manually adding in any missing chunks.</strong></li>\n"
else:
    html_result = "<ul>\n"
    for folder_name in caught_404:
        html_result += f"<li><strong>{folder_name} was found to have uncaught data. Please add it in manually.</strong></li>\n"
    
    for folder_name in handled_500:
        if handled_500[folder_name] == intended_chunks:
            html_result += f"<li><strong>{folder_name} had an OutOfMemory Exception that was handled successfully.</strong></li>\n"
        
        else:
            html_result += f"<li><strong>{folder_name} had an OutOfMemory Exception that was not handled successfully. {intended_chunks} of 8-hour data chunks were expected and only {handled_500[folder_name]} were found. Consider manually adding in any missing chunks.</strong></li>\n"
    html_result += "</ul>"
    

# Send the email!
body = """
<html>
  <body>
    <h3 style="font-weight: normal;">The following data gives the error checking results for today's MIS Download.</h3>
    
    <h4 style="font-weight: normal;">Queried Lower Bound Date: {0} {7}:00:00</h4>
    <h4 style="font-weight: normal;">Queried Upper Bound Date: {1} {7}:00:00</h4>

    <h4 style="font-weight: normal;">Total Download Time: {2} seconds</h4>
    
    <h4 style="font-weight: normal;">Request URL template: <a href="https://ercotapi.app.calpine.com/reports?reportId=(reportID)&marketParticipantId=CRRAH&startTime={0}T{7}:00:00&endTime={1}T{7}:00:00&unzipFiles=false">https://ercotapi.app.calpine.com/reports?reportId=(reportID)&marketParticipantId=CRRAH&startTime={0}T{7}:00:00&endTime={1}T{7}:00:00&unzipFiles=false</a></h4>
    
    <h4 style="font-weight: normal;">A total of {3} folders out of 115 were not written to:</h4>
    <ul>
        {4}
    </ul style="font-weight: normal;">

    <h4 style="font-weight: normal;"> Upon double checking, the following was discovered: </h4>
        {6}
    
    <h3 style="font-weight: normal;"> Table Summary of Findings: </h3>
    <p style="margin-top: 10px;">{5}</p>
  </body>
</html>
""".format(yesterday, today, runtime, len(result_df), ''.join(["<li>{0} folders were status code {1}</li>".format(status_count[code], code) for code in status_count]), result_df.to_html(index=False, justify='center'), html_result, hour)

msg = MIMEMultipart('alternative')
msg['Subject'] = "MIS Scheduled Download Error Checking Report"
sender = 'Stephen.Xu@calpine.com'

# Edit this line to determine who receives the email.
receivers = ['Stephen.Xu@calpine.com']

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")
smtpObj.sendmail(sender, receivers, msg.as_string())

smtpObj.quit()

