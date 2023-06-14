import smtplib
from datetime import date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import DAM_Gn_Comparator

"""
This Python tool aims to automate the comparison between today and yesterday's DAM Generator data 
by sending an email through Outlook storing the CSV that summarizes the comparison. Always defaults
to the 16th hour.
"""
today = date.today().strftime('%m/%d/%Y')
yesterday = (date.today() - timedelta(days=1)).strftime('%m/%d/%Y')

output_csv = f"./Data/MIS Gen_CIM Comparisons/" \
              f"{yesterday.replace('/', '')}_16_{today.replace('/', '')}_16.csv"

df_today = DAM_Gn_Comparator.find_generator_data(yesterday, 16)
df_yesterday = DAM_Gn_Comparator.find_generator_data(today, 16)

shared, first, second, change = DAM_Gn_Comparator.compare_data(df_yesterday, df_today)
df_csv = DAM_Gn_Comparator.write_results(shared, first, second, change, yesterday, today)

body = ""
if len(change) == 0:
    body += '<html><p> No generator statuses have changed from yesterday to today. <br> </p></html>'

body += '<html><body>' + df_csv.to_html(index=False) + '</body></html>'

msg = MIMEMultipart('alternative')
msg['Subject'] = "Daily DAM Generator Comparison Results"
sender = 'Stephen.Xu@calpine.com'
receivers = ['Stephen.Xu@calpine.com']

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")
smtpObj.sendmail(sender, receivers, msg.as_string())

smtpObj.quit()
