import requests
from bs4 import BeautifulSoup
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

os.chdir("//pzpwcmfs01/CA/11_Transmission Analysis/ERCOT/101 - Misc/CRR Limit Aggregates/Python Scripts/Animal Planet Scheduling")

# URL of the Animal Planet TV schedule page
url = "https://www.ontvtonight.com/guide/listings/channel/69047235/animal-planet-east.html"

recipient_file = "./recipients.txt"

with open(recipient_file, "r") as r_file:
    recipients = r_file.read().split("\n")

# Define time zones for ET and CT
et_timezone = timezone('US/Eastern')
ct_timezone = timezone('US/Central')

# Function to convert ET time to CT time
def et_to_ct(et_time):
    et_time = datetime.strptime(et_time, '%I:%M %p')
    et_time = et_time - timedelta(hours=1)
    return et_time.strftime('%I:%M %p')

# Function to scrape the TV schedule
def scrape_tv_schedule(url) -> pd.DataFrame:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find and extract the schedule information
    times = []
    shows = []
    episode_names = []
    seasons = []
    episode_nums = []
    descriptions = []

    # Grab the raw HTML content for the schedule table
    schedule_elements = soup.find("tbody")

    # Parse each row of the table
    for row in schedule_elements.find_all("tr"): 
        # Parse each cell to extract time, show name, and episode name
        cells = row.find_all("td")
        times.append(cells[0].text.strip())
        show_names = cells[1].text.strip().split("-")
        shows.append(show_names[0].strip().split("\n")[0])
        episode_names.append(show_names[0].strip().split("\n")[1])
        
        # Grab the season and episode number, if available
        if len(show_names) >= 2:
            stripped_season_info = show_names[1].strip()
            seasons.append(stripped_season_info[7])
            episode_idx = stripped_season_info.index("Episode")
            episode_nums.append(stripped_season_info[episode_idx + 8: episode_idx + 10])
        
        else:
            seasons.append("N/A")
            episode_nums.append("N/A")
        
        # Grab the description of the episode
        des_url = row.find("a").get('href')
        des_response = requests.get(des_url)

        if des_response.status_code == 200:
            soup = BeautifulSoup(des_response.content, "html.parser")
            description = soup.find_all("p")[1].text
            descriptions.append(description)
        
        else:
            descriptions.append("")


    res = pd.DataFrame()
    res['Time'] = times
    res['Show Name'] = shows
    res['Episode Name'] = episode_names
    res['Season'] = seasons
    res['Episode #'] = episode_nums
    res['Synopsis'] = descriptions

    return res

# Scrape the TV schedule
body = ""
tv_schedule = scrape_tv_schedule(url)
tv_schedule['Time'] = tv_schedule['Time'].apply(et_to_ct)
print(tv_schedule)

result = tv_schedule[tv_schedule['Show Name'].str.contains('Naked and Afraid', case=False)]

if not result.empty:
    body += "<h2> Naked and Afraid is on today! </h2>"

body += '<html><body>' + tv_schedule.to_html(index=False) + '</body></html>'

msg = MIMEMultipart('alternative')
msg['Subject'] = "Animal Planet Daily Schedule!"
sender = 'Stephen.Xu@calpine.com'

part2 = MIMEText(body, 'html')
msg.attach(part2)

smtpObj = smtplib.SMTP(host="relay.calpine.com")
smtpObj.sendmail(sender, recipients, msg.as_string())

smtpObj.quit()