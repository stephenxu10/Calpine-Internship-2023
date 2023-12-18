##===============================================================================================================
## Eliminates Load zones and Hubs
##===============================================================================================================
import requests
import pandas as pd
from io import StringIO
import math
from datetime import datetime, timedelta
import time

my_auth=('transmission.yesapi@calpine.com','texasave717')
start_time = time.time()

outputroot = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\01 - General\\Extracts\\DAM_ENERGY_SOLD_DELTA.csv'
outputroot_hr = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\01 - General\\Extracts\\DAM_ENERGY_SOLD_DELTA_HOURLY.csv'
iso='ERCOT'
PEAKTYPE=('WDPEAK','WEPEAK','OFFPEAK')

startdate = 'today-120'
enddate = 'today+1'

call1 = "https://services.yesenergy.com/PS/rest/timeseries/TOTAL_DAM_ENERGY_SOLD.csv?ISO="+iso+""
print(call1)
call_one=requests.get(call1, auth=my_auth)
df1 = pd.read_csv(StringIO(call_one.text))

df1 = df1[~df1['NAME'].str.startswith(('LZ_', 'HB_', 'BRP_'))]
print(df1.info())

url_string = ["TOTAL_DAM_ENERGY_SOLD:"+str(i) for i in df1.OBJECTID]
node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length

for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])

merged = []
print(len(node_list))
for i in range(len(node_list)):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))  
    print(df3.info())      
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)

    merged.append(df3)

    ## IMPORTANT: Experiment with this throttle sleeping process to find the best balance between wait time and reliability.
    if i > 0 and i % 5 == 0:
        print ("Waiting to avoid throttle")
        time.sleep(250)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2 = pd.concat(merged, axis=1)
df2 = df2.loc[:,~df2.columns.duplicated()]

## Removing ' (TOTAL_DAM_ENERGY_SOLD)' from the column headers
df5 = df2.rename(columns={col: col.split(' (TOTAL_DAM_ENERGY_SOLD)')[0] for col in df2.columns})
df5 = df5.fillna(0)
cols = [col for col in df5.columns if col not in ['HOURENDING','MARKETDAY','PEAKTYPE']]

df6 = pd.melt(df5, id_vars=["HOURENDING", "MARKETDAY","PEAKTYPE"],
               value_name="MW")

df6=df6.rename(columns = {'variable':'NODENAME'})

df6['MARKETDAY'] = pd.to_datetime(df6['MARKETDAY'])

# Calculate the date 30 days before today
thirty_days_ago = datetime.now() - timedelta(days=30)

# Filter the DataFrame
dam_df = df6[df6['MARKETDAY'] >= thirty_days_ago]

dam_df['Delta'] = dam_df.groupby(['NODENAME', 'PEAKTYPE'])['MW'].diff().fillna(0)
dam_df.sort_index(inplace=True)

dam_df.to_csv(outputroot_hr, index=False)

df6_daily = df6.groupby(['MARKETDAY','PEAKTYPE','NODENAME'],as_index=False)['MW'].mean()

## This modified version computes the delta column by grouping together each (Node, Peaktype) combination
## and subtracting the current day MW from the next day MW.
df6_daily = df6_daily.sort_values(by=['NODENAME', 'PEAKTYPE', 'MARKETDAY'])
df6_daily['Delta'] = df6_daily.groupby(['NODENAME', 'PEAKTYPE'])['MW'].diff().fillna(0)
df6_daily.sort_index(inplace=True)

df6_daily.to_csv(outputroot, index=False)

end_time = time.time()
execution_time = (end_time - start_time)
print("Generation Complete")
print(f"The script took {execution_time:.2f} seconds to run.")