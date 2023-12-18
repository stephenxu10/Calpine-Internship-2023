###===============================================================================================================
## Get Load participation in AS Market from YES Energy
## Reads AS Requirement from the ACCORD Database (No data prior to 2019)
## Read the data from after last record from exisitng file
## Warning : Be mindful of the throttles
##===============================================================================================================

import requests
import pandas as pd
from io import StringIO
import math
import time
import pyodbc
import numpy as np
import datetime

my_auth=('transmission.yesapi@calpine.com','texasave717')
outputroot = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\01 - General\\Extracts\\'
df3a = pd.read_csv(outputroot+'AS_ClearfromLoad_withquantity.csv')
cnxn = pyodbc.connect(driver = '{SQL Server Native Client 11.0}',server = 'pzpwbidb01',database = 'Accord',trusted_connection='yes')

iso='ERCOT'

startdate = ((pd.to_datetime(df3a['MARKETDAY'])).max()+datetime.timedelta(days=1)).strftime('%m/%d/%Y')
enddate = ((datetime.datetime.now() + datetime.timedelta(days=-60)).date()).strftime("%m/%d/%Y")

## Get RRS Clears for Loads
call1 = "https://services.yesenergy.com/PS/rest/timeseries/ERCOT_DAM_LOAD_RRS_AWARDED.csv?ISO="+iso+""
call_one=requests.get(call1, auth=my_auth)
df1 = pd.read_csv(StringIO(call_one.text))
print(df1.info())

url_string = ["ERCOT_DAM_LOAD_RRS_AWARDED:"+str(i) for i in df1.OBJECTID]

node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length


for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])

print(node_list)
    
call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[0]+""
call_three=requests.get(call3, auth=my_auth)
df2_S = pd.read_csv(StringIO(call_three.text))
df2_S = df2_S.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)
print(call3) 

for i in range(1,len(node_list)-1):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))
    
    print(i)
    
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)      
    df2_S = pd.concat([df2_S,df3],axis=1) 
    if((i>1) and (i%8)==0):
        print ("Waiting to avoid throttle")
        time.sleep(280)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2_S = df2_S.loc[:,~df2_S.columns.duplicated()]

df2_S['DAM RRS Awarded'] = df2_S[[col for col in df2_S.columns if col.endswith('(ERCOT_DAM_LOAD_RRS_AWARDED)')]].sum(axis=1)

## Dropinng all the column containing indiviual load
df2_S = df2_S.drop([col for col in df2_S.columns if col.endswith('(ERCOT_DAM_LOAD_RRS_AWARDED)')], axis=1) 


## Load which cleared NON SPIN #####
url_string = ["ERCOT_DAM_LOAD_NONSPIN_AWARDED:"+str(i) for i in df1.OBJECTID]

node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length


for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])
    
call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[0]+""
call_three=requests.get(call3, auth=my_auth)
df2_NS = pd.read_csv(StringIO(call_three.text))
df2_NS = df2_NS.drop(labels=['DATETIME','MONTH','YEAR'],axis=1) 

for i in range(1,len(node_list)-1):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)      
    df2_NS = pd.concat([df2_NS,df3],axis=1) 
    if((i>1) and (i%8)==0):
        print ("Waiting to avoid throttle")
        time.sleep(280)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2_NS = df2_NS.loc[:,~df2_NS.columns.duplicated()]

df2_NS['DAM NS Awarded'] = df2_NS[[col for col in df2_NS.columns if col.endswith('(ERCOT_DAM_LOAD_NONSPIN_AWARDED)')]].sum(axis=1)

## Dropinng all the column containing indiviual load
df2_NS = df2_NS.drop([col for col in df2_NS.columns if col.endswith('(ERCOT_DAM_LOAD_NONSPIN_AWARDED)')], axis=1) 


## Merge Spin and NonSpin Quantities
df2_S_NS = pd.merge(df2_S,df2_NS,how='inner',left_on=['HOURENDING','MARKETDAY','PEAKTYPE'],right_on=['HOURENDING','MARKETDAY','PEAKTYPE'])

## Load which cleared Regulation DOWN #####
url_string = ["ERCOT_DAM_LOAD_REGDOWN_AWARDED:"+str(i) for i in df1.OBJECTID]

node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length


for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])
    
call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[0]+""
call_three=requests.get(call3, auth=my_auth)
df2_RD = pd.read_csv(StringIO(call_three.text))
df2_RD = df2_RD.drop(labels=['DATETIME','MONTH','YEAR'],axis=1) 

for i in range(1,len(node_list)-1):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)      
    df2_RD = pd.concat([df2_RD,df3],axis=1) 
    if((i>1) and (i%48)==0):
        print ("Waiting to avoid throttle \n")
        time.sleep(280)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2_RD = df2_RD.loc[:,~df2_RD.columns.duplicated()]

df2_RD['DAM RD Awarded'] = df2_RD[[col for col in df2_RD.columns if col.endswith('(ERCOT_DAM_LOAD_REGDOWN_AWARDED)')]].sum(axis=1)

## Dropinng all the column containing indiviual load
df2_RD = df2_RD.drop([col for col in df2_RD.columns if col.endswith('(ERCOT_DAM_LOAD_REGDOWN_AWARDED)')], axis=1) 

## Merge Spin and NonSpin and Regulation Down Quantities
df2_S_NS_RD = pd.merge(df2_S_NS,df2_RD,how='inner',left_on=['HOURENDING','MARKETDAY','PEAKTYPE'],right_on=['HOURENDING','MARKETDAY','PEAKTYPE'])

## Load which cleared Regulation UP #####
url_string = ["ERCOT_DAM_LOAD_REGUP_AWARDED:"+str(i) for i in df1.OBJECTID]

node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length


for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])
    
call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[0]+""
call_three=requests.get(call3, auth=my_auth)
df2_RU = pd.read_csv(StringIO(call_three.text))
df2_RU = df2_RU.drop(labels=['DATETIME','MONTH','YEAR'],axis=1) 

for i in range(1,len(node_list)-1):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)      
    df2_RU = pd.concat([df2_RU,df3],axis=1) 
    if((i>1) and (i%48)==0):
        print ("Waiting to avoid throttle \n")
        time.sleep(280)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2_RU = df2_RU.loc[:,~df2_RU.columns.duplicated()]

df2_RU['DAM REGUP Awarded'] = df2_RU[[col for col in df2_RU.columns if col.endswith('(ERCOT_DAM_LOAD_REGUP_AWARDED)')]].sum(axis=1)

## Dropinng all the column containing indiviual load
df2_RU = df2_RU.drop([col for col in df2_RU.columns if col.endswith('(ERCOT_DAM_LOAD_REGUP_AWARDED)')], axis=1) 


## Load which cleared ECRS SCED Dispatched Awards #####
url_string = ["ERCOT_DAM_LOAD_ECRSSD_AWARDED:"+str(i) for i in df1.OBJECTID]

node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length


for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])
    
startdate = '06/10/2023'
call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[0]+""
call_three=requests.get(call3, auth=my_auth)
df2_ECRSSD = pd.read_csv(StringIO(call_three.text))
df2_ECRSSD = df2_ECRSSD.drop(labels=['DATETIME','MONTH','YEAR'],axis=1) 

for i in range(1,len(node_list)-1):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)      
    df2_ECRSSD = pd.concat([df2_ECRSSD,df3],axis=1) 
    if((i>1) and (i%48)==0):
        print ("Waiting to avoid throttle \n")
        time.sleep(280)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2_ECRSSD = df2_ECRSSD.loc[:,~df2_ECRSSD.columns.duplicated()]

df2_ECRSSD['DAM ECRSSD Awarded'] = df2_ECRSSD[[col for col in df2_ECRSSD.columns if col.endswith('(ERCOT_DAM_LOAD_ECRSSD_AWARDED)')]].sum(axis=1)

## Dropinng all the column containing indiviual load
df2_ECRSSD = df2_ECRSSD.drop([col for col in df2_ECRSSD.columns if col.endswith('(ERCOT_DAM_LOAD_ECRSSD_AWARDED)')], axis=1) 

## Load which cleared ECRS Manually Dispatched Awards #####
url_string = ["ERCOT_DAM_LOAD_ECRSMD_AWARDED:"+str(i) for i in df1.OBJECTID]

node_list_length=math.ceil(len(url_string)/75)
node_list = [0]*node_list_length


for i in range(node_list_length):
    node_list[i] = url_string[0+i*75:75+i*75]

for i in range(0,len(node_list)):
    node_list[i] = ','.join(str(j) for j in node_list[i])
    
startdate = '06/10/2023'
call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[0]+""
call_three=requests.get(call3, auth=my_auth)
df2_ECRSMD = pd.read_csv(StringIO(call_three.text))
df2_ECRSMD = df2_ECRSMD.drop(labels=['DATETIME','MONTH','YEAR'],axis=1) 

for i in range(1,len(node_list)-1):
    call3 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+node_list[i]+""
    call_three=requests.get(call3, auth=my_auth)
    df3 = pd.read_csv(StringIO(call_three.text))
    df3 = df3.drop(labels=['DATETIME','MONTH','YEAR'],axis=1)      
    df2_ECRSMD = pd.concat([df2_ECRSMD,df3],axis=1) 
    if((i>1) and (i%8)==0):
        print ("Waiting to avoid throttle \n")
        time.sleep(280)       
    else: 
       continue

## Remove duplicated columns of HOURENDINDG,MARKETDAY,PEAKTYPE   
df2_ECRSMD = df2_ECRSMD.loc[:,~df2_ECRSMD.columns.duplicated()]

df2_ECRSMD['DAM ECRSMD Awarded'] = df2_ECRSMD[[col for col in df2_ECRSMD.columns if col.endswith('(ERCOT_DAM_LOAD_ECRSMD_AWARDED)')]].sum(axis=1)

## Dropinng all the column containing indiviual load
df2_ECRSMD = df2_ECRSMD.drop([col for col in df2_ECRSMD.columns if col.endswith('(ERCOT_DAM_LOAD_ECRSMD_AWARDED)')], axis=1) 


df2_ECRS = pd.concat([df2_ECRSSD, df2_ECRSMD]).groupby(['HOURENDING','MARKETDAY','PEAKTYPE']).sum().reset_index()
df2_ECRS['DAM ECRS Awarded'] = df2_ECRS['DAM ECRSSD Awarded'] + df2_ECRS['DAM ECRSMD Awarded']
df2_ECRS = df2_ECRS.drop(labels=['DAM ECRSSD Awarded','DAM ECRSMD Awarded'],axis=1)

## Merge Spin and NonSpin and Regulation Down Quantities
df2_S_NS_RD_RU = pd.merge(df2_S_NS_RD,df2_RU,how='inner',left_on=['HOURENDING','MARKETDAY','PEAKTYPE'],right_on=['HOURENDING','MARKETDAY','PEAKTYPE'])

## Get AS Quantities from ACCORD Database
query = ' Select * from [fact_ERCOTBI_Traders].[dbo].[vw_ERCOT_DAM_Ancillary_Service_Plan] '\
        ' PIVOT (AVG([Quantity]) FOR [Ancillary_Type] IN (REGUP,REGDN,RRS,NSPIN,ECRS)) AS PivotTable '
        
df_ASQ = pd.read_sql_query(query,cnxn)         
df_ASQ = df_ASQ.drop(columns=['DST','OriginalSourceFileName'])

df_ASQ['Hour_Ending'] = df_ASQ['Hour_Ending'].str.replace(":","").astype(int)/100

df_ASQ['Delivery_date'] = pd.to_datetime(df_ASQ['Delivery_date']).dt.strftime('%m/%d/%Y')

## Rename the table before merge
df_ASQ=df_ASQ.rename(columns = {'Delivery_date':'MARKETDAY'})
df_ASQ=df_ASQ.rename(columns = {'Hour_Ending':'HOURENDING'})
