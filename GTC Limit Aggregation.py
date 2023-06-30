import pandas as pd
import numpy as np
import glob
import requests
from io import StringIO

## New List for the new results

my_auth=('transmission.yesapi@calpine.com','texasave717')
mappingroot = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\06 - CRR\\02 - Summary\\MappingDocument\\'
outputroot = '\\\\pzpwcmfs01\\CA\\11_Transmission Analysis\\ERCOT\\501 - Templates\\GTL Aggregator\\Extracts\\'

inputroot_2023 = '\\\\Pzpwuplancli01\\f$\\Uplan\\ERCOT\\MIS 2023\\12_GTL\\'
files = glob.glob(inputroot_2023 + "*.Generic_Constraints*")
list_of_dfs = [pd.read_excel(files) for files in files]
df_2023 = pd.concat(list_of_dfs, ignore_index=True)
df23Check = pd.concat(list_of_dfs, ignore_index=True)
df_2023.insert(df_2023.columns.get_loc("Time")+1,'Marketdate',df_2023['Time'].dt.strftime('%m/%d/%Y'))   
df_2023.insert(df_2023.columns.get_loc("Time")+2,'Hourending',df_2023['Time'].dt.hour+1)
df_2023= df_2023.drop('Time',axis=1) 
df_2023 = df_2023.melt(id_vars=['Marketdate','Hourending'])


inputroot_2022 = '\\\\Pzpwuplancli01\\f$\\Uplan\\ERCOT\\MIS 2022\\12_GTL\\'
files = glob.glob(inputroot_2022 + "*.Generic_Constraints*")
list_of_dfs = [pd.read_excel(files) for files in files]
df_2022 = pd.concat(list_of_dfs, ignore_index=True)
df22Check = pd.concat(list_of_dfs, ignore_index=True)
df_2022.insert(df_2022.columns.get_loc("Time")+1,'Marketdate',df_2022['Time'].dt.strftime('%m/%d/%Y'))   
df_2022.insert(df_2022.columns.get_loc("Time")+2,'Hourending',df_2022['Time'].dt.hour+1)
df_2022= df_2022.drop('Time',axis=1) 
df_2022 = df_2022.melt(id_vars=['Marketdate','Hourending'])

inputroot_2021 = '\\\\Pzpwuplancli01\\f$\\Uplan\\ERCOT\\MIS 2021\\12_GTL\\'
files = glob.glob(inputroot_2021 + "*.Generic_Constraints*")
list_of_dfs = [pd.read_excel(files) for files in files]
df_2021 = pd.concat(list_of_dfs, ignore_index=True)
df21Check = pd.concat(list_of_dfs, ignore_index=True)
df_2021.insert(df_2021.columns.get_loc("Time")+1,'Marketdate',df_2021['Time'].dt.strftime('%m/%d/%Y'))   
df_2021.insert(df_2021.columns.get_loc("Time")+2,'Hourending',df_2021['Time'].dt.hour+1)
df_2021= df_2021.drop('Time',axis=1) 
#df21Check= df21Check.drop('Time',axis=1) 
df_2021 = df_2021.melt(id_vars=['Marketdate','Hourending'])
#df21Check = df21Check.melt(id_vars=['Marketdate','Hourending'])

inputroot_2020 = '\\\\Pzpwuplancli01\\f$\\Uplan\\ERCOT\\MIS 2020\\12_GTL\\'
files = glob.glob(inputroot_2020 + "*.Generic_Constraints*")
list_of_dfs = [pd.read_excel(files) for files in files]
df_2020 = pd.concat(list_of_dfs, ignore_index=True)
df20Check = pd.concat(list_of_dfs, ignore_index=True)
df_2020.insert(df_2020.columns.get_loc("Time")+1,'Marketdate',df_2020['Time'].dt.strftime('%m/%d/%Y'))   
df_2020.insert(df_2020.columns.get_loc("Time")+2,'Hourending',df_2020['Time'].dt.hour+1)
df_2020= df_2020.drop('Time',axis=1) 
#df20Check= df20Check.drop('Time',axis=1) 
df_2020 = df_2020.melt(id_vars=['Marketdate','Hourending'])
#df20Check = df20Check.melt(id_vars=['Marketdate','Hourending'])

inputroot_2019 = '\\\\Pzpwuplancli01\\f$\\Uplan\\ERCOT\\MIS 2019\\12_GTL\\'
files = glob.glob(inputroot_2019 + "*.Generic_Constraints*")
list_of_dfs = [pd.read_excel(files) for files in files]
df_2019 = pd.concat(list_of_dfs, ignore_index=True)
df19Check = pd.concat(list_of_dfs, ignore_index=True)
df_2019.insert(df_2019.columns.get_loc("Time")+1,'Marketdate',df_2019['Time'].dt.strftime('%m/%d/%Y'))   
df_2019.insert(df_2019.columns.get_loc("Time")+2,'Hourending',df_2019['Time'].dt.hour+1)
df_2019= df_2019.drop('Time',axis=1)
#df19Check= df19Check.drop('Time',axis=1)
df_2019 = df_2019.melt(id_vars=['Marketdate','Hourending']) 
#df19Check = df19Check.melt(id_vars=['Marketdate','Hourending']) 

inputroot_2018 = '\\\\Pzpwuplancli01\\f$\\Uplan\\ERCOT\\MIS 2018\\12_GTL\\'
files = glob.glob(inputroot_2018 + "*.Generic_Constraints*")
list_of_dfs = [pd.read_excel(files) for files in files]
df_2018 = pd.concat(list_of_dfs, ignore_index=True)
df18Check = pd.concat(list_of_dfs, ignore_index=True)
df_2018.insert(df_2018.columns.get_loc("Time")+1,'Marketdate',df_2018['Time'].dt.strftime('%m/%d/%Y'))   
df_2018.insert(df_2018.columns.get_loc("Time")+2,'Hourending',df_2018['Time'].dt.hour+1)
df_2018= df_2018.drop('Time',axis=1) 
#df18Check= df18Check.drop('Time',axis=1) 
df_2018 = df_2018.melt(id_vars=['Marketdate','Hourending'])
#df18Check = df18Check.melt(id_vars=['Marketdate','Hourending'])

df_pivot = pd.concat([df_2023,df_2022,df_2021,df_2020,df_2019],axis=0)
df_pivotCheck = pd.concat([df23Check,df22Check,df21Check,df20Check,df19Check],axis=0)
#df_pivot.fillna(9999, inplace=True)

#df_pivot = df_pivot(id_vars=['Marketdate','Hourending'])

zz = df_pivot[df_pivot['variable'].str.contains('DAM')]
zz['variable'] = zz['variable'].str.replace('\nDAM', '')
zz1 = df_pivot[~df_pivot['variable'].str.contains('DAM')]

zz1.columns = ['MARKETDAY','HOURENDING','Name','Operating Limit']
zz.columns = ['MARKETDAY','HOURENDING','Name','DAM Limit']

dfResult = pd.merge(zz1, zz, on=['MARKETDAY','HOURENDING','Name'], how='inner')

dfmapping = pd.read_csv(mappingroot + 'GTC_Mapping.csv')
dfResult = pd.merge(dfResult,dfmapping,how='inner',left_on=['Name'],right_on=['Name'])   
dfResult = dfResult.drop('Name', axis=1)


call1 = "https://services.yesenergy.com/PS/rest/timeseries/COMBINED_CONSTRAINT_LIMIT.csv"
call_one=requests.get(call1, auth=my_auth)
df1 = pd.read_csv(StringIO(call_one.text))
df1 = df1[df1['NAME'].str.len() < 10]


df1 = df1[~df1['NAME'].isin(['CRLNW','S_TO_N','ZO_AJO','LISTON'])]

df1 = df1[df1['NAME'].isin(['RV_RH','WESTEX','VALEXP','N_TO_H','PNHNDL','NELRIO','NE_LOB','MCCAMY','VALIMP','EASTEX'])]

url_string = ["COMBINED_CONSTRAINT_LIMIT:"+str(i) for i in df1.OBJECTID]
url_string = ','.join(str(j) for j in url_string)

df_pivotCheck = df_pivotCheck.set_index('Time')
mindate= df_pivotCheck.index.min()
mindate = mindate.strftime('%m/%d/%Y')

startdate=mindate
enddate = 'today-1'

startdate='01/01/2019'
call1 = "https://services.yesenergy.com/PS/rest/timeseries/multiple.csv?agglevel=hour&startdate="+startdate+"&enddate="+enddate+"&items="+url_string+""
call_three=requests.get(call1, auth=my_auth)
df2 = pd.read_csv(StringIO(call_three.text))
df2 = df2.drop(labels=['DATETIME','MONTH','YEAR','PEAKTYPE'],axis=1) 
df2 = df2.replace(np.nan,9999)

df2 = df2.rename(columns={col: col.split(' (COMBINED_CONSTRAINT_LIMIT)')[0] for col in df2.columns})

df3 = pd.melt(df2, id_vars=["MARKETDAY","HOURENDING", ],
               value_name="MW")

df3 = df3.fillna(9999)
df3=df3.rename(columns = {'variable':'NAME'})
df3=df3.rename(columns = {'MW':'RT Limit'})

df4 = pd.merge(df3,dfResult,how='inner',left_on=['MARKETDAY','HOURENDING','NAME'],right_on=['MARKETDAY','HOURENDING','NAME'])
df4 = df4.drop_duplicates()
df4 = df4.replace(np.nan,9999)

df4.to_csv(outputroot + 'GTC_Aggregator.csv',index=False)