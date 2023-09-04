import pandas as pd
import geopandas as gpd
from sql_tools import azSqlDB
import numpy as np

sqlCon = azSqlDB()

df = pd.read_csv('confirmed_cases_table1_location_agg.csv')
df = df[df['lga_code19'].apply(lambda x : len(x) <= 5)]
df[['notification_date','lga_code19','confirmed_cases_count']]
sqlCon.sqlCC(df.groupby(['notification_date','lga_code19'])['confirmed_cases_count'].sum().reset_index())


gdf = gpd.read_file('1270055003_lga_2019_aust_shp.zip')
gdf = gdf[(gdf['geometry'] != None) & (gdf['STE_NAME16'] == 'New South Wales')]
gdf['geo_wkt'] = gdf['geometry'].to_wkt()
sqlCon.sqlGeo(gdf[['LGA_CODE19','geo_wkt']])
sqlCon.sqlLGA(gdf[['LGA_CODE19','LGA_NAME19']])

# Load initial Data
df1 = pd.read_excel('twenty19.xlsx')
df2 = pd.read_excel('twenty20.xlsx')
df3 = pd.read_excel('twenty21.xlsx')
# Rearrange columns
df1 = df1[['lga','date','State','Dose1','Dose2']]
df1['Dose3'] = [np.nan]*df1.shape[0]
df1['Dose4'] = [np.nan]*df1.shape[0]

df2 = df2[['lga','date','State','Dose1','Dose2','MT2']]
df2['Dose4'] = [np.nan]*df2.shape[0]
df2.columns = ['lga','date','State','Dose1','Dose2','Dose3','Dose4']

df3 = df3[['lga','date','State','DoseOld1','DoseOld2','DoseOld3','DoseOlder4']]
df3.columns = ['lga','date','State','Dose1','Dose2','Dose3','Dose4']
# Concat all data into a single DataFrame
df = pd.concat([df1,df2,df3])
df.drop(['State'],axis=1,inplace=True)
df['lga'] = df['lga'].apply(lambda x : x.replace(' (A)','').replace(' (C)',''))
sqlCon.sqlVax(df)