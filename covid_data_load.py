import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import numpy as np
import sqlalchemy as sal
from shapely import wkb
from sql_tools import azSqlDB

sqlCon = azSqlDB()

df = pd.read_csv('confirmed_cases_table1_location_agg.csv')
df = df[df['lga_code19'].apply(lambda x : len(x) <= 5)]
df[['notification_date','lga_code19','confirmed_cases_count']]
sqlCon.sqlCC(df.groupby(['notification_date','lga_code19'])['confirmed_cases_count'].sum().reset_index())

gdf = gpd.read_file('1270055003_lga_2019_aust_shp.zip')
gdf = gdf[(gdf['geometry'] != None) & (gdf['STE_NAME16'] == 'New South Wales')]
gdf['geo_wkt'] = gdf['geometry'].to_wkt()
sqlCon.sqlGeo(gdf[['LGA_CODE19','LGA_NAME19','geo_wkt']])