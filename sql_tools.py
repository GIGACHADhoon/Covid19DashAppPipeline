import pyodbc
from dotenv import load_dotenv
import os
import geopandas as gpd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, text
load_dotenv()

class azSqlDB:
    def __init__(self):
        server = os.getenv("server")
        database = os.getenv("database")
        username = os.getenv("username")
        password = os.getenv("password")
        self.conString = 'Driver={ODBC Driver 18 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+ \
            ';UID='+username+';PWD='+ password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
    
    def sqlGeo(self,gdf):
        with pyodbc.connect(self.conString) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"IF NOT EXISTS ( \
                                SELECT * FROM sys.tables t \
                                JOIN sys.schemas s ON (t.schema_id = s.schema_id) \
                                WHERE s.name = 'dbo' AND t.name = 'nswGeo') 	\
                                CREATE TABLE nswGeo (\
                                    lgaCode VARCHAR(5),\
                                    Geom geography,\
                                    PRIMARY KEY(lgaCode));")
                
                for _,row in gdf.iterrows():
                    query = f"INSERT INTO nswGeo VALUES ('{row['LGA_CODE19']}',geography::STGeomFromText('{row['geo_wkt']}',4283))"
                    cursor.execute(query)

    def sqlLGA(self,gdf):
        with pyodbc.connect(self.conString) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"IF NOT EXISTS ( \
                                SELECT * FROM sys.tables t \
                                JOIN sys.schemas s ON (t.schema_id = s.schema_id) \
                                WHERE s.name = 'dbo' AND t.name = 'lgaDetails') 	\
                                CREATE TABLE lgaDetails (\
                                    lgaCode VARCHAR(5),\
                                    lgaName VARCHAR(100),\
                                    PRIMARY KEY(lgaCode));")
                
                for _,row in gdf.iterrows():
                    query = f"INSERT INTO lgaDetails VALUES ('{row['LGA_CODE19']}','{row['LGA_NAME19']}')"
                    cursor.execute(query)
            
    def sqlCC(self,df):
        with pyodbc.connect(self.conString) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"IF NOT EXISTS ( \
                                SELECT * FROM sys.tables t \
                                JOIN sys.schemas s ON (t.schema_id = s.schema_id) \
                                WHERE s.name = 'dbo' AND t.name = 'confirmedCases') 	\
                                CREATE TABLE confirmedCases (\
                                    date DATE, \
                                    lgaCode VARCHAR(5),\
                                    cc INT, \
                                    PRIMARY KEY(date,lgaCode));")
                for _,row in df.iterrows():
                    cursor.execute(f"INSERT INTO confirmedCases VALUES ('{row['notification_date']}','{row['lga_code19']}','{row['confirmed_cases_count']}')")