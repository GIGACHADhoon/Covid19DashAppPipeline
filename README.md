# Covid19DashAppPipeline

## Data Sources

The Pipeline Data Sources are: 
1. The covid case data for NSW by LGA was attained can be found here : https://data.nsw.gov.au/data/dataset/covid-19-cases-by-location/resource/5d63b527-e2b8-4c42-ad6f-677f14433520
2. The shapefile used to extract the Geo-Data can be found here : https://www.abs.gov.au/AUSSTATS/abs@.nsf/Lookup/1270.0.55.003Explanatory%20Notes1July%202019?OpenDocument

## The Scripts

Consider the Scripts:

1. covid_data_load.py contains the Orchestration written in Python. It will ingest the data from the sources noted above and transform them then it'll load the data on an Azure SQL Database using methods from the class in sql_tools.py
2. sql_tools.py contains the methods to push the ingested do an Azure SQL Database.