
# # Read Taxi data and ingest into Postgres tables
# - read green txi data csv file as pandas dataframe
# - read taxi+zone lookup csv file 
# - upload taxi+zone lookup into zones table
# - read green txi data as chuncks
# - convert lpep_pickup_datetime	lpep_dropoff_datetime into datetime datatype
# - upload data into green_taxi_data into table


import pandas as pd
from sqlalchemy import create_engine
import os
from time import time

"""
argparse used to incorporate the parsing of command line arguments. 
Instead of having to manually set variables inside of the code,
argparse can be used to add flexibility and reusability to your code by allowing user input values to be parsed and utilized.
"""
import argparse

"""
########################## ConnectDB class ###########################################
    1. initiate with parameters needed for to connect to postgres DB
    2. Create engine with postgresql and fed with parameters needed
"""
class ConnectDB():
    def __init__(self, host, user, password, port, db_name):
        self.host_name = host
        self.user_name = user
        self.password = password
        self.port = port
        self.db_name = db_name

    def connect_engine(self):
        #create engine to connect to postgresql with ny_taxi DB
        engine = create_engine(f'postgresql://{self.user_name}:{self.password}@{self.host_name}:{self.port}/{self.db_name}') 

        return engine

"""
####################################### ReadData class ######################################################################
    1. initate with get/download data source
    2. assign file name, 
    for ex url= https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
    filename = green_tripdata_2019-09.csv.gz
"""  
class ReadData():
    def __init__(self, url):
        self.url = url
        os.system(f"wget {self.url}")
        self.file_name = self.url.split('/')[-1]
    
    def read_file(self):
        self.df = pd.read_csv(self.file_name)

        return self.df

"""
############################################### IngestZonesData class ###################################################
    1. initiate with assign url(attribute from ReadData superclass), connection with DB, & table_name where data loaded
    2. Create zones table & load df (which returned from ReadData.read_file() which in superclass)
"""
class IngestZonesData(ReadData):
    def __init__(self, url, conn, table_name):
        super().__init__(url)
        self.engine = conn
        self.table_name = table_name

    def ingest_data(self):
        super().read_file().to_sql(name=self.table_name, con= self.engine, if_exists= "replace")

"""
############################################ IngestTaxisData class ########################################################
    1. initiate with assign url(attribute from ReadData superclass), connection with DB, & table_name where data loaded
    2. Create green_taxi_data table & load df (which returned from ReadData.read_file() which in superclass) but with 0 rows
    but before that change datatype of lpep_pickup_datetime & lpep_dropoff_datetime into datetime
    
    3. Ingest data into table but with chunks, every chuksize = 100K rows
"""
class IngestTaxisData(ReadData):
    def __init__(self, url, conn, table_name):
        super().__init__(url)
        self.engine = conn
        self.table_name = table_name

    def creat_table(self):
        df = super().read_file()
        # convert lpep_pickup_datetime & lpep_dropoff_datetime into datetime
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df.head(n=0).to_sql(name=self.table_name, con= self.engine, if_exists= "replace")

    def ingest_data(self):
        green_taxi_iterator = pd.read_csv(self.file_name, iterator=True, chunksize=100000)

       #Upload green_taxi_data as chunks

        while True:
            t_start = time()
            
            try:
                #Read chuksize as dataframe
                green_taxi = next(green_taxi_iterator)
                
                # convert lpep_pickup_datetime & lpep_dropoff_datetime into datetime
                green_taxi['lpep_pickup_datetime'] = pd.to_datetime(green_taxi['lpep_pickup_datetime'])
                green_taxi['lpep_dropoff_datetime'] = pd.to_datetime(green_taxi['lpep_dropoff_datetime'])
                
                #ingest dataframe into green_taxi_data table
                green_taxi.to_sql(name=self.table_name, con=self.engine, if_exists="append")
                
                t_end = time()
                print("time to take to ingest chuksize with 100K= %.3f seconds" %(t_end-t_start))
                
            except StopIteration:
                print("Green Taxi data ingested successfully")
                break


if __name__ == '__main__':

    taxi_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    zones_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    # Create the parser
    parser = argparse.ArgumentParser()
    # Add an argument:
        #host
        #user
        #password
        #port
        #db_name
        #table_name
        #url
    parser.add_argument('--host', required=True, help="host for postgres")
    parser.add_argument('--user', required=True, help="user for postgres")
    parser.add_argument('--password', required=True, help="password for postgres")
    parser.add_argument('--port', required=True, help="port for postgres")
    parser.add_argument('--db_name', required=True, help="DB name in postgres")
    parser.add_argument('--taxi_table_name', required=True, help="Taxi table name will ingest data in")
    parser.add_argument('--zones_table_name', required=True, help="Zones table name will ingest data in")


    # Parse the argument
    args = parser.parse_args()
    
    # Connect to postgres DB
    engine_postgres = ConnectDB(args.host, args.user, args.password, args.port, args.db_name)
    connect_postgres = engine_postgres.connect_engine()

    # Crate & ingest zones data into Postgre DB
    zones = IngestZonesData(url=zones_url, conn=connect_postgres, table_name= args.taxi_table_name)
    zones.ingest_data()

    # Crate & ingest green taxi data into Postgre DB
    green_taxi = IngestTaxisData(url=taxi_url, conn=connect_postgres, table_name= args.taxi_table_name)
    green_taxi.creat_table()
    green_taxi.ingest_data()
