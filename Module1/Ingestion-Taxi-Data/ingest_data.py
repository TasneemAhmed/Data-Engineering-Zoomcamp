
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

def main(params):
    host = params.host
    username = params.user
    password = params.password
    port = params.port
    db_name = params.db_name
    taxi_table_name = params.taxi_table_name
    zones_table_name = params.zones_table_name
    taxi_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    zones_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

    def get_file_name(url):
        file_name = url.split('/')[-1]
        
        return file_name

    def read_file(url):
        os.system(f"wget {url}")
        file_name = url.split('/')[-1]
        df = pd.read_csv(file_name)

        return df    
                     

        
    
    green_taxi = read_file(taxi_url)
    print(green_taxi.head())

    # convert lpep_pickup_datetime & lpep_dropoff_datetime into datetime
    green_taxi['lpep_pickup_datetime'] = pd.to_datetime(green_taxi['lpep_pickup_datetime'])
    green_taxi['lpep_dropoff_datetime'] = pd.to_datetime(green_taxi['lpep_dropoff_datetime'])
    #print(green_taxi.dtypes)

    zones = read_file(zones_url)
    print(zones.head())


    ## 1. Connect to postgres DB

    #create engine to connect to postgresql with ny_taxi DB
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')
    #check connectivity with postgresql
    conn = engine.connect()
    #print(conn)


    # ## 2. truncate green_taxi_data tables if have any data

    #truncate green_taxi_data table if exists
    green_taxi.head(n=0).to_sql(name=taxi_table_name, con= engine, if_exists= "replace")


    # ## 3. Upload zones dataframe into zones table
    zones.to_sql(name=zones_table_name, con= engine, if_exists= "replace")


    ## 4. Read green_taxi_data as chuncks every chuncksize= 100K records

    # read file as TextFileReader
    green_taxi_iterator = pd.read_csv(get_file_name(taxi_url), iterator=True, chunksize=100000)
    green_taxi_iterator


    # ## 5. Upload green_taxi_data as chunks

    while True:
        t_start = time()
        
        try:
            #Read chuksize as dataframe
            df = next(green_taxi_iterator)
            
            # convert lpep_pickup_datetime & lpep_dropoff_datetime into datetime
            green_taxi['lpep_pickup_datetime'] = pd.to_datetime(green_taxi['lpep_pickup_datetime'])
            green_taxi['lpep_dropoff_datetime'] = pd.to_datetime(green_taxi['lpep_dropoff_datetime'])
            
            #ingest dataframe into green_taxi_data table
            df.to_sql(name=taxi_table_name, con=engine, if_exists="append")
            
            t_end = time()
            print("time to take to ingest chuksize with 100K= %.3f seconds" %(t_end-t_start))
            
        except StopIteration:
            print("Green Taxi data ingested successfully")
            break


if __name__ == '__main__':
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

    main(args)

