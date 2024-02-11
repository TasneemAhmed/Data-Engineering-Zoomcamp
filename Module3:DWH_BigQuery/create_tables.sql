#### First create table to get data from GCP storage buckets ###########
CREATE OR REPLACE EXTERNAL TABLE zoomcamp-module3.ny_taxi_data.green_taxi2022
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-green-taxi-data2022-to-bq/green_taxi_2022 dataset/*']
);

#### Second need to create materalized table from green_taxi2022 ########
CREATE OR REPLACE TABLE zoomcamp-module3.ny_taxi_data.green_taxi2022_materialized
AS 
(select * from zoomcamp-module3.ny_taxi_data.green_taxi2022);

#### Third need to create create partitioned  table from green_taxi2022 ########
CREATE OR REPLACE TABLE zoomcamp-module3.ny_taxi_data.green_taxi2022_materialized_partitioned
partition by DATE(lpep_pickup_datetime)
AS 
(select * from zoomcamp-module3.ny_taxi_data.green_taxi2022);

##### show each partition, rows number of each partition & number of GB scaned in each partition
 select PARTITION_ID, TOTAL_ROWS, round(TOTAL_LOGICAL_BYTES/(1024*3), 2) as number_GB_per_partition
from ny_taxi_data.INFORMATION_SCHEMA.PARTITIONS
where TABLE_NAME = 'green_taxi2022_materialized_partitioned';