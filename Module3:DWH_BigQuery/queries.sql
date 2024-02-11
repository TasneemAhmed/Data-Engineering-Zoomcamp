/*
   Question 1. What is count of records for the 2022 Green Taxi Data?
Output: 840,402 records
*/
select count(*) as count_records
from `zoomcamp-module3.ny_taxi_data.green_taxi2022`;
#############################################################################
/*
Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
*/
/* 
First show External table green_taxi2022
0 Bytes for external table
*/
select count(distinct PULocationID) as count_records
from `zoomcamp-module3.ny_taxi_data.green_taxi2022`;

/* 
Second show Materialized table green_taxi2022
6.41 Bytes for materialized table
*/
select count(distinct PULocationID) as count_records
from `zoomcamp-module3.ny_taxi_data.green_taxi2022_materialized`;
#######################################################################################
/*
   Question 3:How many records have a fare_amount of 0?
Output: 1622 records
*/
select count(*) as count_fare_amount_0
from `zoomcamp-module3.ny_taxi_data.green_taxi2022`
where fare_amount = 0;
########################################################################################
/*
   Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022
Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
*/

/*
First show green_taxi2022_materialized
Output: processed 12.82 MB
*/
select count(distinct PULocationID) as count_recorde
from `zoomcamp-module3.ny_taxi_data.green_taxi2022_materialized`
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';

/*
Second show green_taxi2022_materialized_partitioned
Output: processed 1.12 MB
*/
select count(distinct PULocationID) as count_recorde
from `zoomcamp-module3.ny_taxi_data.green_taxi2022_materialized_partitioned`
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30';
#############################################################################################################################