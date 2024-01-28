/*Question 3. Count records
How many taxi trips were totally made on September 18th 2019?

Output: 15612*/

select count(*) as count_trips
from green_taxi_data
where cast(lpep_pickup_datetime as date) = '2019-09-18'
and cast(lpep_dropoff_datetime as date) = '2019-09-18';


