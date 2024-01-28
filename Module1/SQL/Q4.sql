/*Question 4. Largest trip for each day

Output: 2019-09-26
*/
select 
	cast(date_trunc('DAY', lpep_pickup_datetime) as date) as day, 
	max(trip_distance) max_trip_distance
from green_taxi_data
group by 1
order by 2 desc
limit 1;