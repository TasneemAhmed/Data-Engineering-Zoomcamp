/*
Question 6. Largest tip
For the passengers picked up in September 2019 in the zone name Astoria 
which was the drop off zone that had the largest tip? We want the name of the zone
*/

select zdo."Zone", max(gt."tip_amount")
from green_taxi_data as gt
join zones as zpu
on gt."PULocationID" = zpu."LocationID"
join zones as zdo
on gt."DOLocationID" = zdo."LocationID"
where TO_CHAR(gt."lpep_pickup_datetime" :: DATE, 'Mon, yyyy') = 'Sep, 2019'
and zpu."Zone" = 'Astoria'
group by 1
order by 2 desc
limit 1