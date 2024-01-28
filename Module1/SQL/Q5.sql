/*
Question 5. Three biggest pick up Boroughs
Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

Solution: "Brooklyn" "Manhattan" "Queens"
*/

select z."Borough", sum(gt."total_amount") as sum_total_amount
from green_taxi_data as gt
join zones as z 
on gt."PULocationID" = z."LocationID"
where cast(lpep_pickup_datetime as date) = '2019-09-18'
group by 1
order by 2 desc;