select count(*)-2 from public.green_taxi_trips_2025_11 
where date(lpep_pickup_datetime) between  '2025-11-01' and '2025-12-01' and trip_distance <=1;

SELECT
DATE(lpep_pickup_datetime) AS pickup_day,
MAX(trip_distance) AS max_trip_distance
FROM green_taxi_trips_2025_11
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_trip_distance DESC
LIMIT 1;

SELECT "PULocationID", sum(total_amount)
FROM public.green_taxi_trips_2025_11
WHERE DATE("lpep_pickup_datetime") = '2025-11-18'
group by "PULocationID" 
ORDER BY 2 DESC limit 1;


SELECT "DOLocationID",*
FROM public.green_taxi_trips_2025_11
where "PULocationID" = 74 
ORDER BY tip_amount DESC
limit 1;
