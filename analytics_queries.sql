CREATE OR REPLACE TABLE `uber-data-pipepline.uber_data.table_analytics` AS (
SELECT
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.pickup_latitude,
pick.pickup_longitude,
dropp.dropoff_latitude,
dropp.dropoff_longitude,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount


FROM `uber-data-pipepline.uber_data.fact_table` f
JOIN `uber-data-pipepline.uber_data.datetime_dim` d
ON f.datetime_id = d.datetime_id

JOIN `uber-data-pipepline.uber_data.passenger_count_dim` p
ON f.passenger_count_id = p.passenger_count_id

JOIN `uber-data-pipepline.uber_data.trip_distance_dim` t
ON f.trip_distance_id = t.trip_distance_id

JOIN `uber-data-pipepline.uber_data.rate_code_dim` r
ON f.rate_code_id = r.rate_code_id

JOIN `uber-data-pipepline.uber_data.pickup_location_dim` pick
ON f.pickup_location_id = pick.pickup_location_id

JOIN `uber-data-pipepline.uber_data.dropoff_location_dim` dropp
ON f.dropoff_location_id = dropp.dropoff_location_id

JOIN `uber-data-pipepline.uber_data.payment_type_dim` pay
ON f.payment_type_id = pay.payment_type_id
);



--  TEST QUERIES
SELECT * FROM `uber-data-pipepline.uber_data.fact_table` LIMIT 1000;


SELECT b.payment_type_name, AVG(a.tip_amount) FROM `uber-data-pipepline.uber_data.fact_table` a
JOIN `uber-data-pipepline.uber_data.payment_type_dim` b
ON a.payment_type_id = b.payment_type_id
GROUP BY b.payment_type_name
