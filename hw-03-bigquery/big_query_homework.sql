-- These are the queried executed for the DE Zoomcamp Homework about BigQuery

-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-412614.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://terraform-dtc-de-bucket-01/green/green_tripdata_20*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `dtc-de-course-412614.ny_taxi.green_tripdata_nonpartitioned` AS
SELECT * FROM `dtc-de-course-412614.ny_taxi.external_green_tripdata`;

-- Counting the number of records from the external table
select count(*)
from `dtc-de-course-412614.ny_taxi.external_green_tripdata`;


-- Counting distinct PULocationIDs from both tables (external and nonpartitioned)
select distinct PULocationID
from `dtc-de-course-412614.ny_taxi.external_green_tripdata`;

select distinct PULocationID
from `dtc-de-course-412614.ny_taxi.green_tripdata_nonpartitioned`;


-- Counting how many records have a fare_amount of 0
select count(fare_amount)
from `dtc-de-course-412614.ny_taxi.green_tripdata_nonpartitioned`
where fare_amount=0;


-- Creating a partition and cluster table. Partition by 
CREATE OR REPLACE TABLE `dtc-de-course-412614.ny_taxi.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dtc-de-course-412614.ny_taxi.external_green_tripdata`;

-- Comparing the estimated bytes to be proccessed between the nonpartitioned and the partitioned table
-- On Nonpartitioned table
select distinct PULocationID
from `dtc-de-course-412614.ny_taxi.green_tripdata_nonpartitioned`
where lpep_pickup_datetime>= '2022-06-01T00:00:00' and 
lpep_pickup_datetime<= '2022-06-30T23:59:59';

-- On Partitioned table
select distinct PULocationID
from `dtc-de-course-412614.ny_taxi.green_tripdata_partitioned_clustered`
where lpep_pickup_datetime>= '2022-06-01T00:00:00' and 
lpep_pickup_datetime<= '2022-06-30T23:59:59';

-- Bonus selecting count(*) from the materialized table (non-partitioned)
select count(*)
from `dtc-de-course-412614.ny_taxi.green_tripdata_nonpartitioned`;
