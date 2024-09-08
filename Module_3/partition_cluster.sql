-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `<GCP PJ ID>.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://mage-zoomcamp-rabz-1/ny_taxi19_20/yellow_tripdata_2019-*.csv', 'gs://mage-zoomcamp-rabz-1/ny_taxi19_20/yellow_tripdata_2020-*.csv']
);

-- Check yello trip data
SELECT * FROM <GCP PJ ID>.ny_taxi.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE <GCP PJ ID>.ny_taxi.yellow_tripdata_non_partitoned AS
SELECT * FROM <GCP PJ ID>.ny_taxi.external_yellow_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE <GCP PJ ID>.ny_taxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM <GCP PJ ID>.ny_taxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 820 MB of data
SELECT DISTINCT(VendorID)
FROM <GCP PJ ID>.ny_taxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-30';

-- Scanning ~116 MB of DATA
SELECT DISTINCT(VendorID)
FROM <GCP PJ ID>.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE <GCP PJ ID>.ny_taxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM <GCP PJ ID>.ny_taxi.external_yellow_tripdata;

-- Query scans 819.4 MB
SELECT count(*) as trips
FROM <GCP PJ ID>.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2020-05-31'
  AND VendorID=1;

-- Query scans 631.65 MB
SELECT count(*) as trips
FROM <GCP PJ ID>.ny_taxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2020-05-31'
  AND VendorID=1;