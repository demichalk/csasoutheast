# Databricks notebook source
# MAGIC %md 
# MAGIC ## setup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE demo2020 CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo2020.zones
# MAGIC (
# MAGIC   LocationID integer, 
# MAGIC   Borough string, 
# MAGIC   Zone string, 
# MAGIC   service_zone string
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://lake@fieldengdeveastus2adls.dfs.core.windows.net/bronze/demo2020/zones'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo2020.tripdata
# MAGIC (
# MAGIC   color string,
# MAGIC   VendorID integer,
# MAGIC   pep_pickup_datetime string,
# MAGIC   pep_dropoff_datetime string,
# MAGIC   store_and_fwd_flag string,
# MAGIC   RatecodeID integer,
# MAGIC   PULocationID integer,
# MAGIC   DOLocationID integer,
# MAGIC   passenger_count integer,
# MAGIC   trip_distance double,
# MAGIC   fare_amount double,
# MAGIC   extra double,
# MAGIC   mta_tax double,
# MAGIC   tip_amount double,
# MAGIC   tolls_amount double,
# MAGIC   ehail_fee double,
# MAGIC   improvement_surcharge double,
# MAGIC   total_amount double,
# MAGIC   payment_type integer,
# MAGIC   trip_type integer,
# MAGIC   congestion_surcharge double
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://lake@fieldengdeveastus2adls.dfs.core.windows.net/bronze/demo2020/tripdata'

# COMMAND ----------

# MAGIC %md ## copy into

# COMMAND ----------

# MAGIC %sql
# MAGIC   COPY INTO demo2020.zones
# MAGIC   FROM (SELECT CAST(LocationID AS integer), Borough, Zone, service_zone FROM 'dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv')
# MAGIC   FILEFORMAT = CSV
# MAGIC   FORMAT_OPTIONS('header' = 'true')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from demo2020.zones

# COMMAND ----------

# MAGIC  %sql
# MAGIC   COPY INTO demo2020.tripdata
# MAGIC   FROM (
# MAGIC     select
# MAGIC       'yellow' as color,
# MAGIC       cast(VendorID as integer),
# MAGIC       tpep_pickup_datetime as pep_pickup_datetime,
# MAGIC       tpep_dropoff_datetime as pep_dropoff_datetime,
# MAGIC       cast(passenger_count as integer),
# MAGIC       cast(trip_distance as double),
# MAGIC       cast(RatecodeID as integer),
# MAGIC       store_and_fwd_flag,
# MAGIC       cast(PULocationID as integer),
# MAGIC       cast(DOLocationID as integer),
# MAGIC       cast(payment_type as integer),
# MAGIC       cast(fare_amount as double),
# MAGIC       cast(extra as double),
# MAGIC       cast(mta_tax as double),
# MAGIC       cast(tip_amount as double),
# MAGIC       cast(tolls_amount as double),
# MAGIC       cast(improvement_surcharge as double),
# MAGIC       cast(total_amount as double),
# MAGIC       cast(congestion_surcharge as double)
# MAGIC     FROM 'dbfs:/databricks-datasets/nyctaxi/tripdata/yellow'
# MAGIC   )
# MAGIC   FILEFORMAT = CSV
# MAGIC   PATTERN = 'yellow_tripdata_2019*.csv.gz'
# MAGIC   FORMAT_OPTIONS('header' = 'true')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from demo2020.tripdata