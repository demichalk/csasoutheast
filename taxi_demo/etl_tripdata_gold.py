# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### etl_gold notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream TripData Silver into TripData Gold

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

tripdata_silver_df = (
  spark
    .readStream
    .option("maxFilesPerTrigger",2)
    .table("tripdata_silver")
)
tripdata_silver_df.createOrReplaceTempView("tripdata_silver_stream")

# COMMAND ----------

tripdata_etl_sql = """
  select color, pickup_borough, dropoff_borough, pickup_zone_name, dropoff_zone_name,
    sum(passenger_count) as total_passengers, sum(total_amount) as total_amount,
    sum(trip_distance) as total_distance, sum(trip_minutes) as total_minutes
  from tripdata_silver_stream
  group by color, pickup_borough, dropoff_borough, pickup_zone_name, dropoff_zone_name
"""
tripdata_etl_df = spark.sql(tripdata_etl_sql)

# COMMAND ----------

 (
   tripdata_etl_df
      .writeStream
      .option("checkpointLocation", f"abfss://lake@{lake_name}/gold/taxidemo/tripdata/etl.checkpoint") 
      .outputMode("complete")
      .trigger(processingTime="30 seconds")
      .table("tripdata_gold")
 )