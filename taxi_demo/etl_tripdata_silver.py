# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### etl_silver_gold notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream TripData Bronze into TripData Silver

# COMMAND ----------

tripdata_bronze_df = (
  spark
    .readStream
    .format("delta")
    .load(f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata")
)
tripdata_bronze_df.createOrReplaceTempView("tripdata_bronze_stream")

# COMMAND ----------

tripdata_etl_sql = """
  SELECT 
      color,
      CAST(pep_pickup_datetime as timestamp) as pickup_time,
      CAST(pep_pickup_datetime as date) as pickup_date,
      CAST(pep_dropoff_datetime as timestamp) as dropoff_time,
      PULocationID as pickup_zone_id,
      zbpu.Borough as pickup_borough,
      zbpu.Zone as pickup_zone_name,
      DOLocationID as dropoff_zone_id,
      zbdo.Borough as dropoff_borough,
      zbdo.Zone as dropoff_zone_name,
      passenger_count,
      trip_distance,
      (
        ROUND(
          (CAST(CAST(pep_dropoff_datetime as timestamp) as long) -
          CAST(CAST(pep_pickup_datetime as timestamp) as long)) / 60
        )
      ) as trip_minutes,
      tip_amount,
      total_amount
    FROM tripdata_bronze_stream tb
    JOIN zones_bronze zbpu ON tb.PULocationID=zbpu.LocationID
    JOIN zones_bronze zbdo ON tb.DOLocationID=zbdo.LocationID
    WHERE CAST(pep_pickup_datetime as timestamp) > '2018-12-31T23:59:59' 
      AND CAST(pep_pickup_datetime as timestamp) < '2021-01-01T00:00:00' 
"""
tripdata_etl_df = spark.sql(tripdata_etl_sql)

# COMMAND ----------

def processETL(batch_df, batch_id):
  batch_df.createOrReplaceTempView("tripdata_etl_batch")
  batch_df._jdf.sparkSession().sql("""
    MERGE INTO tripdata_silver ts
    USING tripdata_etl_batch tb
    ON ts.pickup_date = tb.pickup_date AND ts.pickup_time = tb.pickup_time AND ts.dropoff_time = tb.dropoff_time and ts.color = tb.color
    WHEN NOT MATCHED
      THEN INSERT *
  """)
  
(
    tripdata_etl_df
      .writeStream
      .foreachBatch(processETL)
      .option("checkpointLocation", f"abfss://lake@{lake_name}/silver/taxidemo/tripdata/etl.checkpoint") 
      .trigger(processingTime='1 minute')
      .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read stream from the Tripdata Silver Delta Lake table

# COMMAND ----------

tripdata_silver_df = (
  spark
    .readStream
    .format("delta")
    .load(f"abfss://lake@{lake_name}/silver/taxidemo/tripdata")
)
tripdata_silver_df.createOrReplaceTempView("tripdata_silver_stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select count(*) from tripdata_silver_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query TripData 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select count(*) from tripdata_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tripdata_silver