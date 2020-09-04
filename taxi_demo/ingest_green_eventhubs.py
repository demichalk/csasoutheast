# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### ingest_green_autoloader notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Event Hubs stream source

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import from_json, col, lit

eh_stream_conf = {
  'eventhubs.connectionString' : eh_connection_encrypted
}
eh_stream_conf['maxEventsPerTrigger'] = 5000
start_pos = { "offset": -1,
              "seqNo": -1,           
              "enqueuedTime": None, 
              "isInclusive": True
            }
eh_stream_conf["eventhubs.startingPosition"] = json.dumps(start_pos)

green_schema = StructType([
  StructField("VendorID",IntegerType()),
  StructField("lpep_pickup_datetime",StringType()),
  StructField("lpep_dropoff_datetime",StringType()),
  StructField("store_and_fwd_flag",StringType()),
  StructField("RatecodeID",IntegerType()),
  StructField("PULocationID",IntegerType()),
  StructField("DOLocationID",IntegerType()),
  StructField("passenger_count",IntegerType()),
  StructField("trip_distance",DoubleType()),
  StructField("fare_amount",DoubleType()),
  StructField("extra",DoubleType()),
  StructField("mta_tax",DoubleType()),
  StructField("tip_amount",DoubleType()),
  StructField("tolls_amount",DoubleType()),
  StructField("ehail_fee",DoubleType()),
  StructField("improvement_surcharge",DoubleType()),
  StructField("total_amount",DoubleType()),
  StructField("payment_type",IntegerType()),
  StructField("trip_type",IntegerType()),
  StructField("congestion_surcharge",DoubleType()),
])

green_ehub_df = (
  spark
    .readStream
    .format("eventhubs") 
    .options(**eh_stream_conf) 
    .load()
    .select(col("body").cast(StringType()))
    .withColumn("bodyJson",from_json(col("body"),green_schema))
    .select("bodyJson.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EventHubs stream into TripData Bronze Delta sink

# COMMAND ----------

(
   green_ehub_df
    .withColumn("color",lit("green"))
    .withColumnRenamed("lpep_pickup_datetime","pep_pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime","pep_dropoff_datetime")
    .writeStream
    .format("delta") 
    .option("checkpointLocation", f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata/green.checkpoint") 
    .trigger(processingTime='15 seconds') # .trigger(once=True) to demo trigger once
    .outputMode("append")
    .start(f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read stream from the Tripdata BronzeDelta Lake table

# COMMAND ----------

yellow_bronze_df = spark.readStream.format("delta").load(f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata")
yellow_bronze_df.createOrReplaceTempView("tripdata_bronze_stream")

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) as count FROM tripdata_bronze_stream where color='green'

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Query TripData Bronze Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as count from tripdata_bronze where color='green'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from tripdata_bronze where color='green'