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
from pyspark.sql.avro.functions import from_avro

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

green_avro_schema = """
 {
   "type":"record",
   "name":"topLevelRecord",
   "fields":
     [
       {"name":"VendorID","type":["int","null"]},
       {"name":"lpep_pickup_datetime","type":["string","null"]},
       {"name":"lpep_dropoff_datetime","type":["string","null"]},
       {"name":"store_and_fwd_flag","type":["string","null"]},
       {"name":"RatecodeID","type":["int","null"]},
       {"name":"PULocationID","type":["int","null"]},
       {"name":"DOLocationID","type":["int","null"]},
       {"name":"passenger_count","type":["int","null"]},
       {"name":"trip_distance","type":["double","null"]},
       {"name":"fare_amount","type":["double","null"]},
       {"name":"extra","type":["double","null"]},
       {"name":"mta_tax","type":["double","null"]},
       {"name":"tip_amount","type":["double","null"]},
       {"name":"tolls_amount","type":["double","null"]},
       {"name":"ehail_fee","type":["double","null"]},
       {"name":"improvement_surcharge","type":["double","null"]},
       {"name":"total_amount","type":["double","null"]},
       {"name":"payment_type","type":["int","null"]},
       {"name":"trip_type","type":["int","null"]},
       {"name":"congestion_surcharge","type":["double","null"]}
     ]
}
"""

green_ehub_df = (
  spark
    .readStream
    .format("eventhubs") 
    .options(**eh_stream_conf) 
    .load()
    .withColumn("body",from_avro(col("body"),green_avro_schema))
    .select("body.*")
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