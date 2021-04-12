# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### ingest_green_ehub_kafka notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Event Hubs Kafka API stream source

# COMMAND ----------

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

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit
from pyspark.sql.avro.functions import from_avro


green_ehub_df = (
  spark
    .readStream
    .format("kafka") 
    .option("subscribe", kafka_topic)
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.security.protocol", "SASL_SSL") 
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) 
    .option("kafka.session.timeout.ms", "60000") 
    .option("kafka.request.timeout.ms", "30000") 
    .option("kafka.group.id", "$Default") 
    .option("failOnDataLoss", "false") 
    .option("startingOffsets", "earliest")
    .load()
    .withColumn("value",from_avro(col("value"),green_avro_schema))
    .select("value.*")
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
    .table("tripdata_bronze")
)