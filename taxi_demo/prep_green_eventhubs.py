# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ##prep_green_eventhubs notebook
# MAGIC <br />
# MAGIC - Creates event hub messages from source Green Taxi cab trips

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Pull the first month of Green Taxi CSV source files and put them into EventHubs

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/green/")

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, to_json, struct

green_schema = """
  VendorID integer,
  lpep_pickup_datetime string,
  lpep_dropoff_datetime string,
  store_and_fwd_flag string,
  RatecodeID integer,
  PULocationID integer,
  DOLocationID integer,
  passenger_count integer,
  trip_distance double,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  ehail_fee double,
  improvement_surcharge double,
  total_amount double,
  payment_type integer,
  trip_type integer,
  congestion_surcharge double
"""

green_2019_df = ( 
  spark
    .read
    .format("csv")
    .schema(green_schema)
    .option("header",True)
    .load("dbfs:/databricks-datasets/nyctaxi/tripdata/green/*2019*.csv.gz")
    .orderBy(col("lpep_pickup_datetime")
    .asc())
)

# COMMAND ----------

green_2019_df.count()

# COMMAND ----------

display(green_2019_df)

# COMMAND ----------

eh_green_df = green_2019_df.select(to_json(struct(col("*"))).alias("body"))

# COMMAND ----------

display(eh_green_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark EventHubs Connector PySpark doc
# MAGIC https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md

# COMMAND ----------

import datetime 

eh_write_conf = {
  'eventhubs.connectionString' : eh_connection_encrypted,
  'eventhubs.operationTimeout' : datetime.time(0,2,0).strftime("PT%HH%MM%SS")  # 3 minute timeout
}

(
  eh_green_df
    .write
    .format("eventhubs") 
    .options(**eh_write_conf) 
    .save()
)


# COMMAND ----------

import json
eh_stream_conf = {
  'eventhubs.connectionString' : eh_connection_encrypted
}
eh_stream_conf['maxEventsPerTrigger'] = 100
start_pos = { "offset": -1,
              "seqNo": -1,           
              "enqueuedTime": None, 
              "isInclusive": True
            }
eh_stream_conf["eventhubs.startingPosition"] = json.dumps(start_pos)
messages_df = (
  spark
    .readStream
    .format("eventhubs") 
    .options(**eh_stream_conf) 
    .load()
    .select(col("body").cast(StringType()))
)
messages_df.createOrReplaceTempView("green_ehub_stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM green_ehub_stream

# COMMAND ----------


eh_batch_conf = {
  'eventhubs.connectionString' : eh_connection_encrypted
}
start_pos = { "offset": -1,
              "seqNo": -1,           
              "enqueuedTime": None, 
              "isInclusive": True
            }
eh_batch_conf["eventhubs.startingPosition"] = json.dumps(start_pos)
read_df = (
  spark
    .read
    .format("eventhubs") 
    .options(**eh_batch_conf) 
    .load()
)
display(read_df)