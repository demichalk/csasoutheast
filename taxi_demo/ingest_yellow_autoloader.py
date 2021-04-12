# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### ingest_yellow_autoloader notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create AutoLoader stream source

# COMMAND ----------

yellow_auto_schema = """
  VendorID integer,
  tpep_pickup_datetime string,
  tpep_dropoff_datetime string,
  passenger_count integer,
  trip_distance double,
  RatecodeID integer,
  store_and_fwd_flag string,
  PULocationID integer,
  DOLocationID integer,
  payment_type integer,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double,
  congestion_surcharge double
"""

yellow_auto_df = (
  spark.readStream.format("cloudFiles") 
    .schema(yellow_auto_schema)
    .option("cloudFiles.format", "json")        
    .option("cloudFiles.connectionString", blob_connection)
    .option("cloudFiles.resourceGroup", resource_group_name)
    .option("cloudFiles.subscriptionId", subscription_id)
    .option("cloudFiles.tenantId", tenant_id)
    .option("cloudFiles.clientId", client_id)
    .option("cloudFiles.clientSecret", client_secret)
    .option("cloudFiles.includeExistingFiles", True)
    .option("cloudFiles.maxFilesPerTrigger", 100)    
    .option("cloudFiles.useNotifications", True) 
    .load(f"wasbs://ingest@{blob_name}/drop/*.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoLoader stream into Tripdata Bronze Delta Lake table sink

# COMMAND ----------

from pyspark.sql.functions import lit, col
(
  yellow_auto_df
    .withColumn("color",lit("yellow"))
    .withColumnRenamed("tpep_pickup_datetime","pep_pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime","pep_dropoff_datetime")
    .writeStream
    .format("delta") 
    .option("checkpointLocation", f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata/yellow.checkpoint") 
    .trigger(processingTime='15 seconds') # .trigger(once=True) to demo trigger once
    .outputMode("append")
    .table("tripdata_bronze")
)