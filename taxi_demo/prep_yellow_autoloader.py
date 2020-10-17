# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ##prep_yellow_autoloader notebook
# MAGIC <br />
# MAGIC - Creates small JSON files staged in DBFS from source Yellow Taxi cab trips
# MAGIC - Copy into drop Blob Storage folder

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Clear the local staging folder that holds the small files from DBFS

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/taxidemo/yellow/", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Clear the drop folder in Blob Storage that will be the autoloader source  

# COMMAND ----------

dbutils.fs.rm(f"wasbs://ingest@{blob_name}/drop", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Pull the first month of Yellow Taxi CSV source files and repartition them into 10000 JSON files in DBFS

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")

# COMMAND ----------

from pyspark.sql.functions import col

yellow_schema = """
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

yellow_201901_df = (
    spark
      .read
      .format("csv")
      .schema(yellow_schema)
      .option("header",True)
      .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019*.csv.gz")
      .orderBy(col("tpep_pickup_datetime").asc())
)

# COMMAND ----------

yellow_201901_df.repartitionByRange(10000,"tpep_pickup_datetime").write.format("json").save("dbfs:/mnt/taxidemo/yellow/")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Copy DBFS json files into the Blob Storage drop folder that will be the autoloader source  

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/taxidemo/yellow/",f"wasbs://ingest@{blob_name}/drop",True)