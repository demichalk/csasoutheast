# Databricks notebook source
# MAGIC %run ./setup_config

# COMMAND ----------

spark.conf.set("spark.sql.shufflePartitions", cluster_cores)

# COMMAND ----------

tripdata_bronze_df = (
  spark
    .readStream
    .table("tripdata_bronze")
)
tripdata_bronze_df.createOrReplaceTempView("tripdata_bronze_stream")

# COMMAND ----------

tripdata_silver_df = (
  spark
    .readStream
    .table("tripdata_silver")
)
tripdata_silver_df.createOrReplaceTempView("tripdata_silver_stream")

# COMMAND ----------

tripdata_gold_df = (
  spark
    .readStream
    .option("ignoreChanges",True)
    .table("tripdata_gold")
)
tripdata_gold_df.createOrReplaceTempView("tripdata_gold_stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TripData Bronze

# COMMAND ----------

# MAGIC %sql select color, count(*) as count from tripdata_bronze_stream
# MAGIC group by color

# COMMAND ----------

# MAGIC %sql select * from tripdata_bronze limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TripData Silver

# COMMAND ----------

# MAGIC %sql select color, count(*) as count from tripdata_silver_stream
# MAGIC group by color

# COMMAND ----------

# MAGIC %sql select * from tripdata_silver limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TripData Gold

# COMMAND ----------

# MAGIC %sql select count(*) from tripdata_gold_stream

# COMMAND ----------

stop_all_streams()
