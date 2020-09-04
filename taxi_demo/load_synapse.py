# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### load_synapse
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

tripdata_silver_df = (
  spark
    .readStream
    .format("delta")
    .load(f"abfss://lake@{lake_name}/silver/taxidemo/tripdata")
)

# COMMAND ----------

(
  tripdata_silver_df.writeStream 
    .format("com.databricks.spark.sqldw") 
    .option("url", synapse_jdbc_url) 
    .option("tempDir", f"wasbs://connector@{blob_name}/temp") 
    .option("forwardSparkAzureStorageCredentials", "true") 
    .option("dbTable", "dbo.tripdata") 
    .option("checkpointLocation", f"abfss://lake@{lake_name}/silver/taxidemo/tripdata/synapse.checkpoint") 
     .trigger(processingTime='1 minute')
    .start()
)
