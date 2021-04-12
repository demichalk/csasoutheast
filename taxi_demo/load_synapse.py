# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ### load_synapse
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Directly stream rows from TripData Silver Delta Lake into dbo.tripdata table in Synapse using Synapse Connector streaming API

# COMMAND ----------


(
   spark.readStream
      .option("maxFilesPerTrigger",1)
      .table("tripdata_silver")
    .writeStream 
      .format("com.databricks.spark.sqldw") 
      .option("url", synapse_jdbc_url) 
      .option("tempDir", f"wasbs://connector@{blob_name}/temp") 
      .option("forwardSparkAzureStorageCredentials", "true") 
      .option("dbTable", "dbo.tripdata") 
      .option("checkpointLocation", f"abfss://lake@{lake_name}/silver/taxidemo/tripdata/syn_tripdata.checkpoint") 
      .trigger(processingTime="30 seconds")
      .start()
)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Process TripData Gold Delta Lake table stream by using foreachBatch and the Synapse Batch API to overwrite Synapse dbo.tripdata_summary table of aggregations with each microbatch.  The streaming support in the Synapse connector doesn't give fine grain control with preActions/postActions

# COMMAND ----------

def tripdata_gold_batch(df, batchId):
  ( 
    df.write
    .format("com.databricks.spark.sqldw") 
    .option("url", synapse_jdbc_url) 
    .option("tempDir", f"wasbs://connector@{blob_name}/temp") 
    .option("forwardSparkAzureStorageCredentials", "true") 
    .option("dbTable", "dbo.tripdata_summary") 
    .option("preActions", "DELETE FROM dbo.tripdata_summary")
    .mode("overwrite")
    .save()
  )

# COMMAND ----------


(
  spark.readStream.table("tripdata_gold")
    .writeStream 
    .option("checkpointLocation", f"abfss://lake@{lake_name}/gold/taxidemo/tripdata/syn_tripdata_summary.checkpoint") 
    .foreachBatch(tripdata_gold_batch)
    .trigger(processingTime="1 minute")
    .start()
)