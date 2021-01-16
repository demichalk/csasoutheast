# Databricks notebook source
# MAGIC %md 
# MAGIC ## multiple display() calls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/taxizone

# COMMAND ----------

pay_type = spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")

# COMMAND ----------

rate_code = spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")

# COMMAND ----------

zone_lookup = spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv")

# COMMAND ----------

display(pay_type)
display(rate_code)
display(zone_lookup)