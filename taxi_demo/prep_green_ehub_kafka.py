# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ##prep_green_ehub_kafka notebook
# MAGIC <br />
# MAGIC - Creates event hub messages from source Green Taxi cab trips

# COMMAND ----------

# MAGIC %run ./setup_config

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Pull the first month of Green Taxi CSV source files and put them into EventHubs using Kafka API

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/green/")

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.avro.functions import to_avro, from_avro

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

eh_green_kafka_df = green_2019_df.select(
  to_avro(struct(col("lpep_pickup_datetime"))).alias("key"), 
  to_avro(struct(col("*"))).alias("value")
)

# COMMAND ----------

display(eh_green_kafka_df)

# COMMAND ----------

test_read_kafka_df = eh_green_kafka_df.select(from_avro(col("value"),green_avro_schema).alias("value"))
display(test_read_kafka_df)

# COMMAND ----------

 (
   eh_green_kafka_df
    .write
    .format("kafka") 
    .option("topic", kafka_topic)
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.security.protocol", "SASL_SSL") 
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) 
    .option("kafka.session.timeout.ms", "60000") 
    .option("kafka.request.timeout.ms", "30000") 
    .option("kafka.group.id", "$Default") 
    .option("kafka.batch.size", 5000) 
    .option("failOnDataLoss", "false") 
    .save()
)