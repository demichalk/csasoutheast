# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC REPLACE TABLE demo2020.zones2
# MAGIC DEEP CLONE demo2020.zones

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from demo2020.zones2

# COMMAND ----------

# MAGIC %sql select * from demo2020.zones2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM demo2020.zones2 where Borough='Brooklyn'

# COMMAND ----------

