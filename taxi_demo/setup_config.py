# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Taxi Demo
# MAGIC ##setup_config notebook
# MAGIC 
# MAGIC ###Requirements
# MAGIC - Blob Storage account with container named "ingest" along with key, connection string, resource group name, and subscription id
# MAGIC - ADLS Gen 2 Storage account with a container named "lake"
# MAGIC - Service Principal with client id, client secret, and Azure AD tenant_id with access to ADLS Gen 2 
# MAGIC - Setup the secrets using a linked Azure Key Vault secret scope: https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
# MAGIC 
# MAGIC ###Results
# MAGIC 
# MAGIC - Variables created using the notebook are imported by %run to bring them into the other notebook scopes
# MAGIC - All Spark security config settings to access blob and lake in demos are setup here
# MAGIC - Create the taxidemos database if it doesnt exist and default to it

# COMMAND ----------

blob_name = "fieldengdeveastus2sa.blob.core.windows.net"
blob_key = dbutils.secrets.get("taxi-demo-scope","taxi-blob-key") 
blob_connection = dbutils.secrets.get("taxi-demo-scope","taxi-blob-connection")   
client_id = "1bcf0495-6572-4f0d-a35b-8a9c785836b9"
client_secret = dbutils.secrets.get("taxi-demo-scope","taxi-sp-key")  
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc"
lake_name = "fieldengdeveastus2adls.dfs.core.windows.net"
resource_group_name = "field-eng-dev-eastus2-rg"
subscription_id = "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"
eh_connection_string = dbutils.secrets.get("taxi-demo-scope","taxi-eh-connection")
eh_connection_encrypted = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_connection_string)
synapse_jdbc_url = dbutils.secrets.get("taxi-demo-scope","taxi-synapse-jdbc-url") 

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{blob_name}", blob_key)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{lake_name}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{lake_name}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{lake_name}", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{lake_name}", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{lake_name}", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidemo;
# MAGIC USE taxidemo;

# COMMAND ----------

spark.sql("USE taxidemo")