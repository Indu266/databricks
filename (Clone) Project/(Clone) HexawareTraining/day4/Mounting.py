# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://<containername>@<storageaccount>.blob.core.windows.net",
  mount_point = "/mnt/<storageaccount>/<containername>",
  extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://testblobcontainer@blobadhex.blob.core.windows.net",
  mount_point = "/mnt/blobadhex/testblobcontainer",
  extra_configs = {"fs.azure.account.key.blobadhex.blob.core.windows.net":"q5I8uK1/Z4PL9+1OsIPWik5gHWWliO8M4RO7iVIYcf2uE+sJCXKTNd5tdcPa/U99JDmpkT7Zbphl+AStgc+3LQ=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/blobadhex/testblobcontainer/

# COMMAND ----------


