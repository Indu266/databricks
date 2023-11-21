# Databricks notebook source
# MAGIC %sql 
# MAGIC create schema if not exists iotdata;
# MAGIC use iotdata

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table iotdata.sample as
# MAGIC (select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/asadlsad/processeddata/iotdata/indu`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`

# COMMAND ----------


