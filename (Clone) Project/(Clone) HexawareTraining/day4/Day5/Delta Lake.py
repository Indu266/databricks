# Databricks notebook source
https://docs.delta.io/latest/delta-batch.html#create-a-table

# COMMAND ----------

ways to create a delta table
sql,python,df

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema indudelta;
# MAGIC use indudelta

# COMMAND ----------

# MAGIC %sql
# MAGIC use indudelta;
# MAGIC CREATE TABLE IF NOT EXISTS indudelta.people10m (  id INT,  firstName STRING,  middleName STRING,  lastName STRING,  gender STRING,  birthDate TIMESTAMP,  ssn STRING,  salary INT) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended indudelta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history indudelta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS indudelta.people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) LOCATION 'dbfs:/mnt/inadlshexaware/processeddata/delta/indu'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended indudelta.people20m

# COMMAND ----------


