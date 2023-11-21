# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("forename",col("name.forename")).display()

# COMMAND ----------

df.withColumn("surname",col("name.surname")).display()

# COMMAND ----------

df.drop("name","url").display()

# COMMAND ----------

df.write.saveAsTable("formula1.drivers")

# COMMAND ----------


