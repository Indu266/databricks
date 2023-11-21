# Databricks notebook source
df=spark.read.option("header",True).json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("ingestiondate",current_date()).display()

# COMMAND ----------

df.withColumn("filepath",col(url)).display()

# COMMAND ----------

df.withColumn("filepath",input_file_name()).display()

# COMMAND ----------

df.drop("url").display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("formula1.constructors")

# COMMAND ----------


