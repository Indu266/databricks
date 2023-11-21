# Databricks notebook source
df=spark.read.option("multiline",True).json("dbfs:/FileStore/tables/formula1/pit_stops.json")

# COMMAND ----------

df.write.saveAsTable("formula1.pitstop")

# COMMAND ----------


