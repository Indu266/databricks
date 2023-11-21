# Databricks notebook source
df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/formula1/circuits.csv")

# COMMAND ----------

df.select("circuitId","circuitRef","name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitId"),"circuitRef",df.name,df["location"]).display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.select(concat("location","country").alias("new column")).display()

# COMMAND ----------

df.select("*", concat("location",lit(" "),"country").alias("new column")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df.columns

# COMMAND ----------

newcolumns=['circuit_id',
 'circuit_ref',
 'firstname',
 'location',
 'country',
 'lat',
 'lng',
 'alt',
 'url']

# COMMAND ----------

df.toDF(newcolumns)

# COMMAND ----------

df.toDF(*newcolumns).display()

# COMMAND ----------

df.withColumn("formula1",lit("Formula1Data")).display()

# COMMAND ----------

df.withColumn("ingestiondate",current_date()).display()

# COMMAND ----------

df.where("circuitID=1").display()

# COMMAND ----------

df.filter(col("circuitID")==1).display()

# COMMAND ----------

df.filter("circuitID > 10 and country ='UK'").display()

# COMMAND ----------

df.filter((col("circuitID") > 10) & (col("country") =='UK')).display()

# COMMAND ----------

df.orderBy(col("circuitID").desc()).display()

# COMMAND ----------

df.orderBy(desc("circuitID")).display()

# COMMAND ----------

df.sort("country").select("country","location").display()

# COMMAND ----------

df.sort("country","location").select("country","location").display()

# COMMAND ----------

df.drop("url","alt").display()

# COMMAND ----------


