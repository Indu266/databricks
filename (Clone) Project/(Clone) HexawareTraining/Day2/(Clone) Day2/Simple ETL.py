# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/circuits.csv

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/formula1/circuits.csv")

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1;
# MAGIC use formula1

# COMMAND ----------

df.write.saveAsTable("formula1.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table formula1.circuits

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)
df.write.mode("overwrite").saveAsTable("formula1.circuits")

# COMMAND ----------

spark.sql("select * from formula1.circuits where location='Melbourne'").display()

# COMMAND ----------


