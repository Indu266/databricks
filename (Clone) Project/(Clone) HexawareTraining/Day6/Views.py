# Databricks notebook source
# MAGIC %sql
# MAGIC View:virtual table
# MAGIC 1.Standard view(sql)--persisted view--can find view inside the catalog.
# MAGIC 2.temp view(sql,python)--if cluster terminated then view will be terminated in notebook
# MAGIC 3.Global view(sql,python)--can be used anywhere

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC use iotdata;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample where temperature> 25

# COMMAND ----------

# MAGIC %sql
# MAGIC create view tempabove25 as (select * from sample where temperature>25)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# MAGIC %sql
# MAGIC create view testunique as(select * from sample where test ==111)

# COMMAND ----------

# MAGIC %sql
# MAGIC Temp view can be created by using temp keyword

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view testunique as(select * from sample where test ==111)

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/inadlshexaware/raw/circuits.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC df.createOrReplaceTempView("namestemp")

# COMMAND ----------

# MAGIC %python
# MAGIC df.createOrReplaceGlobalTempView("namesglobal")

# COMMAND ----------

show views in global_temp

# COMMAND ----------

select * from global_temp.namesglobal
