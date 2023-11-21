-- Databricks notebook source
-- MAGIC %sql
-- MAGIC create table tableusingcopyinto

-- COMMAND ----------

COPY INTO tableusingcopyinto
FROM 'dbfs:/mnt/asadlsad/processeddata/inputstream/csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

select * from tableusingcopyinto

-- COMMAND ----------


