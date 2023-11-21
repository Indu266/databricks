# Databricks notebook source
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/indu/schemalocation")
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/indu/checkpoint")
.option("path",f"{outputpath}/indu/files")
.table("stream.firstauto")
)

# COMMAND ----------

inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"
outputpath="dbfs:/mnt/asadlsad/processeddata/outputautoloader"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table stream.firstauto

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/indu/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/indu/checkpoint")
.option("path",f"{outputpath}/indu/files")
.option("mergeSchema",True)
.table("stream.firstauto")
)

# COMMAND ----------


