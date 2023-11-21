# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/iotdata/

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.inadlshexaware.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.inadlshexaware.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.inadlshexaware.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=c&sp=rlx&se=2023-11-14T02:02:35Z&st=2023-11-13T18:02:35Z&spr=https&sig=bKCRrnv5%2BD9XHBQLAPM5w3hOvdUNGC72Pj%2FUNwpCxNE%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@inadlshexaware.dfs.core.windows.net/fact")

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsad/processeddata")

# COMMAND ----------

# MAGIC %fs ls /mnt/asadlsad/

# COMMAND ----------


