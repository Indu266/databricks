# Databricks notebook source
spark

# COMMAND ----------

users=[(1,'a',30),(2,'b',32)]
sampledf=spark.createDataFrame(users)

# COMMAND ----------

sampledf.show()

# COMMAND ----------

display(sampledf)

# COMMAND ----------

users_schema_str="id int, name string, age int"

# COMMAND ----------

df1=spark.createDataFrame(users,users_schema_str)

# COMMAND ----------

df1.display()

# COMMAND ----------


