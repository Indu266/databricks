# Databricks notebook source
from delta.tables import*

# COMMAND ----------

DeltaTable.createOrReplace(spark)\
    .tableName("indudelta.employee")\
    .addColumn("emp_id","int")\
    .addColumn("emp_name","String")\
    .addColumn("gender","String")\
    .location("dbfs:/mnt/asadlsad/processeddata/delta/emp")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indudelta.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into indudelta.employee values(1,"Sachin","M")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indudelta.employee

# COMMAND ----------

from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %python
# MAGIC data=[(2,'Rohit',"M")]
# MAGIC schema=StructType([StructField("emp_id",IntegerType()),
# MAGIC                    StructField("emp_name",StringType()),
# MAGIC                    StructField("gender",StringType()),
# MAGIC ])
# MAGIC (spark.createDataFrame(data,schema).write.mode("append").saveAsTable("indudelta.employee"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indudelta.employee

# COMMAND ----------

df=spark.read.table("indudelta.employee")
df.dropDuplicates("emp_id")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %python
# MAGIC data=[(3,'virat',"M","Batsman")]
# MAGIC schema=StructType([StructField("emp_id",IntegerType()),
# MAGIC                    StructField("emp_name",StringType()),
# MAGIC                    StructField("gender",StringType()),
# MAGIC                    StructField("dept",StringType())
# MAGIC ])
# MAGIC (spark.createDataFrame(data,schema).write.mode("append").option("mergeSchema","true").saveAsTable("indudelta.employee"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from indudelta.employee

# COMMAND ----------


