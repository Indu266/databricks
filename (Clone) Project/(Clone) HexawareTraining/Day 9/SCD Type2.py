# Databricks notebook source
employeesrc = [(1, "Scott", "Tiger", 1000.0,
                      "united states"
                     )]
df = spark. \
    createDataFrame(employeesrc,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employeetrgt(
# MAGIC   employee_id int,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   salary int,
# MAGIC   nationality string,
# MAGIC   startdate date,
# MAGIC   endate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into employeetrgt a
# MAGIC using
# MAGIC (
# MAGIC   select employee_id as mergeKey,* from source_view
# MAGIC   union all
# MAGIC   select NULL as mergeKey,a.*from source_view a join employeetrgt b on a.employee_id =b.employee_id
# MAGIC )
# MAGIC b
# MAGIC on a.employee_id =b.mergeKey
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC   a.endate=current_date()-1
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,
# MAGIC     startdate,
# MAGIC     endate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,current_date(),'9999-12-31'
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employeetrgt

# COMMAND ----------

employeesrc = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Clair",2000.0,"UK")]
df = spark. \
    createDataFrame(employeesrc,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )

# COMMAND ----------

employeesrc = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Sena",2000.0,"UK"),
             (3,"priya","Singh",3000.0,"USA"),
             (4,"Arjun","Sharma",4000.0,"India"),
             (5,"Anu","Agarwal",5000.0,"Canada"),
             (6,"Supriya","Kumar",6000.0,"Australia")]
df = spark. \
    createDataFrame(employeesrc,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employeetrgt

# COMMAND ----------


