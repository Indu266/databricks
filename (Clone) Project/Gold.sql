-- Databricks notebook source
use project;

-- COMMAND ----------

create Or replace table project.gold as (select product_name, sum(quantity) as totalquantity from silver group by all order by totalquantity desc)

-- COMMAND ----------

select * from gold

-- COMMAND ----------


