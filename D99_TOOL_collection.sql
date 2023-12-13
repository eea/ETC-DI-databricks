-- Databricks notebook source
-- MAGIC %md # HELP
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ## 1.1 reading csv
-- MAGIC
-- MAGIC

-- COMMAND ----------

val test  = spark.read.format("csv")
  .options(Map("delimiter"->"|"))
  .option("header", "true")
  .load("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube/part-00000-tid-3349834704023963942-253eb619-be31-4616-b126-644c20d1a0e2-8725-1-c000.csv.gz")
test.createOrReplaceTempView("test")

-- COMMAND ----------

-- MAGIC %md ## 1.2 JOIN..of different GRID-sizes

-- COMMAND ----------

select 
gridnum & cast(-65536 as bigint) as GridNum100m,
gridnum & cast(- 4294967296 as bigint) as GridNum10km,

if (SUM(Areaha)> 0.5,  1, 0 ) as Majority_protected_by_N2k_net_100m,
Natura2000_net,
SUM(AreaHa) as AreaHa
from n2k_net_10m_v2021

where gridnum & cast(- 4294967296 as bigint) =18587398686375936  ---- somewhere in Luxembourg
group by 
gridnum & cast(-65536 as bigint) 
,Natura2000_net, 
gridnum & cast(- 4294967296 as bigint)
