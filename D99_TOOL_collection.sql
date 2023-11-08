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
