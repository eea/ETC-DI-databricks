# Databricks notebook source
# MAGIC %md
# MAGIC # Quality check of JEDI dimensions
# MAGIC ## Example: Checking Census grid 2021 dimension 

# COMMAND ----------

# importing required python modules
import pandas as pd
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading the dimension and converting to pandas

# COMMAND ----------

# reading the dimension and saving to a spark dataframe; just replace dimension folder name below to read from a different dimension
dim_folder = "D_Pop_census_2021_992_2023510_1km"
df_spark = spark.read.format("delta").load(f"dbfs:/mnt/trainingDatabricks/Dimensions/{dim_folder}/")
# converting the spark dataframe to a pandas dataframe
df = df_spark.select("*").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overview of dimension

# COMMAND ----------

print(f"Current dimension has {df.shape[0]} rows and {df.shape[1]} columns")
print("These are the column names:")
for col in df.columns:
    print(col)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Explore the data 

# COMMAND ----------

# 5 sample rows
df.sample(5)

# COMMAND ----------

# 10 top rows
df.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Overall statistics

# COMMAND ----------

pd.set_option('display.float_format', lambda x: '%.3f' % x) # setting the format for the output
df.describe(percentiles=[.1, .25, .5, .75, .9])

# COMMAND ----------

#checking for NaN, replace field to check name to check other fields
field_to_check = 'ESTAT_OBSVALUET_2021_V10'
df[field_to_check].isnull().values.any()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Histogram of dimension figures

# COMMAND ----------

df['ESTAT_OBSVALUET_2021_V10'] = df['ESTAT_OBSVALUET_2021_V10'].astype(float)
pop_figs = df['ESTAT_OBSVALUET_2021_V10'].loc[df['ESTAT_OBSVALUET_2021_V10'] > 0]
histo = px.histogram(pop_figs,
                     x="ESTAT_OBSVALUET_2021_V10",
                     nbins=1000,
                     log_y=True)
histo.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verifying figures for a known area (Luxembourg)

# COMMAND ----------

# Let's check if population figures for Luxembourg make sense
# read table with grid cells for Luxembourg
df_spark = spark.read.format("delta").load(f"dbfs:/user/hive/warehouse/databricks_1_km_and_10_k_grids_for_lu/")
# converting the spark dataframe to a pandas dataframe
df_lux = df_spark.select("*").toPandas()

# COMMAND ----------

# quick check if import was OK
df_lux.head()

# COMMAND ----------

# join dimension info to the lux dataframe
#
df.rename(columns = {'gridnum':'GridNum'}, inplace = True)
merged_lux = pd.merge(df_lux, df, on='GridNum')
merged_lux.head()

# COMMAND ----------

# calculation of total population from the census grid
total_pop_lux = int(merged_lux['ESTAT_OBSVALUET_2021_V10'].sum())
print(f'The total population of Luxembourg from Eurostat Census Grid 2021 data is {total_pop_lux}\nThe official figure from the World Bank and same year was 640064')
