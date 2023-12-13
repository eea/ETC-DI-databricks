# Databricks notebook source
# MAGIC %md
# MAGIC # Quality check of JEDI dimensions
# MAGIC ## 

# COMMAND ----------

# MAGIC %md
# MAGIC Errors can occur at every step of a process, which also applies to the ingestion process. Therefore, it is extremely important to check after each ingestion whether it was performed correctly. 
# MAGIC The implementation of data validation for ingestion involves a series of steps to ensure the quality and integrity of the ingested datasets. Here is an overview of the key aspects of the implementation:
# MAGIC
# MAGIC 1. Feature List Generation: The first step is to define a comprehensive list of features that need to be validated for each ingested dataset. This list encompasses various aspects such as data completeness, correctness, consistency, and conformity to predefined standards. It serves as a reference for the subsequent validation process.
# MAGIC
# MAGIC 2. Descriptive Statistics Calculation: Once the dataset is ingested, descriptive statistics are computed for each feature. These statistics capture important characteristics of the data, including measures such as mean, standard deviation, minimum, maximum, and other relevant statistical indicators. These statistics provide valuable insights into the distribution and properties of the data.
# MAGIC
# MAGIC 3. Anomaly detection: The computed descriptive statistics are then compared to the statistics derived from previously ingested datasets. A detection process is applied to identify any significant deviations or anomalies in the newly ingested data. This process uses the reference statistics of acceptable data as a baseline for detecting inconsistencies or unexpected patterns.
# MAGIC
# MAGIC 4. Error Labelling and Data Incorporation: Based on the results of above, the ingested data is labelled either as acceptable or erroneous. If significant discrepancies are detected, the data is flagged as erroneous, indicating the presence of potential data quality issues. However, if no discrepancies are found, the data is labelled as acceptable, and its descriptive statistics are incorporated into the ensemble of reference statistics for future comparisons.
# MAGIC
# MAGIC 5. Spatial Validation: In addition to the feature-based validation, spatial validation is performed to ensure the spatial integrity of the ingested data. This involves verifying that the spatial attributes have been preserved accurately during the ingestion process. It also includes checks for prop
# MAGIC
# MAGIC er re-projection, if applicable, to ensure the spatial relationships and alignments are maintained correctly.
# MAGIC
# MAGIC 6. Reporting and Logging: Throughout the validation process, comprehensive reports and logs are generated to document the validation results. These reports provide detailed information about the validation outcomes, including any detected errors or anomalies. This documentation facilitates further analysis, investigation, and troubleshooting of data quality issues. This information should be part of the meta-data catalogue of each dataset.
# MAGIC
# MAGIC Methodology: ------------------
# MAGIC
# MAGIC The proposed method consists of two steps: first, a list of features to be checked after ingesting the dataset; secondly, an automatic anomaly detection method to identify deviations from previously ingested data and by means of feature vector validation This approach does not require domain experts to define data quality constraints or provide valid examples.
# MAGIC
# MAGIC The proposed method for data ingestion validation and anomaly detection consists of three key steps:
# MAGIC
# MAGIC · Step 1: List of features is generated, specifying the aspects that need to be examined after ingesting the dataset.
# MAGIC
# MAGIC · Step 2: Anomaly detection method to identify any deviations or anomalies in the newly ingested data.
# MAGIC
# MAGIC By implementing this method, data quality assessment becomes more efficient and less reliant on manual intervention. The automated nature of the approach allows for consistent and reliable anomaly detection, even without extensive domain expertise or predefined quality constraints.
# MAGIC
# MAGIC
# MAGIC
# MAGIC |     Feature                               |     Description                                                                                                                                                                                  |   |   |   |
# MAGIC |-------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|---|---|
# MAGIC |     Duplicates                            |     Check for duplicate   entries                                                                                                                                                                |   |   |   |
# MAGIC |     Date overlap                          |     Check for overlaps   in the date column, i.e., repeated dates with different values (only for   timeseries)                                                                                  |   |   |   |
# MAGIC |     Date gaps                             |     Check for missing   dates between start and end date (only for timeseries)                                                                                                                   |   |   |   |
# MAGIC |     No data values                        |     Verification of the   correct use of no data                                                                                                                                                 |   |   |   |
# MAGIC |     Value types                           |     Check if data types   are correct (string, integer, float, datetime format)                                                                                                                  |   |   |   |
# MAGIC |     Value encoding                        |     Check if the encoding of the data is correct (e.g.,   character encoding is utf-8; point (.) is used as decimal separator)     Check the special   characters in the different languages.    |   |   |   |
# MAGIC |     Completeness                          |     The ratio of   not-NULL values                                                                                                                                                               |   |   |   |
# MAGIC |     Count of distinctive   values         |     Number of distinct   values in the dataset                                                                                                                                                   |   |   |   |
# MAGIC |     Ratio of the most   frequent value    |     Number of   occurrences for the most frequently repeated value, normalized by the batch   size                                                                                               |   |   |   |
# MAGIC |     Maximum                               |     Maximum value of the   dataset                                                                                                                                                               |   |   |   |
# MAGIC |     Mean                                  |     Mean value of the   dataset                                                                                                                                                                  |   |   |   |
# MAGIC |     Minimum                               |     Minimum value of the   dataset                                                                                                                                                               |   |   |   |
# MAGIC |     Standard deviation                    |     Standard deviation   of the dataset                                                                                                                                                          |   |   |   |
# MAGIC |     Number of records                     |     Number of rows in   the dataset (e.g., number of polygons)                                                                                                                                   |   |   |   |
# MAGIC |     Date range                            |     Start and end date   (only for timeseries)                                                                                                                                                   |   |   |   |
# MAGIC |     Grid boundaries                       |     Top-left and bottom-right coordinates (only for   gridded datasets)     (GridNum or X,Y   values of the dimension)                                                                           |   |   |   |
# MAGIC |     Data completeness                     |      Check to verify that the data set is   complete (total area, total number of features or pixel)                                                                                             |   |   |   |
# MAGIC |                                           |                                                                                                                                                                                                  |   |   |   |
# MAGIC |     Pixel size                            |     Check to verify that   the pixel size is correct – bases on AreaHa value                                                                                                                     |   |   |   |
# MAGIC |     Number of attributes                  |     Verification that   all attributes of the table or vector dataset have been transmitted   correctly. – compare the metadata on JEDI with the dimension on Azure                              |   |   |   |
# MAGIC |                                           |                                                                                                                                                                                                  |   |   |   |

# COMMAND ----------

# MAGIC %md
# MAGIC ## (1) Example: Checking Census grid 2021 dimension 

# COMMAND ----------

# MAGIC %md
# MAGIC Dimension INFO:
# MAGIC https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1967&fileId=992
# MAGIC
# MAGIC ESMS Census Grid 2021 - Population grid 1km²
# MAGIC
# MAGIC cwsblobstorage01/cwsblob01/Dimensions/D_Pop_census_2021_992_2023510_1km
# MAGIC
# MAGIC

# COMMAND ----------

# importing required python modules
import pandas as pd
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC  ###(1.2) Reading the dimension
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC  ####(1.2.1) Reading the dimension and converting to pandas

# COMMAND ----------

# reading the dimension and saving to a spark dataframe; just replace dimension folder name below to read from a different dimension
dim_folder = "D_Pop_census_2021_992_2023510_1km"
df_spark = spark.read.format("delta").load(f"dbfs:/mnt/trainingDatabricks/Dimensions/{dim_folder}/")
# converting the spark dataframe to a pandas dataframe
df = df_spark.select("*").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC  ####(1.2.2) Reading the dimension and converting to scala

# COMMAND ----------

# MAGIC %scala
# MAGIC //##########################################################################################################################################
# MAGIC //   THIS BOX reads all Dimensions (DIM) and Lookuptables (LUT) 
# MAGIC
# MAGIC ////  !!!!!!!!!!!!!!!!!!!!!!!!
# MAGIC
# MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
# MAGIC import spark.sqlContext.implicits._ 
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //// (0) ESMS Census Grid 2021 - Population grid 1km² ################################################################################
# MAGIC // Reading the admin DIM:---------------------------------------------
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1967&fileId=992
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_Pop_census_2021_992_2023510_1km
# MAGIC val parquetFileDF_Population  = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_Pop_census_2021_992_2023510_1km/")             /// use load
# MAGIC parquetFileDF_Population.createOrReplaceTempView("Population")

# COMMAND ----------

# MAGIC %md
# MAGIC ###(1.3)  Overview of dimension

# COMMAND ----------

print(f"Current dimension has {df.shape[0]} rows and {df.shape[1]} columns")
print("These are the column names:")
for col in df.columns:
    print(col)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- this function described the selected dimension table:
# MAGIC DESCRIBE TABLE Population

# COMMAND ----------

# MAGIC %md
# MAGIC  ###(1.4) Explore the data 

# COMMAND ----------

# 5 sample rows
df.sample(5)

# COMMAND ----------

# 10 top rows
df.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC  ###(1.5) Overall statistics

# COMMAND ----------

pd.set_option('display.float_format', lambda x: '%.3f' % x) # setting the format for the output
df.describe(percentiles=[.1, .25, .5, .75, .9])

# COMMAND ----------

#checking for NaN, replace field to check name to check other fields
field_to_check = 'ESTAT_OBSVALUET_2021_V10'
df[field_to_check].isnull().values.any()

# COMMAND ----------

# MAGIC %md
# MAGIC  ###(1.6) Histogram of dimension figures

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
# MAGIC  ###(1.7)  Verifying figures for a known area (Luxembourg)

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
