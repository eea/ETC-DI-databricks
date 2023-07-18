# Databricks notebook source
# MAGIC %md # Coperniucs data validation
# MAGIC

# COMMAND ----------

# MAGIC %md ## (1) Read libaries

# COMMAND ----------

# MAGIC %python
# MAGIC import pyspark
# MAGIC
# MAGIC from delta import *
# MAGIC # Required for StructField, StringType, IntegerType, etc.
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC import pyodbc 
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC import matplotlib.pyplot as plt
# MAGIC #import sqlalchemy as sa
# MAGIC #from sqlalchemy import create_engine, event
# MAGIC #from sqlalchemy.engine.url import URL
# MAGIC import json
# MAGIC
# MAGIC from scipy import stats
# MAGIC
# MAGIC #check processing time:##
# MAGIC import time  
# MAGIC import datetime
# MAGIC start_time = time.time()
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
# MAGIC import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
# MAGIC import org.apache.spark.sql.{Row, SparkSession}
# MAGIC import spark.sqlContext.implicits._ 

# COMMAND ----------

# MAGIC %md ## (2) Read dimensions

# COMMAND ----------

# MAGIC %scala
# MAGIC //##########################################################################################################################################
# MAGIC //   THIS BOX reads all Dimensions (DIM) and Lookuptables (LUT) that are needed for the NUTS3 and 10x10km GRID statistics
# MAGIC //   info: loehnertz@space4environment.com
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //// FIRST start the cluster: ETC-ULS !!!!!!!!!!!!!!!!!!!!!!!!
# MAGIC
# MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
# MAGIC import spark.sqlContext.implicits._ 
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC  
# MAGIC         
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (2) Urban Atlas 2018    ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1552&fileId=577
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_ua2018_10m_july20_577_2020818_10m
# MAGIC //D_UA_06_12_18_693_2021224_10m
# MAGIC //val parquetFileDF_ua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ua2018_10m_july20_577_2020818_10m/")             /// use load
# MAGIC //parquetFileDF_ua2018.createOrReplaceTempView("ua2018")
# MAGIC
# MAGIC val parquetFileDF_ua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_UA_06_12_18_693_2021224_10m/")             /// use load
# MAGIC parquetFileDF_ua2018.createOrReplaceTempView("ua2018")
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (3) CLC pro Backbone (BB) 2018    ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //#cwsblobstorage01/cwsblob01/Dimensions/D_CLCplus2018bb_965_202329_10m
# MAGIC //#https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1940&fileId=965
# MAGIC
# MAGIC //#Value    Class_name
# MAGIC //#1	-	Sealed
# MAGIC //#2	-	Woody needle leaved trees
# MAGIC //#3	-	Woody Broadleaved deciduous trees
# MAGIC //#4	-	Woody Broadleaved evergreen trees
# MAGIC //#5	-	Low-growing woody plants
# MAGIC //#6	-	Permanent herbaceous
# MAGIC //#7	-	Periodically herbaceous
# MAGIC //#8	-	Lichens and mosses
# MAGIC //#9	-	Non and sparsely vegetated
# MAGIC //#10	-	Water
# MAGIC //#11	-	Snow and ice
# MAGIC //#254	-	Outside area
# MAGIC //#255	-	No data
# MAGIC
# MAGIC val parquetFileDF_clc_plus_bb = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_CLCplus2018bb_965_202329_10m/")             /// use load
# MAGIC parquetFileDF_clc_plus_bb.createOrReplaceTempView("clc_plus_bb_2018")
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (4) Functional urban areas based on Urban Atlas 2018   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //#https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1712&fileId=737
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_fua_based_on_UA2018_737_2021427_10m
# MAGIC
# MAGIC val parquetFileDF_fua = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_fua_based_on_UA2018_737_2021427_10m/")             /// use load
# MAGIC parquetFileDF_fua.createOrReplaceTempView("fua")
# MAGIC
# MAGIC //#LUT https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=91  cwsblobstorage01/cwsblob01/Lookups/FUA_urban_atlas/20210428134723.06.csv
# MAGIC
# MAGIC val schema_fua = StructType(Array(
# MAGIC     StructField("fua_category",IntegerType,true),
# MAGIC     StructField("fua_code",StringType,true),
# MAGIC     StructField("country",StringType,true),
# MAGIC     StructField("fua_name", StringType, true),
# MAGIC     StructField("fua_code_ua", StringType, true),
# MAGIC     StructField("version", StringType, true),
# MAGIC     StructField("area_ha", IntegerType, true)
# MAGIC   ))
# MAGIC
# MAGIC
# MAGIC val LUT_city  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_fua)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups//FUA_urban_atlas/20210428134723.06.csv")
# MAGIC LUT_city.createOrReplaceTempView("LUT_fua")
# MAGIC
# MAGIC
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and NUTS information:---------------------------------------------
# MAGIC val fua_ua = spark.sql(""" 
# MAGIC            
# MAGIC select 
# MAGIC fua.gridnum,
# MAGIC fua.fua_ua2018_10m,
# MAGIC fua.AreaHa,
# MAGIC fua.GridNum10km,
# MAGIC LUT_fua.fua_category,
# MAGIC LUT_fua.fua_code	,
# MAGIC LUT_fua.country	,
# MAGIC LUT_fua.fua_name	,
# MAGIC LUT_fua.fua_code_ua	
# MAGIC
# MAGIC FROM fua 
# MAGIC   LEFT JOIN LUT_fua  ON fua.fua_ua2018_10m = LUT_fua.fua_category  
# MAGIC   
# MAGIC        
# MAGIC  
# MAGIC                                   """)
# MAGIC
# MAGIC fua_ua.createOrReplaceTempView("fua_ua")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (5) Small Woody Features 2018 10m -SUBEST for Lux   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC ///cwsblobstorage01/cwsblob01/Dimensions/D_swf_2018_subset_lux_990_202352_10m
# MAGIC ///https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1965&fileId=990
# MAGIC val parquetFileDF_swf18_aoi = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_swf_2018_subset_lux_990_202352_10m/")             /// use load
# MAGIC parquetFileDF_swf18_aoi.createOrReplaceTempView("swf18_aoi")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (6) Tree Cover Density 2018   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC ///Dimensions/D_TreeCoverDens201810m_694_202134_10m/
# MAGIC ///https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1669&fileId=694
# MAGIC val parquetFileDF_treecover2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_TreeCoverDens201810m_694_202134_10m/")             /// use load
# MAGIC parquetFileDF_treecover2018.createOrReplaceTempView("treecover2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (7) Urban Atlas - Street Tree Layer 2018 (10m raster)  ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC ///https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1683&fileId=708
# MAGIC ///cwsblobstorage01/cwsblob01/Dimensions/D_StreetTreeUA2018_708_2021326_10m
# MAGIC
# MAGIC val parquetFileDF_streettree2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_StreetTreeUA2018_708_2021326_10m/")             /// use load
# MAGIC parquetFileDF_streettree2018.createOrReplaceTempView("street_tree_2018")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## (3) Build datacube for FUA Luxembourg and Metz
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fua_ua

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select 
# MAGIC fua_ua.gridnum,
# MAGIC fua_ua.AreaHa,
# MAGIC fua_ua.fua_code,
# MAGIC fua_ua.fua_name,
# MAGIC
# MAGIC clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 as clc_plus_code,
# MAGIC ua2018.Category2018 as ua2018_class_code,
# MAGIC
# MAGIC
# MAGIC swf18_aoi.SWF_2018_10m_subest_lux_0100 as swf_orig,  -- % of pixel 0/25/50/75/100
# MAGIC street_tree_2018.Category as street_tree_orig,
# MAGIC treecover2018.Category as tree_cover_orig,
# MAGIC
# MAGIC IF(SWF_2018_10m_subest_lux_0100 >0 , 1,0)as swf_cover_1,
# MAGIC IF(street_tree_2018.Category >0 , 10,0) as street_tree_cover_10,
# MAGIC IF(treecover2018.Category>0 , 100,0) as tree_cover_100
# MAGIC
# MAGIC
# MAGIC from fua_ua 
# MAGIC
# MAGIC left JOIN swf18_aoi ON fua_ua.gridnum = swf18_aoi.gridnum  
# MAGIC left JOIN street_tree_2018 ON fua_ua.gridnum = street_tree_2018.gridnum  
# MAGIC left JOIN treecover2018 ON fua_ua.gridnum = treecover2018.gridnum  
# MAGIC left JOIN clc_plus_bb_2018 ON fua_ua.gridnum = clc_plus_bb_2018.gridnum  
# MAGIC left JOIN ua2018 ON fua_ua.gridnum = ua2018.gridnum  
# MAGIC
# MAGIC
# MAGIC ----where fua_ua.fua_code = 'LU001' or fua_ua.fua_code = 'FR017' or fua_ua.fua_code = 'DE026' --- Luxembourg and Metz or Trier
# MAGIC where fua_ua.fua_code in ('BE004',	'BE005',	'BE007',	'BE009',	'CH003',	'DE026',	'DE034',	'DE040',	'DE042',	'DE044',	'DE507',	'FR006',	'FR009',	'FR016',	'FR017',	'FR018',	'FR034',	'FR040',	'FR051',	'FR076',	'FR079',	'FR104',	'FR207',	'FR208',	'FR209',	'FR505',	'FR506',	'LU001',	'NL010',	'NL505')
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val FUA_CUBE_lux_metz = spark.sql("""
# MAGIC Select 
# MAGIC fua_ua.gridnum,
# MAGIC fua_ua.AreaHa,
# MAGIC fua_ua.fua_code,
# MAGIC fua_ua.fua_name,
# MAGIC
# MAGIC swf18_aoi.SWF_2018_10m_subest_lux_0100 as swf_orig,  -- % of pixel 0/25/50/75/100
# MAGIC street_tree_2018.Category as street_tree_orig,
# MAGIC treecover2018.Category as tree_cover_orig,
# MAGIC
# MAGIC IF(SWF_2018_10m_subest_lux_0100 >0 , 1,0)as swf_cover_1,
# MAGIC IF(street_tree_2018.Category >0 , 10,0) as street_tree_cover_10,
# MAGIC IF(treecover2018.Category>0 , 100,0) as tree_cover_100
# MAGIC
# MAGIC
# MAGIC from fua_ua 
# MAGIC
# MAGIC left JOIN swf18_aoi ON fua_ua.gridnum = swf18_aoi.gridnum  
# MAGIC left JOIN street_tree_2018 ON fua_ua.gridnum = street_tree_2018.gridnum  
# MAGIC left JOIN treecover2018 ON fua_ua.gridnum = treecover2018.gridnum  
# MAGIC
# MAGIC where fua_ua.fua_code = 'LU001' or fua_ua.fua_code = 'FR017' --- Luxembourg and Metz
# MAGIC
# MAGIC           
# MAGIC             """)
# MAGIC
# MAGIC FUA_CUBE_lux_metz.createOrReplaceTempView("FUA_CUBE_lux_metz")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val FUA_CUBE_subset = spark.sql("""
# MAGIC
# MAGIC Select 
# MAGIC fua_ua.gridnum,
# MAGIC fua_ua.AreaHa,
# MAGIC fua_ua.fua_code,
# MAGIC fua_ua.fua_name,
# MAGIC
# MAGIC clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 as clc_plus_code,
# MAGIC ua2018.Category2018 as ua2018_class_code,
# MAGIC
# MAGIC
# MAGIC swf18_aoi.SWF_2018_10m_subest_lux_0100 as swf_orig,  -- % of pixel 0/25/50/75/100
# MAGIC street_tree_2018.Category as street_tree_orig,
# MAGIC treecover2018.Category as tree_cover_orig,
# MAGIC
# MAGIC IF(SWF_2018_10m_subest_lux_0100 >0 , 1,0)as swf_cover_1,
# MAGIC IF(street_tree_2018.Category >0 , 10,0) as street_tree_cover_10,
# MAGIC IF(treecover2018.Category>0 , 100,0) as tree_cover_100
# MAGIC
# MAGIC
# MAGIC from fua_ua 
# MAGIC
# MAGIC left JOIN swf18_aoi ON fua_ua.gridnum = swf18_aoi.gridnum  
# MAGIC left JOIN street_tree_2018 ON fua_ua.gridnum = street_tree_2018.gridnum  
# MAGIC left JOIN treecover2018 ON fua_ua.gridnum = treecover2018.gridnum  
# MAGIC left JOIN clc_plus_bb_2018 ON fua_ua.gridnum = clc_plus_bb_2018.gridnum  
# MAGIC left JOIN ua2018 ON fua_ua.gridnum = ua2018.gridnum  
# MAGIC
# MAGIC where fua_ua.fua_code in ('BE004',	'BE005',	'BE007',	'BE009',	'CH003',	'DE026',	'DE034',	'DE040',	'DE042',	'DE044',	'DE507',	'FR006',	'FR009',	'FR016',	'FR017',	'FR018',	'FR034',	'FR040',	'FR051',	'FR076',	'FR079',	'FR104',	'FR207',	'FR208',	'FR209',	'FR505',	'FR506',	'LU001',	'NL010',	'NL505')
# MAGIC
# MAGIC ----where fua_ua.fua_code = 'LU001' or fua_ua.fua_code = 'FR017' or fua_ua.fua_code = 'DE026' --- Luxembourg and Metz or Trier
# MAGIC
# MAGIC
# MAGIC           
# MAGIC             """)
# MAGIC
# MAGIC FUA_CUBE_subset.createOrReplaceTempView("FUA_CUBE_subset")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC val FUA_CUBE_subset_stat = spark.sql("""
# MAGIC
# MAGIC Select  fua_code, 
# MAGIC         fua_name,
# MAGIC         clc_plus_code,
# MAGIC         ua2018_class_code,
# MAGIC         swf_cover_1,
# MAGIC         street_tree_cover_10,
# MAGIC         tree_cover_100,
# MAGIC         count(gridnum) as count_cells,
# MAGIC         sum(AreaHa) as AreaHa
# MAGIC      
# MAGIC from FUA_CUBE_subset
# MAGIC group by fua_code, 
# MAGIC         fua_name,
# MAGIC         swf_cover_1,
# MAGIC         street_tree_cover_10,
# MAGIC         tree_cover_100,
# MAGIC         clc_plus_code,
# MAGIC         ua2018_class_code
# MAGIC
# MAGIC       
# MAGIC             """)
# MAGIC
# MAGIC FUA_CUBE_subset_stat.createOrReplaceTempView("FUA_CUBE_subset_stat")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC FUA_CUBE_subset_stat
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC    // .save("dbfs:/mnt/trainingDatabricks/ExportTable/EEA10km//")
# MAGIC     //.save("dbfs:/mnt/trainingDatabricks/ExportTable/EEA10km/export10km_tableDF_10km_lcf_carbon_de")
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/copernicus")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from FUA_CUBE_subset

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC Select 
# MAGIC fua_ua.gridnum,
# MAGIC fua_ua.AreaHa,
# MAGIC fua_ua.fua_code,
# MAGIC fua_ua.fua_name,
# MAGIC
# MAGIC clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 as clc_plus_code,
# MAGIC ua2018.Category2018 as ua2018_class_code,
# MAGIC
# MAGIC
# MAGIC swf18_aoi.SWF_2018_10m_subest_lux_0100 as swf_orig,  -- % of pixel 0/25/50/75/100
# MAGIC street_tree_2018.Category as street_tree_orig,
# MAGIC treecover2018.Category as tree_cover_orig,
# MAGIC
# MAGIC IF(SWF_2018_10m_subest_lux_0100 >0 , 1,0)as swf_cover_1,
# MAGIC IF(street_tree_2018.Category >0 , 10,0) as street_tree_cover_10,
# MAGIC IF(treecover2018.Category>0 , 100,0) as tree_cover_100,
# MAGIC IF(clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 in (2,3,4,5) , 1000,0) as clcPlus_1000
# MAGIC from fua_ua 
# MAGIC
# MAGIC left JOIN swf18_aoi ON fua_ua.gridnum = swf18_aoi.gridnum  
# MAGIC left JOIN street_tree_2018 ON fua_ua.gridnum = street_tree_2018.gridnum  
# MAGIC left JOIN treecover2018 ON fua_ua.gridnum = treecover2018.gridnum  
# MAGIC left JOIN clc_plus_bb_2018 ON fua_ua.gridnum = clc_plus_bb_2018.gridnum  
# MAGIC left JOIN ua2018 ON fua_ua.gridnum = ua2018.gridnum  
# MAGIC
# MAGIC where fua_ua.fua_code in ('DE026') and ua2018.Category2018 in (11100,	11210,	11220,	11230,	11240,	11300,	12100,	12210,	12220,	12230,	12300,	12400,	13100,	13300,	13400,	14100,	14200) 
# MAGIC
# MAGIC ----where fua_ua.fua_code = 'LU001' or fua_ua.fua_code = 'FR017' or fua_ua.fua_code = 'DE026' --- Luxembourg and Metz or Trier
# MAGIC
# MAGIC  LIMIT 100
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from FUA_CUBE_trier_stat

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from FUA_CUBE_trier

# COMMAND ----------

# MAGIC %md
# MAGIC ## (4) Correlation by 10m cell
# MAGIC

# COMMAND ----------

## read cube
df_test= spark.sql("""
Select * from LUT_fua limit 5
            """)

display (df_test)

# COMMAND ----------



## read cube  (limit by 1000 and one FUA!!!!)
df_cube= spark.sql("""

Select 
fua_ua.gridnum,
fua_ua.AreaHa,
fua_ua.fua_code,
fua_ua.fua_name,

clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 as clc_plus_code,
ua2018.Category2018 as ua2018_class_code,


swf18_aoi.SWF_2018_10m_subest_lux_0100 as swf_orig,  -- % of pixel 0/25/50/75/100
street_tree_2018.Category as street_tree_orig,
treecover2018.Category as tree_cover_orig,

IF(SWF_2018_10m_subest_lux_0100 >0 , 1,0)as swf_cover_1,
IF(street_tree_2018.Category >0 , 10,0) as street_tree_cover_10,
IF(treecover2018.Category>0 , 100,0) as tree_cover_100


from fua_ua 

left JOIN swf18_aoi ON fua_ua.gridnum = swf18_aoi.gridnum  
left JOIN street_tree_2018 ON fua_ua.gridnum = street_tree_2018.gridnum  
left JOIN treecover2018 ON fua_ua.gridnum = treecover2018.gridnum  
left JOIN clc_plus_bb_2018 ON fua_ua.gridnum = clc_plus_bb_2018.gridnum  
left JOIN ua2018 ON fua_ua.gridnum = ua2018.gridnum  

where fua_ua.fua_code in ('DE026')

----where fua_ua.fua_code = 'LU001' or fua_ua.fua_code = 'FR017' or fua_ua.fua_code = 'DE026' --- Luxembourg and Metz or Trier

 LIMIT 100
          
            """)



# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ///## read cube  (one FUA!!!!)
# MAGIC val FUA_CUBE_DE026_correlation = spark.sql("""
# MAGIC
# MAGIC Select 
# MAGIC fua_ua.gridnum,
# MAGIC fua_ua.AreaHa,
# MAGIC fua_ua.fua_code,
# MAGIC fua_ua.fua_name,
# MAGIC
# MAGIC clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 as clc_plus_code,
# MAGIC ua2018.Category2018 as ua2018_class_code,
# MAGIC
# MAGIC
# MAGIC swf18_aoi.SWF_2018_10m_subest_lux_0100 as swf_orig,  -- % of pixel 0/25/50/75/100
# MAGIC street_tree_2018.Category as street_tree_orig,
# MAGIC treecover2018.Category as tree_cover_orig,
# MAGIC
# MAGIC IF(SWF_2018_10m_subest_lux_0100 >0 , 1,0)as swf_cover_1,
# MAGIC IF(street_tree_2018.Category >0 , 10,0) as street_tree_cover_10,
# MAGIC IF(treecover2018.Category>0 , 100,0) as tree_cover_100,
# MAGIC IF(clc_plus_bb_2018.CLMS_CLCplus_RASTER_2018_010m_eu_03035 in (2,3,4,5) , 1000,0) as clcPlus_1000
# MAGIC
# MAGIC from fua_ua 
# MAGIC
# MAGIC left JOIN swf18_aoi ON fua_ua.gridnum = swf18_aoi.gridnum  
# MAGIC left JOIN street_tree_2018 ON fua_ua.gridnum = street_tree_2018.gridnum  
# MAGIC left JOIN treecover2018 ON fua_ua.gridnum = treecover2018.gridnum  
# MAGIC left JOIN clc_plus_bb_2018 ON fua_ua.gridnum = clc_plus_bb_2018.gridnum  
# MAGIC left JOIN ua2018 ON fua_ua.gridnum = ua2018.gridnum  
# MAGIC
# MAGIC where fua_ua.fua_code in ('DE026') and ua2018.Category2018 in (11100,	11210,	11220,	11230,	11240,	11300,	12100,	12210,	12220,	12230,	12300,	12400,	13100,	13300,	13400,	14100,	14200) --- only artif. ua. classes
# MAGIC
# MAGIC ----where fua_ua.fua_code = 'LU001' or fua_ua.fua_code = 'FR017' or fua_ua.fua_code = 'DE026' --- Luxembourg and Metz or Trier
# MAGIC
# MAGIC --- LIMIT 100
# MAGIC           
# MAGIC             """)
# MAGIC
# MAGIC FUA_CUBE_DE026_correlation.createOrReplaceTempView("FUA_CUBE_DE026_correlation")
# MAGIC
# MAGIC ///////////////////////// Correlation for DE023 Pixel based:
# MAGIC
# MAGIC val FUA_DE026_correlation = spark.sql("""
# MAGIC
# MAGIC --- correlation using SQL: 
# MAGIC SELECT 
# MAGIC corr(swf_cover_1, street_tree_cover_10) as swf_stl_1_10,
# MAGIC corr(swf_cover_1, tree_cover_100) as swf_tc_1_100,
# MAGIC corr(swf_cover_1, clcPlus_1000) as swf_clcplus_1_1000,
# MAGIC corr(tree_cover_100, clcPlus_1000) as tc_clcplus_100_1000,
# MAGIC corr(clcPlus_1000, street_tree_cover_10) as swf_stl_10_1000,
# MAGIC corr(tree_cover_100, street_tree_cover_10) as tc_stl_10_100
# MAGIC FROM FUA_CUBE_DE026_correlation ----as tab(swf_cover_1, street_tree_cover_10) 
# MAGIC             """)
# MAGIC FUA_DE026_correlation.createOrReplaceTempView("FUA_CUBE_test_coFUA_DE026_correlationrrelation")
# MAGIC
# MAGIC display(FUA_DE026_correlation)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from FUA_CUBE_DE026_correlation

# COMMAND ----------

# MAGIC %scala
# MAGIC val FUA_DE026_correlation = spark.sql("""
# MAGIC
# MAGIC --- correlation using SQL: 
# MAGIC SELECT 
# MAGIC corr(swf_cover_1, street_tree_cover_10) as swf_stl_1_10,
# MAGIC corr(swf_cover_1, tree_cover_100) as swf_tc_1_100,
# MAGIC corr(swf_cover_1, clcPlus_1000) as swf_clcplus_1_1000,
# MAGIC corr(tree_cover_100, clcPlus_1000) as tc_clcplus_100_1000,
# MAGIC corr(clcPlus_1000, street_tree_cover_10) as swf_stl_10_1000,
# MAGIC corr(tree_cover_100, street_tree_cover_10) as tc_stl_10_100
# MAGIC FROM FUA_CUBE_test_correlation ----as tab(swf_cover_1, street_tree_cover_10) 
# MAGIC
# MAGIC FUA_DE026_correlation.createOrReplaceTempView("FUA_CUBE_test_coFUA_DE026_correlationrrelation")
# MAGIC

# COMMAND ----------

# correlation between swf_cover_1  and street_tree_cover_10


#https://stackoverflow.com/questions/53787663/contains-pyspark-sql-typeerror-column-object-is-not-callable




# COMMAND ----------


display(df_cube)





# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC  SELECT corr(c1, c2) FROM VALUES (3, 2), (3, 3), (3, 3), (6, 4) as tab(c1, c2);
# MAGIC  0.816496580927726
# MAGIC  
