# Databricks notebook source
# MAGIC %md # Urban GREEN - statistics on urban green inside FUA 
# MAGIC
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/ua_2018_legend.PNG?raw=true)

# COMMAND ----------

# MAGIC %md ## 1) Reading DIMs
# MAGIC
# MAGIC check encoding:
# MAGIC https://docs.databricks.com/en/sql/language-manual/functions/encode.html
# MAGIC  .option("encoding", "UTF-16")  /// check ENCODING

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
# MAGIC //// (0) ADMIN layer  Nuts2021 ################################################################################
# MAGIC // Reading the admin DIM:---------------------------------------------
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1517&fileId=542
# MAGIC val parquetFileDF_D_ADMbndEEA39v2021 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ADMbndEEA39v2021_542_2022613_100m/")             /// use load
# MAGIC parquetFileDF_D_ADMbndEEA39v2021.createOrReplaceTempView("D_admbndEEA39v2021")
# MAGIC
# MAGIC ///// Reading the LUT :---------------------------------------------
# MAGIC ///https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=65
# MAGIC
# MAGIC import org.apache.spark.sql.types._
# MAGIC val schema_nuts2021 = new StructType()
# MAGIC .add("ADM_ID",LongType,true)
# MAGIC .add("ISO2",StringType,true)
# MAGIC .add("ESTAT",StringType,true)
# MAGIC .add("ADM_COUNTRY",StringType,true)
# MAGIC
# MAGIC .add("LEVEL3_name",StringType,true)
# MAGIC .add("LEVEL2_name",StringType,true)
# MAGIC .add("LEVEL1_name",StringType,true)
# MAGIC .add("LEVEL0_name",StringType,true)
# MAGIC .add("LEVEL3_code",StringType,true)
# MAGIC .add("LEVEL2_code",StringType,true)
# MAGIC .add("LEVEL1_code",StringType,true)
# MAGIC .add("LEVEL0_code",StringType,true)
# MAGIC
# MAGIC .add("EEA32_2020",IntegerType,true)
# MAGIC .add("EEA38_2020",IntegerType,true)
# MAGIC .add("EEA39",IntegerType,true)
# MAGIC .add("EEA33",IntegerType,true)
# MAGIC .add("EEA32_2006",IntegerType,true)
# MAGIC .add("EU27_2020",IntegerType,true)
# MAGIC .add("EU28",IntegerType,true)
# MAGIC .add("EU27_2007",IntegerType,true)
# MAGIC .add("EU25",IntegerType,true)
# MAGIC .add("EU15",IntegerType,true)
# MAGIC .add("EU12",IntegerType,true)
# MAGIC .add("EU10",IntegerType,true)
# MAGIC .add("EFTA4",IntegerType,true)
# MAGIC .add("NUTS_EU",StringType,true)
# MAGIC .add("TAA",StringType,true)
# MAGIC
# MAGIC
# MAGIC val LUT_nuts2021  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_nuts2021)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/adm_eea39_2021LUT/20200527111402.69.csv")
# MAGIC LUT_nuts2021.createOrReplaceTempView("LUT_nuts2021")
# MAGIC
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and NUTS information:---------------------------------------------
# MAGIC
# MAGIC val nuts3_2021 = spark.sql(""" 
# MAGIC                SELECT 
# MAGIC
# MAGIC D_admbndEEA39v2021.GridNum,
# MAGIC D_admbndEEA39v2021.Category,
# MAGIC D_admbndEEA39v2021.AreaHa,
# MAGIC D_admbndEEA39v2021.GridNum10km,
# MAGIC D_admbndEEA39v2021.gridnum &  -16777216 as GridNum1km,
# MAGIC LUT_nuts2021.ADM_ID,
# MAGIC LUT_nuts2021.ADM_COUNTRY	,
# MAGIC LUT_nuts2021.ISO2	,
# MAGIC LUT_nuts2021.LEVEL3_name	,
# MAGIC LUT_nuts2021.LEVEL2_name	,
# MAGIC LUT_nuts2021.LEVEL1_name	,
# MAGIC LUT_nuts2021.LEVEL0_name	,
# MAGIC LUT_nuts2021.LEVEL3_code	,
# MAGIC LUT_nuts2021.LEVEL2_code	,
# MAGIC LUT_nuts2021.LEVEL1_code	,
# MAGIC LUT_nuts2021.LEVEL0_code	,
# MAGIC LUT_nuts2021.EEA32_2020	,
# MAGIC LUT_nuts2021.EEA38_2020,	
# MAGIC LUT_nuts2021.EEA39	,
# MAGIC LUT_nuts2021.EEA33	,
# MAGIC LUT_nuts2021.EEA32_2006,	
# MAGIC LUT_nuts2021.EU27_2020	,
# MAGIC LUT_nuts2021.EU28	,
# MAGIC LUT_nuts2021.EU27_2007,	
# MAGIC LUT_nuts2021.EU25	,
# MAGIC LUT_nuts2021.EU15	,
# MAGIC LUT_nuts2021.EU12	,
# MAGIC LUT_nuts2021.EU10	,
# MAGIC LUT_nuts2021.EFTA4	,
# MAGIC LUT_nuts2021.NUTS_EU,	
# MAGIC LUT_nuts2021.TAA	
# MAGIC
# MAGIC FROM D_admbndEEA39v2021 
# MAGIC   LEFT JOIN LUT_nuts2021  ON D_admbndEEA39v2021.Category = LUT_nuts2021.ADM_ID 
# MAGIC   
# MAGIC        
# MAGIC  
# MAGIC                                   """)
# MAGIC
# MAGIC nuts3_2021.createOrReplaceTempView("nuts3_2021")
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
# MAGIC
# MAGIC
# MAGIC
# MAGIC //// FOREST 2018 based on Urban Atlas 10m: ########################################################################
# MAGIC val forest_ua_2018 = spark.sql(""" 
# MAGIC select 
# MAGIC     gridnum,
# MAGIC     Category2018 as UA_2018_class_code,
# MAGIC     areaHa,'forest' as forest_ua
# MAGIC     from ua2018
# MAGIC     where Category2018 =31000         
# MAGIC                                   """)
# MAGIC forest_ua_2018.createOrReplaceTempView("forest_ua_2018")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (5) city (core city) urban areas   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2024&fileId=1046&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_city_urban_audit2021_1046_2023922_10m
# MAGIC // CITY 2021:
# MAGIC // LUT: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=154
# MAGIC // cwsblobstorage01/cwsblob01/Lookups/LUT_CITY_2021_r/20230922142519.42.csv
# MAGIC
# MAGIC val parquetFileDF_city = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_city_urban_audit2021_1046_2023922_10m/")  /// use load
# MAGIC parquetFileDF_city.createOrReplaceTempView("city_base_2021")
# MAGIC
# MAGIC val schema_city = new StructType()
# MAGIC .add("ogc_fid",IntegerType,true)
# MAGIC .add("URAU_CODE",StringType,true)
# MAGIC .add("URAU_CATG",StringType,true)
# MAGIC .add("cntr_code",StringType,true)
# MAGIC .add("urau_name",StringType,true)
# MAGIC .add("city_cptl",StringType,true)
# MAGIC .add("fua_code",StringType,true)
# MAGIC .add("area_sqm",StringType,true)
# MAGIC .add("nuts3_2021",StringType,true)
# MAGIC .add("fid",IntegerType,true)
# MAGIC .add("_wgs84x",IntegerType,true)
# MAGIC .add("_wgs84y",IntegerType,true)
# MAGIC .add("_laeax",IntegerType,true)
# MAGIC .add("_laeay",IntegerType,true)
# MAGIC .add("city_code",StringType,true)
# MAGIC .add("raster_val",IntegerType,true)
# MAGIC val LUT_city  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_city)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/LUT_CITY_2021_r/20230922142519.42.csv")
# MAGIC LUT_city.createOrReplaceTempView("LUT_city2021")
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and FUA information:---------------------------------------------
# MAGIC val city_2021 = spark.sql(""" 
# MAGIC      select 
# MAGIC       city_base_2021.gridnum, --10m
# MAGIC       city_base_2021.gridnum & cast(-65536 as bigint) as GridNum100m, ----  100m
# MAGIC       city_base_2021.gridnum & cast(-65536 as bigint) &  -16777216 as GridNUM1km, --- 1m
# MAGIC       city_base_2021.GridNum10km,
# MAGIC       city_base_2021.urban_audit_2021_city_10m,
# MAGIC       city_base_2021.AreaHa,
# MAGIC       
# MAGIC
# MAGIC       LUT_city2021.URAU_CODE,
# MAGIC
# MAGIC       LUT_city2021.URAU_CODE,
# MAGIC       LUT_city2021.URAU_CATG,
# MAGIC       LUT_city2021.urau_name,
# MAGIC       LUT_city2021.fua_code,
# MAGIC       LUT_city2021.nuts3_2021,
# MAGIC       LUT_city2021.city_code
# MAGIC           from city_base_2021
# MAGIC            left join   LUT_city2021 on LUT_city2021.raster_val=city_base_2021.urban_audit_2021_city_10m   
# MAGIC
# MAGIC                                  """)
# MAGIC city_2021.createOrReplaceTempView("city_2021")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (5) tree-cover density   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1669&fileId=694&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/D_TreeCoverDens201810m_694_202134_10m/
# MAGIC
# MAGIC
# MAGIC val treecover2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_TreeCoverDens201810m_694_202134_10m//")             /// use load
# MAGIC treecover2018.createOrReplaceTempView("treecover2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (6) STREET tree-layer 10m  ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1683&fileId=708
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_StreetTreeUA2018_708_2021326_10m
# MAGIC
# MAGIC
# MAGIC val street_tree = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_StreetTreeUA2018_708_2021326_10m//")             /// use load
# MAGIC street_tree.createOrReplaceTempView("street_tree")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC /// WILD FIRE 
# MAGIC
# MAGIC val lut_fire  = spark.read.format("csv")
# MAGIC .options(Map("delimiter"->","))
# MAGIC  .option("header", "true")
# MAGIC       .load("dbfs:/mnt/trainingDatabricks/LookupTablesFiles/LUT_EFFIS_Fires_00_22_v2_146.csv")
# MAGIC lut_fire.createOrReplaceTempView("LUT_fire")
# MAGIC //cwsblobstorage01/cwsblob01/Lookups/Wildfires0022LUT/20220718121715.823.csv
# MAGIC //https://cwsblobstorage01.blob.core.windows.net/cwsblob01/LookupTablesFiles/LUT_EFFIS_Fires_00_22_138.csv
# MAGIC
# MAGIC //https://cwsblobstorage01.blob.core.windows.net/cwsblob01/LookupTablesFiles/LUT_EFFIS_Fires_00_22_v23_138.csv
# MAGIC val parquetFileDF_wildfire = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_Wildfires0021_905_202331_100m/")
# MAGIC parquetFileDF_wildfire.createOrReplaceTempView("wildfire")
# MAGIC
# MAGIC val fire_sq1 = spark.sql(""" 
# MAGIC  select 
# MAGIC GridNum,
# MAGIC GridNum10km,
# MAGIC p2000.MONTH as FIREDATE_2000,
# MAGIC p2001.MONTH as FIREDATE_2001,
# MAGIC p2002.MONTH as FIREDATE_2002,
# MAGIC p2003.MONTH as FIREDATE_2003,
# MAGIC p2004.MONTH as FIREDATE_2004,
# MAGIC p2005.MONTH as FIREDATE_2005,
# MAGIC p2006.MONTH as FIREDATE_2006,
# MAGIC p2007.MONTH as FIREDATE_2007,
# MAGIC p2008.MONTH as FIREDATE_2008,
# MAGIC p2009.MONTH as FIREDATE_2009,
# MAGIC p2010.MONTH as FIREDATE_2010,
# MAGIC p2011.MONTH as FIREDATE_2011,
# MAGIC p2012.MONTH as FIREDATE_2012,
# MAGIC p2013.MONTH as FIREDATE_2013,
# MAGIC p2014.MONTH as FIREDATE_2014,
# MAGIC p2015.MONTH as FIREDATE_2015,
# MAGIC p2016.MONTH as FIREDATE_2016,
# MAGIC p2017.MONTH as FIREDATE_2017,
# MAGIC p2018.MONTH as FIREDATE_2018,
# MAGIC p2019.MONTH as FIREDATE_2019,
# MAGIC p2020.MONTH as FIREDATE_2020,
# MAGIC p2021.MONTH as FIREDATE_2021,
# MAGIC p2022.MONTH as FIREDATE_2022,
# MAGIC
# MAGIC p2000.EFFISID as EFFISID_2000,
# MAGIC p2001.EFFISID as EFFISID_2001,
# MAGIC p2002.EFFISID as EFFISID_2002,
# MAGIC p2003.EFFISID as EFFISID_2003,
# MAGIC p2004.EFFISID as EFFISID_2004,
# MAGIC p2005.EFFISID as EFFISID_2005,
# MAGIC p2006.EFFISID as EFFISID_2006,
# MAGIC p2007.EFFISID as EFFISID_2007,
# MAGIC p2008.EFFISID as EFFISID_2008,
# MAGIC p2009.EFFISID as EFFISID_2009,
# MAGIC p2010.EFFISID as EFFISID_2010,
# MAGIC p2011.EFFISID as EFFISID_2011,
# MAGIC p2012.EFFISID as EFFISID_2012,
# MAGIC p2013.EFFISID as EFFISID_2013,
# MAGIC p2014.EFFISID as EFFISID_2014,
# MAGIC p2015.EFFISID as EFFISID_2015,
# MAGIC p2016.EFFISID as EFFISID_2016,
# MAGIC p2017.EFFISID as EFFISID_2017,
# MAGIC p2018.EFFISID as EFFISID_2018,
# MAGIC p2019.EFFISID as EFFISID_2019,
# MAGIC p2020.EFFISID as EFFISID_2020,
# MAGIC p2021.EFFISID as EFFISID_2021,
# MAGIC p2022.EFFISID as EFFISID_2022,
# MAGIC GridNum10km,
# MAGIC AreaHa
# MAGIC from wildfire 
# MAGIC LEFT JOIN   LUT_fire     as p2000 ON  wildfire.BA2000  = p2000.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2001 ON  wildfire.BA2001  = p2001.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2002 ON  wildfire.BA2002  = p2002.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2003 ON  wildfire.BA2003  = p2003.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2004 ON  wildfire.BA2004  = p2004.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2005 ON  wildfire.BA2005  = p2005.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2006 ON  wildfire.BA2006  = p2006.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2007 ON  wildfire.BA2007  = p2007.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2008 ON  wildfire.BA2008  = p2008.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2009 ON  wildfire.BA2009  = p2009.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2010 ON  wildfire.BA2010  = p2010.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2011 ON  wildfire.BA2011  = p2011.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2012 ON  wildfire.BA2012  = p2012.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2013 ON  wildfire.BA2013  = p2013.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2014 ON  wildfire.BA2014  = p2014.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2015 ON  wildfire.BA2015  = p2015.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2016 ON  wildfire.BA2016  = p2016.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2017 ON  wildfire.BA2017  = p2017.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2018 ON  wildfire.BA2018  = p2018.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2019 ON  wildfire.BA2019  = p2019.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2020 ON  wildfire.BA2020  = p2020.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2021 ON  wildfire.BA2021  = p2021.EFFISID 
# MAGIC LEFT JOIN   LUT_fire     as p2022 ON  wildfire.BA2022  = p2022.EFFISID                       
# MAGIC                                               
# MAGIC  """)
# MAGIC                                   
# MAGIC fire_sq1.createOrReplaceTempView("fire")
# MAGIC
# MAGIC
# MAGIC val fire_sq1_year = spark.sql(""" 
# MAGIC          select  
# MAGIC                   GridNum,
# MAGIC                   GridNum & cast(-16777216 as bigint) as GridNum1km, -----######################## new gridnum 1km !!!
# MAGIC                   GridNum10km,
# MAGIC                   AreaHa,
# MAGIC                   IF(FIREDATE_2000>0, 1,0)as fire_2000,
# MAGIC                   IF(FIREDATE_2001>0, 1,0)as fire_2001,
# MAGIC                   IF(FIREDATE_2002>0, 1,0)as fire_2002,
# MAGIC                   IF(FIREDATE_2003>0, 1,0)as fire_2003,
# MAGIC                   IF(FIREDATE_2004>0, 1,0)as fire_2004,
# MAGIC                   IF(FIREDATE_2005>0, 1,0)as fire_2005,
# MAGIC                   IF(FIREDATE_2006>0, 1,0)as fire_2006,
# MAGIC                   IF(FIREDATE_2007>0, 1,0)as fire_2007,
# MAGIC                   IF(FIREDATE_2008>0, 1,0)as fire_2008,
# MAGIC                   IF(FIREDATE_2009>0, 1,0)as fire_2009,
# MAGIC                   IF(FIREDATE_2010>0, 1,0)as fire_2010,
# MAGIC                   IF(FIREDATE_2011>0, 1,0)as fire_2011,
# MAGIC                   IF(FIREDATE_2012>0, 1,0)as fire_2012,
# MAGIC                   IF(FIREDATE_2013>0, 1,0)as fire_2013,
# MAGIC                   IF(FIREDATE_2014>0, 1,0)as fire_2014,
# MAGIC                   IF(FIREDATE_2015>0, 1,0)as fire_2015,
# MAGIC                   IF(FIREDATE_2016>0, 1,0)as fire_2016,
# MAGIC                   IF(FIREDATE_2017>0, 1,0)as fire_2017,
# MAGIC                   IF(FIREDATE_2018>0, 1,0)as fire_2018,
# MAGIC                   IF(FIREDATE_2019>0, 1,0)as fire_2019,
# MAGIC                   IF(FIREDATE_2020>0, 1,0)as fire_2020,
# MAGIC                   IF(FIREDATE_2021>0, 1,0)as fire_2021,
# MAGIC                   IF(FIREDATE_2022>0, 1,0)as fire_2022
# MAGIC
# MAGIC
# MAGIC
# MAGIC                   from fire 
# MAGIC                  where 
# MAGIC                  IF(FIREDATE_2000>0, 1,0)+
# MAGIC                  IF(FIREDATE_2001>0, 1,0)+
# MAGIC                  IF(FIREDATE_2002>0, 1,0) +
# MAGIC                  IF(FIREDATE_2003>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2004>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2005>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2006>0, 1,0) +
# MAGIC                  IF(FIREDATE_2007>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2008>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2009>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2010>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2011>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2012>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2013>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2014>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2015>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2016>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2017>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2018>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2019>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2020>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2021>0, 1,0)+ 
# MAGIC                  IF(FIREDATE_2022>0, 1,0)
# MAGIC                  >0  --- removing all cells without a fire between 2000-2022
# MAGIC  """)
# MAGIC                                   
# MAGIC fire_sq1_year.createOrReplaceTempView("fire_year")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////20 Drought pressure intensity and occurence 1km 
# MAGIC //##########################################################################################################################################
# MAGIC ///Annual (in growing season) drought pressure intensity https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2023&fileId=1045
# MAGIC ///Annual (in growing season) soil moisture anomaly https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2022&fileId=1044
# MAGIC ///annual nr of drought events (drought pressure occurrence) https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2023&fileId=1045
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_drought_press_occure_1045_2023921_1km
# MAGIC val D_drought_press_occure = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_drought_press_occure_1045_2023921_1km/")
# MAGIC D_drought_press_occure.createOrReplaceTempView("D_drought_press_occure")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2) Testing DIMs & Calculations

# COMMAND ----------

# MAGIC %sql --- testing
# MAGIC show columns from D_drought_press_occure
# MAGIC
# MAGIC ---where GridNUM1km =9404561726898176
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns from fire_year 
# MAGIC

# COMMAND ----------

# MAGIC %md ## 3) Urban indicators

# COMMAND ----------

# MAGIC %md ### 3.1) Tree-cover density by CITY (version 2021) 

# COMMAND ----------

# MAGIC %md
# MAGIC the following box calculatet the Tree-cover inside every CITY:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC the next box builded a urban-fua cube with the combination of Urban Atlas forest, Urban-street tree layer and TreecoverDensity:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns from fire

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC       --ity_2021.gridnum,
# MAGIC       --city_2021.gridnum100m,
# MAGIC       --city_2021.gridnum1km,
# MAGIC       --city_2021.GridNum10km,
# MAGIC
# MAGIC
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name as city_name,
# MAGIC       SUM(city_2021.AreaHa) as AreaHa,
# MAGIC       SUM(treecover2018.category /10000) as Tree_area_HRL_ha,
# MAGIC
# MAGIC     
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2000) as drought_pressure_occurrence_gs_1km_2000,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2001) as drought_pressure_occurrence_gs_1km_2001,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2002) as drought_pressure_occurrence_gs_1km_2002,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2003) as drought_pressure_occurrence_gs_1km_2003,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2004) as drought_pressure_occurrence_gs_1km_2004,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2005) as drought_pressure_occurrence_gs_1km_2005,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2006) as drought_pressure_occurrence_gs_1km_2006,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2007) as drought_pressure_occurrence_gs_1km_2007,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2008) as drought_pressure_occurrence_gs_1km_2008,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2009) as drought_pressure_occurrence_gs_1km_2009,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2010) as drought_pressure_occurrence_gs_1km_2010,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2011) as drought_pressure_occurrence_gs_1km_2011,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2012) as drought_pressure_occurrence_gs_1km_2012,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2013) as drought_pressure_occurrence_gs_1km_2013,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2014) as drought_pressure_occurrence_gs_1km_2014,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2015) as drought_pressure_occurrence_gs_1km_2015,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2016) as drought_pressure_occurrence_gs_1km_2016,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2017) as drought_pressure_occurrence_gs_1km_2017,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2018) as drought_pressure_occurrence_gs_1km_2018,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2019) as drought_pressure_occurrence_gs_1km_2019,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2020) as drought_pressure_occurrence_gs_1km_2020,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2021) as drought_pressure_occurrence_gs_1km_2021,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2022) as drought_pressure_occurrence_gs_1km_2022,
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC       
# MAGIC
# MAGIC             SUM(fire_2000) as fire_2000,
# MAGIC             SUM(fire_2001) as fire_2001,
# MAGIC             SUM(fire_2002) as fire_2002,
# MAGIC             SUM(fire_2003) as fire_2003,
# MAGIC             SUM(fire_2004) as fire_2004,
# MAGIC             SUM(fire_2005) as fire_2005,
# MAGIC             SUM(fire_2006) as fire_2006,
# MAGIC             SUM(fire_2007) as fire_2007,
# MAGIC             SUM(fire_2008) as fire_2008,
# MAGIC             SUM(fire_2009) as fire_2009,
# MAGIC             SUM(fire_2010) as fire_2010,
# MAGIC             SUM(fire_2011) as fire_2011,
# MAGIC             SUM(fire_2012) as fire_2012,
# MAGIC             SUM(fire_2013) as fire_2013,
# MAGIC             SUM(fire_2014) as fire_2014,
# MAGIC             SUM(fire_2015) as fire_2015,
# MAGIC             SUM(fire_2016) as fire_2016,
# MAGIC             SUM(fire_2017) as fire_2017,
# MAGIC             SUM(fire_2018) as fire_2018,
# MAGIC             SUM(fire_2019) as fire_2019,
# MAGIC             SUM(fire_2020) as fire_2020,
# MAGIC             SUM(fire_2021) as fire_2021,
# MAGIC             SUM(fire_2022) as fire_2022
# MAGIC
# MAGIC       from city_2021 -- new fua
# MAGIC
# MAGIC       left join treecover2018 on treecover2018.gridnum= city_2021.gridnum   --value
# MAGIC       left join street_tree on street_tree.gridnum= city_2021.gridnum    -- value
# MAGIC       left join forest_ua_2018 on forest_ua_2018.gridnum= city_2021.gridnum -- value
# MAGIC
# MAGIC
# MAGIC       left join fire_year on fire_year.gridnum = city_2021.gridnum1km
# MAGIC       left join D_drought_press_occure on D_drought_press_occure.GridNum = city_2021.gridnum1km
# MAGIC
# MAGIC ---
# MAGIC       ---LEFT JOIN city_2021 ON city_2021.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---CITY core-city ---outline
# MAGIC   
# MAGIC
# MAGIC group by 
# MAGIC
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val urban_stat = spark.sql(""" 
# MAGIC       
# MAGIC
# MAGIC select 
# MAGIC   
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name as city_name,
# MAGIC       SUM(city_2021.AreaHa) as AreaHa,
# MAGIC       SUM(treecover2018.category /10000) as Tree_area_HRL_ha,
# MAGIC
# MAGIC     
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2000) as drought_pressure_occurrence_gs_1km_2000,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2001) as drought_pressure_occurrence_gs_1km_2001,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2002) as drought_pressure_occurrence_gs_1km_2002,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2003) as drought_pressure_occurrence_gs_1km_2003,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2004) as drought_pressure_occurrence_gs_1km_2004,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2005) as drought_pressure_occurrence_gs_1km_2005,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2006) as drought_pressure_occurrence_gs_1km_2006,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2007) as drought_pressure_occurrence_gs_1km_2007,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2008) as drought_pressure_occurrence_gs_1km_2008,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2009) as drought_pressure_occurrence_gs_1km_2009,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2010) as drought_pressure_occurrence_gs_1km_2010,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2011) as drought_pressure_occurrence_gs_1km_2011,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2012) as drought_pressure_occurrence_gs_1km_2012,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2013) as drought_pressure_occurrence_gs_1km_2013,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2014) as drought_pressure_occurrence_gs_1km_2014,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2015) as drought_pressure_occurrence_gs_1km_2015,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2016) as drought_pressure_occurrence_gs_1km_2016,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2017) as drought_pressure_occurrence_gs_1km_2017,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2018) as drought_pressure_occurrence_gs_1km_2018,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2019) as drought_pressure_occurrence_gs_1km_2019,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2020) as drought_pressure_occurrence_gs_1km_2020,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2021) as drought_pressure_occurrence_gs_1km_2021,
# MAGIC       MAX(drought_pressure_occurrence_gs_1km_2022) as drought_pressure_occurrence_gs_1km_2022,
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC       
# MAGIC
# MAGIC             SUM(fire_2000) as fire_2000,
# MAGIC             SUM(fire_2001) as fire_2001,
# MAGIC             SUM(fire_2002) as fire_2002,
# MAGIC             SUM(fire_2003) as fire_2003,
# MAGIC             SUM(fire_2004) as fire_2004,
# MAGIC             SUM(fire_2005) as fire_2005,
# MAGIC             SUM(fire_2006) as fire_2006,
# MAGIC             SUM(fire_2007) as fire_2007,
# MAGIC             SUM(fire_2008) as fire_2008,
# MAGIC             SUM(fire_2009) as fire_2009,
# MAGIC             SUM(fire_2010) as fire_2010,
# MAGIC             SUM(fire_2011) as fire_2011,
# MAGIC             SUM(fire_2012) as fire_2012,
# MAGIC             SUM(fire_2013) as fire_2013,
# MAGIC             SUM(fire_2014) as fire_2014,
# MAGIC             SUM(fire_2015) as fire_2015,
# MAGIC             SUM(fire_2016) as fire_2016,
# MAGIC             SUM(fire_2017) as fire_2017,
# MAGIC             SUM(fire_2018) as fire_2018,
# MAGIC             SUM(fire_2019) as fire_2019,
# MAGIC             SUM(fire_2020) as fire_2020,
# MAGIC             SUM(fire_2021) as fire_2021,
# MAGIC             SUM(fire_2022) as fire_2022
# MAGIC
# MAGIC       from city_2021 -- new fua
# MAGIC
# MAGIC       left join treecover2018 on treecover2018.gridnum= city_2021.gridnum   --value
# MAGIC       left join street_tree on street_tree.gridnum= city_2021.gridnum    -- value
# MAGIC       left join forest_ua_2018 on forest_ua_2018.gridnum= city_2021.gridnum -- value
# MAGIC
# MAGIC
# MAGIC       left join fire_year on fire_year.gridnum = city_2021.gridnum1km
# MAGIC       left join D_drought_press_occure on D_drought_press_occure.GridNum = city_2021.gridnum1km
# MAGIC
# MAGIC
# MAGIC
# MAGIC group by 
# MAGIC
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name
# MAGIC
# MAGIC
# MAGIC                                   """)
# MAGIC
# MAGIC urban_stat
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC
# MAGIC     ///.option("encoding", "UTF-16")  /// check ENCODING
# MAGIC
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/urban_stat")
# MAGIC
# MAGIC     //urban_stat.createOrReplaceTempView("urban_stat")

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/urban_stat"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print ("-------------------------------------")
        print (URL)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // This box constructed the new Urban FUA TREE CUBE
# MAGIC
# MAGIC val tree_cube_fua = spark.sql(""" 
# MAGIC   select 
# MAGIC       city_2021.gridnum,
# MAGIC       city_2021.GridNum10km,
# MAGIC       city_2021.AreaHa as AreaHa,
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name as city_name,
# MAGIC
# MAGIC       treecover2018.category /10000 as Tree_area_HRL_ha,
# MAGIC       street_tree.areaHa  as Tree_area_Street_ha,
# MAGIC       forest_ua_2018.areaHa  as Tree_area_UA18_forest_ha,
# MAGIC
# MAGIC       if(treecover2018.category>0, 0.01, if(street_tree.areaHa>0,0.01, if(forest_ua_2018.areaHa >0, 0.01,0))) as TREE_area_ha
# MAGIC
# MAGIC
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC
# MAGIC       left join treecover2018 on treecover2018.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   --value
# MAGIC       left join street_tree on street_tree.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum    -- value
# MAGIC       left join forest_ua_2018 on forest_ua_2018.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum -- value
# MAGIC
# MAGIC       LEFT JOIN city_2021 ON city_2021.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---CITY core-city outline
# MAGIC   
# MAGIC
# MAGIC
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC       
# MAGIC                                     """)
# MAGIC
# MAGIC       tree_cube_fua.createOrReplaceTempView("tree_cube_fua")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val tree_fua = spark.sql(""" 
# MAGIC             select 
# MAGIC             fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             lau_name,
# MAGIC             sum(AreaHa) as AreaHa,
# MAGIC             sum(Tree_area_HRL_ha) as Tree_area_HRL_ha,
# MAGIC             sum(Tree_area_Street_ha) as Tree_area_Street_ha,
# MAGIC             sum(Tree_area_UA18_forest_ha) as  Tree_area_UA18_forest_ha,
# MAGIC             sum(TREE_area_ha) as  TREE_area_ha,
# MAGIC             100.00/SUM(AreaHa) * SUM(TREE_area_ha) as Tree_area_UA18_forest_percent
# MAGIC
# MAGIC             from tree_cube_fua
# MAGIC             where fua_code is not null
# MAGIC             group by fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             lau_name
# MAGIC                                   """)
# MAGIC
# MAGIC tree_fua
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC
# MAGIC     ///.option("encoding", "UTF-16")  /// check ENCODING
# MAGIC
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_tree_cover1")
# MAGIC
# MAGIC     tree_fua.createOrReplaceTempView("tree_fua_indicator")
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_tree_cover1"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ### 3.2) Green Urban Areas - based on Urban Atlas 2018 and other data sources

# COMMAND ----------

# MAGIC %md
# MAGIC Urban Green Infrastructure, 2018
# MAGIC
# MAGIC Green infrastructure in urban areas consist of vegetated green surfaces, such as parks, trees and small forests, grasslands, but also private gardens or cemeteries. These all contribute to supporting biodiversity, pollinators, carbon sequestration, flood protection and protection against excess heats events. This dashboard facilitates the understanding of the amount of urban green in Functional Urban Areas of the EU and EEA member states.
# MAGIC
# MAGIC https://www.eea.europa.eu/data-and-maps/dashboards/urban-green-infrastructure-2018
# MAGIC
# MAGIC
# MAGIC https://jedi.discomap.eea.europa.eu/Cube/Show/218
# MAGIC
# MAGIC   Green Infrastructure Elements (10m x 10m)
# MAGIC
# MAGIC   Green Infrastructure Hubs (10m x 10m)
# MAGIC   
# MAGIC   Green Infrastructure Links (10m x 10m)
# MAGIC
# MAGIC   Green Infrastructure Physical Network (10m x 10m)
# MAGIC   
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC     gridnum,
# MAGIC     Category2018 as UA_2018_class_code,
# MAGIC     areaHa,
# MAGIC     if(Category2018 in ())
# MAGIC     'green_urban_areas' as urban_indicator
# MAGIC
# MAGIC     from ua2018
# MAGIC     where Category2018 =31000    
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### 3.3) Blue Urban Areas - bases on Urban Atlas 2018

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC     gridnum,
# MAGIC     Category2018 as UA_2018_class_code,
# MAGIC     areaHa,
# MAGIC     if(Category2018 in (50000), areaHa, 0) as urban_blue_indicator
# MAGIC
# MAGIC     from ua2018
# MAGIC     where Category2018 =50000    

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // This box constructed the new Urban FUA TREE CUBE
# MAGIC
# MAGIC val blue_cube_fua = spark.sql(""" 
# MAGIC   select 
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum10km,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa as AreaHa,
# MAGIC
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name as city_name,
# MAGIC       lau_2020.LAU_ID as lau_code,
# MAGIC       lau_2020.LAU_NAME as lau_name,
# MAGIC
# MAGIC       if(ua2018.Category2018 in (50000), FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa, 0) as urban_blue_indicator_2018
# MAGIC
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC       LEFT JOIN city_2021 ON city_2021.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---CITY core-city outline
# MAGIC       LEFT JOIN lau_2020 ON lau_2020.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---lau  outline
# MAGIC       LEFT JOIN ua2018 ON ua2018.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---lau  outline
# MAGIC
# MAGIC
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC       
# MAGIC                                     """)
# MAGIC
# MAGIC       blue_cube_fua.createOrReplaceTempView("bluecube_fua")
# MAGIC
# MAGIC
# MAGIC val blue_c_cube_fua = spark.sql(""" 
# MAGIC             select 
# MAGIC             fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             lau_name,
# MAGIC             sum(AreaHa) as AreaHa,
# MAGIC             sum(urban_blue_indicator_2018) as urban_blue_indicator_2018
# MAGIC             from bluecube_fua
# MAGIC             where fua_code is not null
# MAGIC             group by fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             lau_name
# MAGIC                                   """)
# MAGIC
# MAGIC blue_c_cube_fua
# MAGIC
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC
# MAGIC     ///.option("encoding", "UTF-16")  /// check ENCODING
# MAGIC
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_blue")
# MAGIC
# MAGIC     blue_c_cube_fua.createOrReplaceTempView("blue_c_cube_fua")
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_blue"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from bluecube_fua
