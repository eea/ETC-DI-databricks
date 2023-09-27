# Databricks notebook source
# MAGIC %md # Wildfires 2023 & GDMP
# MAGIC
# MAGIC The following notebooks compared the wild fire timeseries with the GDMP time series und burnt Carbon.
# MAGIC -/info: loehnertz@space4environment.com
# MAGIC ---add. TEXT
# MAGIC

# COMMAND ----------

# MAGIC %md ### 1 Load DIMS:
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import pyspark
# MAGIC from delta import *
# MAGIC # Required for StructField, StringType, IntegerType, etc.
# MAGIC from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md #### 1.1 Wildfires

# COMMAND ----------

# MAGIC %scala
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
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from fire_year where fire_2020 >0
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### 1.2 Administrative Units - NUTS3

# COMMAND ----------

# MAGIC %scala
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
# MAGIC
# MAGIC D_admbndEEA39v2021.GridNum10km,
# MAGIC D_admbndEEA39v2021.GridNum & cast(-16777216 as bigint) as GridNum1km, -----######################## new gridnum 1km !!!
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
# MAGIC                                   """)
# MAGIC
# MAGIC nuts3_2021.createOrReplaceTempView("nuts3_2021")
# MAGIC //################################################################################################

# COMMAND ----------

# MAGIC %md #### 1.3 Protected areas

# COMMAND ----------

# MAGIC %scala
# MAGIC val PA2022 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_PA2022_100m_935_20221111_100m/")             /// use load
# MAGIC PA2022.createOrReplaceTempView("PA2022_raw")
# MAGIC
# MAGIC
# MAGIC val pa_update_sq1 = spark.sql(""" 
# MAGIC                    SELECT     
# MAGIC                      gridnum, GridNum10km,
# MAGIC                      if(ProtectedArea2022_10m >0,'Procteted','Not protected') as protected_area  ,   
# MAGIC                      
# MAGIC                      AreaHa
# MAGIC  
# MAGIC  from PA2022_raw                                 
# MAGIC                                                         """)                                  
# MAGIC pa_update_sq1.createOrReplaceTempView("PA2022") 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from PA2022_raw where 
# MAGIC ProtectedArea2022_10m =0

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from PA2022

# COMMAND ----------

# MAGIC %md #### 1.4 MAES based on CLC

# COMMAND ----------

# MAGIC %scala
# MAGIC //// (1) CLC and LUT-clc for MAES classes ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the MAES classes:
# MAGIC // Reading CLC2018 100m DIM:.....
# MAGIC val parquetFileDF_clc18 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_A_CLC_18_210_20181129_100m/")
# MAGIC parquetFileDF_clc18.createOrReplaceTempView("CLC_2018")
# MAGIC
# MAGIC // Reading the LUT for CLC...:
# MAGIC val lut_clc  = spark.read.format("csv")
# MAGIC .options(Map("delimiter"->","))
# MAGIC  .option("header", "true")
# MAGIC    .load("dbfs:/mnt/trainingDatabricks/LookupTablesFiles/Corine_Land_Cover_LUT_JEDI_4.csv")     ////------Lookup_CLC_07112022_4.csv   Lookup_CLC_24032021_4.csv
# MAGIC lut_clc.createOrReplaceTempView("LUT_clc_classes")
# MAGIC // Construction of a new table: with MAES level 1 classes bases on CLC2018 100m:...................
# MAGIC val maes_sq1 = spark.sql(""" 
# MAGIC                    SELECT                
# MAGIC                   CLC_2018.GridNum,
# MAGIC                   CLC_2018.GridNum10km,                     
# MAGIC                   CONCAT('MAES_',LUT_clc_classes.MAES_CODE) as MAES_CODE ,                  
# MAGIC                   CLC_2018.AreaHa
# MAGIC                   from CLC_2018   
# MAGIC                   LEFT JOIN   LUT_clc_classes  
# MAGIC                      ON  CLC_2018.Category  = LUT_clc_classes.LEVEL3_CODE where AreaHa = 1                                 
# MAGIC                                                         """)                                  
# MAGIC maes_sq1.createOrReplaceTempView("maes_sq1")  
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################

# COMMAND ----------

# MAGIC %md #### 1.5 Environmental Zones

# COMMAND ----------

# MAGIC %scala
# MAGIC val EnvZones_dim = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_EnvZones_544_2020528_100m/")             /// use load
# MAGIC EnvZones_dim.createOrReplaceTempView("D_EnvZones")
# MAGIC
# MAGIC //// Reading the LUT :---------------------------------------------
# MAGIC ///https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=66
# MAGIC
# MAGIC import org.apache.spark.sql.types._
# MAGIC val schema_EnvZones = new StructType()
# MAGIC .add("Value",IntegerType,true)
# MAGIC .add("Description",StringType,true)
# MAGIC .add("Text",StringType,true)
# MAGIC
# MAGIC val LUT_EnvZones  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_EnvZones)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups//EnvZones/20200528182525.807.csv")
# MAGIC LUT_EnvZones.createOrReplaceTempView("LUT_EnvZones")
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and NUTS information:---------------------------------------------
# MAGIC
# MAGIC val EnvZones = spark.sql(""" 
# MAGIC                SELECT 
# MAGIC
# MAGIC D_EnvZones.GridNum,
# MAGIC D_EnvZones.Category,
# MAGIC D_EnvZones.AreaHa,
# MAGIC D_EnvZones.GridNum10km,
# MAGIC LUT_EnvZones.Description,
# MAGIC LUT_EnvZones.Text	
# MAGIC
# MAGIC FROM D_EnvZones 
# MAGIC   LEFT JOIN LUT_EnvZones  ON D_EnvZones.Category = LUT_EnvZones.Description 
# MAGIC
# MAGIC                                   """)
# MAGIC
# MAGIC EnvZones.createOrReplaceTempView("EnvZones")

# COMMAND ----------

# MAGIC %md #### 1.6 LandCoverFlows

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (3) CLC and LUT-LandCoverFlows  ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //(A) Reading lookup table for LandCover Flows: #############################################################
# MAGIC
# MAGIC val schema_lcf = new StructType()
# MAGIC .add("SCHANG",IntegerType,true)
# MAGIC .add("LCF3",StringType,true)
# MAGIC .add("LCF2",StringType,true)
# MAGIC .add("LCF1",StringType,true)
# MAGIC .add("LCFL3",StringType,true)
# MAGIC .add("LCFL2",StringType,true)
# MAGIC .add("LCFL1",StringType,true)
# MAGIC .add("CHANGE",StringType,true)
# MAGIC .add("GRIDCODE",IntegerType,true)
# MAGIC .add("CC",IntegerType,true)
# MAGIC .add("GCHA",IntegerType,true)
# MAGIC .add("lcf3_num",IntegerType,true)
# MAGIC .add("Cng_n",IntegerType,true)
# MAGIC .add("LTAKE",IntegerType,true)
# MAGIC .add("INV_LTAKE",IntegerType,true)
# MAGIC .add("LTAKE_ALL",IntegerType,true)
# MAGIC .add("LD_code",StringType,true)
# MAGIC .add("LD_description",StringType,true)
# MAGIC .add("SOC_factor",FloatType,true)
# MAGIC
# MAGIC
# MAGIC val lut_lcf  = spark.read.format("csv")
# MAGIC     .options(Map("delimiter"->","))
# MAGIC     .schema(schema_lcf)
# MAGIC     .option("header", "true")
# MAGIC     .load("dbfs:/mnt/trainingDatabricks//LookupTablesFiles/LCF_LUT_version02June2021_68.csv")
# MAGIC lut_lcf.createOrReplaceTempView("lcf")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //#####################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1086&fileId=106
# MAGIC
# MAGIC //Now joining the LUT to CLC 2000-2018 CLC:
# MAGIC
# MAGIC val clc0018_dim = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_LCF0018_106_2018328_100m/")             /// use load
# MAGIC clc0018_dim.createOrReplaceTempView("clc0018_dim")
# MAGIC // joining: clc with lut from lcf
# MAGIC val lcf_0018 = spark.sql(""" 
# MAGIC                SELECT 
# MAGIC                 clc0018_dim.GridNum,
# MAGIC                 clc0018_dim.AreaHa,
# MAGIC                 clc0018_dim.GridNum10km,
# MAGIC                 lcf.SCHANG,
# MAGIC                 lcf.LCF3	
# MAGIC
# MAGIC                 FROM clc0018_dim 
# MAGIC   LEFT JOIN lcf  ON clc0018_dim.clc0018 = lcf.SCHANG 
# MAGIC
# MAGIC                                   """)
# MAGIC
# MAGIC lcf_0018.createOrReplaceTempView("lcf_0018")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md #### 1.7 GDMP time series
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 13 (GDMP 1km   1999-2022)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  absolute value and standard deviation The GDMP_annual is expressed in kg DM/ha    (DM= dry matter)
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2020&fileId=1042
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_pv_1042_2023918_1km
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_pv_1042_2023918_1km/")
# MAGIC parquetFileDF_gdmp_1km.createOrReplaceTempView("gdmp_1km_pv_raw")
# MAGIC
# MAGIC
# MAGIC // we found GAPs in the time-series.. therefore we add. an attribute which shows the gaps [QC_gap_YES]
# MAGIC // if the attribute is 1, then this row should not be used for statistics OR a gab filling should be done:
# MAGIC
# MAGIC val GDMP_1km_99_22 = spark.sql(""" 
# MAGIC Select
# MAGIC gridnum as GridNum1km,
# MAGIC GridNum10km,
# MAGIC x,
# MAGIC y,
# MAGIC AreaHa,
# MAGIC
# MAGIC GDMP_1999_pv_1000m_EPSG3035 as GDMP_1999 ,
# MAGIC GDMP_2000_pv_1000m_EPSG3035 as GDMP_2000 ,
# MAGIC GDMP_2001_pv_1000m_EPSG3035 as GDMP_2001 ,
# MAGIC GDMP_2002_pv_1000m_EPSG3035 as GDMP_2002 ,
# MAGIC GDMP_2003_pv_1000m_EPSG3035 as GDMP_2003 ,
# MAGIC GDMP_2004_pv_1000m_EPSG3035 as GDMP_2004 ,
# MAGIC GDMP_2005_pv_1000m_EPSG3035 as GDMP_2005 ,
# MAGIC GDMP_2006_pv_1000m_EPSG3035 as GDMP_2006 ,
# MAGIC GDMP_2007_pv_1000m_EPSG3035 as GDMP_2007 ,
# MAGIC GDMP_2008_pv_1000m_EPSG3035 as GDMP_2008 ,
# MAGIC GDMP_2009_pv_1000m_EPSG3035 as GDMP_2009 ,
# MAGIC GDMP_2010_pv_1000m_EPSG3035 as GDMP_2010 ,
# MAGIC GDMP_2011_pv_1000m_EPSG3035 as GDMP_2011 ,
# MAGIC GDMP_2012_pv_1000m_EPSG3035 as GDMP_2012 ,
# MAGIC GDMP_2013_pv_1000m_EPSG3035 as GDMP_2013 ,
# MAGIC GDMP_2014_pv_1000m_EPSG3035 as GDMP_2014 ,
# MAGIC GDMP_2015_pv_1000m_EPSG3035 as GDMP_2015 ,
# MAGIC GDMP_2016_pv_1000m_EPSG3035 as GDMP_2016 ,
# MAGIC GDMP_2017_pv_1000m_EPSG3035 as GDMP_2017 ,
# MAGIC GDMP_2018_pv_1000m_EPSG3035 as GDMP_2018 ,
# MAGIC GDMP_2019_pv_1000m_EPSG3035 as GDMP_2019 ,
# MAGIC GDMP_2020_pv_300to1000m_EPSG3035 as GDMP_2020 ,
# MAGIC GDMP_2021_pv_300to1000m_EPSG3035 as GDMP_2021 ,
# MAGIC GDMP_2022_pv_300to1000m_EPSG3035 as GDMP_2022,
# MAGIC
# MAGIC if(GDMP_1999_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2000_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2001_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2002_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2003_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2004_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2005_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2006_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2007_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2008_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2009_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2010_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2011_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2012_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2013_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2014_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2015_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2016_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2017_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2018_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2019_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2020_pv_300to1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2021_pv_300to1000m_EPSG3035= 0 , 1 , 
# MAGIC if(GDMP_2022_pv_300to1000m_EPSG3035= 0 , 1 , 
# MAGIC 0))))))))))))))))))))))))
# MAGIC    as QC_gap_YES
# MAGIC     from gdmp_1km_pv_raw  
# MAGIC """)  
# MAGIC GDMP_1km_99_22.createOrReplaceTempView("GDMP_1km_99_22")  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 14 (GDMP 100m  2014-2022)  100m -- ############################## 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2016&fileId=1038
# MAGIC //  absolute value and standard deviation The GDMP_annual is expressed in kg DM/ha    (DM= dry matter)
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_100m_1038_2023731_100m
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_gdmp_100m = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_100m_1038_2023731_100m/")
# MAGIC parquetFileDF_gdmp_100m.createOrReplaceTempView("GDMP_100m_14_22_raw")
# MAGIC
# MAGIC // if the attribute is 1, then this row should not be used for statistics OR a gab filling should be done:
# MAGIC
# MAGIC val parquetFileDF_gdmp_100m_2 = spark.sql(""" 
# MAGIC    Select
# MAGIC       gridnum,
# MAGIC       GridNum10km,
# MAGIC       x,
# MAGIC       y,
# MAGIC       AreaHa,
# MAGIC       GDMP_2014_pv_100m_EPSG3035 as GDMP_2014 ,
# MAGIC       GDMP_2015_pv_100m_EPSG3035 as GDMP_2015 ,
# MAGIC       GDMP_2016_pv_100m_EPSG3035 as GDMP_2016 ,
# MAGIC       GDMP_2017_pv_100m_EPSG3035 as GDMP_2017 ,
# MAGIC       GDMP_2018_pv_100m_EPSG3035 as GDMP_2018 ,
# MAGIC       GDMP_2019_pv_100m_EPSG3035 as GDMP_2019 ,
# MAGIC       GDMP_2020_pv_100m_EPSG3035 as GDMP_2020 ,
# MAGIC       GDMP_2021_pv_100m_EPSG3035 as GDMP_2021 ,
# MAGIC       GDMP_2022_pv_100m_EPSG3035 as GDMP_2022 ,
# MAGIC
# MAGIC       if(GDMP_2014_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2015_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2016_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2017_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2018_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2019_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2020_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2021_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2022_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       0)))))))))
# MAGIC          as QC_gap_YES
# MAGIC          from GDMP_100m_14_22_raw
# MAGIC                                                       """)                                  
# MAGIC parquetFileDF_gdmp_100m_2.createOrReplaceTempView("GDMP_100m_14_22")  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md #### 1. 8 GDMP statistics

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //// 14 (GDMP 1km  STATISTICS 1999-2022)  1km-- ############################## 1000m DIM
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km_statistics = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km/")
# MAGIC
# MAGIC parquetFileDF_gdmp_1km_statistics.createOrReplaceTempView("GDMP_1km_99_22_stats")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 14 (GDMP 300m  STATISTICS 2014-2022)  100m-- ############################## 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2017&fileId=1039
# MAGIC //  
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_100m_statistics_1039_202381_100m
# MAGIC
# MAGIC val parquetFileDF_gdmp_100m = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_100m_statistics_1039_202381_100m/")
# MAGIC parquetFileDF_gdmp_100m.createOrReplaceTempView("GDMP_100m_statistics")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val parquetFileDF_GDMP_collection= spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC GDMP_1km_99_22_stats.gridnum as gridnum_1km
# MAGIC ,GDMP_1km_99_22_stats.GridNum10km
# MAGIC ,GDMP_1km_99_22_stats.AreaHa
# MAGIC  
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_1999
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2000
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2001
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2002
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2003
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2004
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2005
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2006
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2007
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2008
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2009
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2010
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2011
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2012
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2013
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2014
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2015
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2016
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2017
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2018
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2019
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2020
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2021
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_anom_2022
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_1999
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2000
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2001
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2002
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2003
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2004
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2005
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2006
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2007
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2008
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2009
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2010
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2011
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2012
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2013
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2014
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2015
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2016
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2017
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2018
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2019
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2020
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2021
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_deviation_2022
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_mean
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_pvalue
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_slope
# MAGIC ,GDMP_1km_99_22_stats.GDMP_1km_std
# MAGIC
# MAGIC ---GDMP_1km_99_22.GridNum1km
# MAGIC ----,GDMP_1km_99_22.GridNum10km
# MAGIC ----,GDMP_1km_99_22.x
# MAGIC ----,GDMP_1km_99_22.y
# MAGIC ----,GDMP_1km_99_22.AreaHa
# MAGIC ,GDMP_1km_99_22.GDMP_1999
# MAGIC ,GDMP_1km_99_22.GDMP_2000
# MAGIC ,GDMP_1km_99_22.GDMP_2001
# MAGIC ,GDMP_1km_99_22.GDMP_2002
# MAGIC ,GDMP_1km_99_22.GDMP_2003
# MAGIC ,GDMP_1km_99_22.GDMP_2004
# MAGIC ,GDMP_1km_99_22.GDMP_2005
# MAGIC ,GDMP_1km_99_22.GDMP_2006
# MAGIC ,GDMP_1km_99_22.GDMP_2007
# MAGIC ,GDMP_1km_99_22.GDMP_2008
# MAGIC ,GDMP_1km_99_22.GDMP_2009
# MAGIC ,GDMP_1km_99_22.GDMP_2010
# MAGIC ,GDMP_1km_99_22.GDMP_2011
# MAGIC ,GDMP_1km_99_22.GDMP_2012
# MAGIC ,GDMP_1km_99_22.GDMP_2013
# MAGIC ,GDMP_1km_99_22.GDMP_2014
# MAGIC ,GDMP_1km_99_22.GDMP_2015
# MAGIC ,GDMP_1km_99_22.GDMP_2016
# MAGIC ,GDMP_1km_99_22.GDMP_2017
# MAGIC ,GDMP_1km_99_22.GDMP_2018
# MAGIC ,GDMP_1km_99_22.GDMP_2019
# MAGIC ,GDMP_1km_99_22.GDMP_2020
# MAGIC ,GDMP_1km_99_22.GDMP_2021
# MAGIC ,GDMP_1km_99_22.GDMP_2022
# MAGIC
# MAGIC
# MAGIC  from GDMP_1km_99_22_stats left join GDMP_1km_99_22 on 
# MAGIC         GDMP_1km_99_22_stats.gridnum = GDMP_1km_99_22.GridNum1km
# MAGIC
# MAGIC                                                       """)                                  
# MAGIC parquetFileDF_GDMP_collection.createOrReplaceTempView("GDMP_collection_1km") 

# COMMAND ----------

# MAGIC %md #### 1. 9 Wildfire burnt carbon
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC //#https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1988&fileId=1010
# MAGIC //Spatial resolution: 10 km (resampled from original resolution 0.1º after reprojection)
# MAGIC //Units: Tn of burnt carbon
# MAGIC
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_Wildfire_burntcarbon_1010_2023626_10km  -----10km
# MAGIC val parquetFileDF_gdmp_1km = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_Wildfire_burntcarbon_1010_2023626_10km/")
# MAGIC parquetFileDF_gdmp_1km.createOrReplaceTempView("burnt_carbon_10km")
# MAGIC

# COMMAND ----------

# MAGIC %md #### 1. 10 LULUCF reporting categories
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (1) CLC and LUT-clc for lULUCF classes ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the lULUCF classes:
# MAGIC // Reading CLC2018 100m DIM:.....
# MAGIC val parquetFileDF_clc18 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_A_CLC_18_210_20181129_100m/")
# MAGIC parquetFileDF_clc18.createOrReplaceTempView("CLC_2018")
# MAGIC
# MAGIC // Reading the LUT for CLC...:
# MAGIC val lut_clc  = spark.read.format("csv")
# MAGIC .options(Map("delimiter"->","))
# MAGIC  .option("header", "true")
# MAGIC    .load("dbfs:/mnt/trainingDatabricks/LookupTablesFiles/Corine_Land_Cover_LUT_JEDI_4.csv")     ////------Lookup_CLC_07112022_4.csv   Lookup_CLC_24032021_4.csv
# MAGIC lut_clc.createOrReplaceTempView("LUT_clc_classes")
# MAGIC // Construction of a new table: with lULUCF level 1 classes bases on CLC2018 100m:...................
# MAGIC val lULUCF_sq1 = spark.sql(""" 
# MAGIC                    SELECT                
# MAGIC                   CLC_2018.GridNum,
# MAGIC                   CLC_2018.GridNum10km,                     
# MAGIC                   ---CONCAT('MAES_',LUT_clc_classes.MAES_CODE) as MAES_CODE ,   
# MAGIC                   LULUCF_CODE,   
# MAGIC                   LULUCF_DESCRIPTION,     
# MAGIC                   CLC_2018.AreaHa
# MAGIC from CLC_2018   
# MAGIC                   LEFT JOIN   LUT_clc_classes  
# MAGIC                      ON  CLC_2018.Category  = LUT_clc_classes.LEVEL3_CODE where AreaHa = 1                                 
# MAGIC                                                         """)                                  
# MAGIC lULUCF_sq1.createOrReplaceTempView("lULUCF_2018")  
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2 Building SUB-CUBES-TABLES
# MAGIC
# MAGIC Here we join all three DIMS via the reference units and years and export the statistics.
# MAGIC

# COMMAND ----------

# MAGIC %md ### 2.1 S-CUBE 1 GDMP and Wildfires & all dims (NUTS, PA, MAES, EnvZones & LCF)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //  sub-cube 1: 
# MAGIC
# MAGIC val S_cube1 = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC       nuts3_2021.GridNum1km
# MAGIC       ,nuts3_2021.ADM_ID
# MAGIC       ,nuts3_2021.ISO2
# MAGIC
# MAGIC       ,nuts3_2021.AreaHa----- use this area
# MAGIC       
# MAGIC       ,PA2022.protected_area
# MAGIC       ,EnvZones.Category as env_zones
# MAGIC       ,lULUCF_2018.LULUCF_CODE
# MAGIC       ,lULUCF_2018.LULUCF_DESCRIPTION
# MAGIC
# MAGIC       ,fire_2000
# MAGIC       ,fire_2001
# MAGIC       ,fire_2002
# MAGIC       ,fire_2003
# MAGIC       ,fire_2004
# MAGIC       ,fire_2005
# MAGIC       ,fire_2006
# MAGIC       ,fire_2007
# MAGIC       ,fire_2008
# MAGIC       ,fire_2009
# MAGIC       ,fire_2010
# MAGIC       ,fire_2011
# MAGIC       ,fire_2012
# MAGIC       ,fire_2013
# MAGIC       ,fire_2014
# MAGIC       ,fire_2015
# MAGIC       ,fire_2016
# MAGIC       ,fire_2017
# MAGIC       ,fire_2018
# MAGIC       ,fire_2019
# MAGIC       ,fire_2020
# MAGIC       ,fire_2021
# MAGIC       ,fire_2022
# MAGIC       
# MAGIC  ,GDMP_collection_1km.gridnum_1km
# MAGIC ,GDMP_collection_1km.GridNum10km
# MAGIC ---- ,GDMP_collection_1km.AreaHa --not use this
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_1999
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2000
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2001
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2002
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2003
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2004
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2005
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2006
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2007
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2008
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2009
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2010
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2011
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2012
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2013
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2014
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2015
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2016
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2017
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2018
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2019
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2020
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2021
# MAGIC ,GDMP_collection_1km.GDMP_1km_anom_2022
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_1999
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2000
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2001
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2002
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2003
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2004
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2005
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2006
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2007
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2008
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2009
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2010
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2011
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2012
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2013
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2014
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2015
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2016
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2017
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2018
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2019
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2020
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2021
# MAGIC ,GDMP_collection_1km.GDMP_1km_deviation_2022
# MAGIC ,GDMP_collection_1km.GDMP_1km_mean
# MAGIC ,GDMP_collection_1km.GDMP_1km_pvalue
# MAGIC ,GDMP_collection_1km.GDMP_1km_slope
# MAGIC ,GDMP_collection_1km.GDMP_1km_std
# MAGIC ,GDMP_collection_1km.GDMP_1999
# MAGIC ,GDMP_collection_1km.GDMP_2000
# MAGIC ,GDMP_collection_1km.GDMP_2001
# MAGIC ,GDMP_collection_1km.GDMP_2002
# MAGIC ,GDMP_collection_1km.GDMP_2003
# MAGIC ,GDMP_collection_1km.GDMP_2004
# MAGIC ,GDMP_collection_1km.GDMP_2005
# MAGIC ,GDMP_collection_1km.GDMP_2006
# MAGIC ,GDMP_collection_1km.GDMP_2007
# MAGIC ,GDMP_collection_1km.GDMP_2008
# MAGIC ,GDMP_collection_1km.GDMP_2009
# MAGIC ,GDMP_collection_1km.GDMP_2010
# MAGIC ,GDMP_collection_1km.GDMP_2011
# MAGIC ,GDMP_collection_1km.GDMP_2012
# MAGIC ,GDMP_collection_1km.GDMP_2013
# MAGIC ,GDMP_collection_1km.GDMP_2014
# MAGIC ,GDMP_collection_1km.GDMP_2015
# MAGIC ,GDMP_collection_1km.GDMP_2016
# MAGIC ,GDMP_collection_1km.GDMP_2017
# MAGIC ,GDMP_collection_1km.GDMP_2018
# MAGIC ,GDMP_collection_1km.GDMP_2019
# MAGIC ,GDMP_collection_1km.GDMP_2020
# MAGIC ,GDMP_collection_1km.GDMP_2021
# MAGIC ,GDMP_collection_1km.GDMP_2022
# MAGIC
# MAGIC FROM nuts3_2021
# MAGIC
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON nuts3_2021.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON nuts3_2021.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN fire_year       ON nuts3_2021.GridNum    = fire_year.GridNum    
# MAGIC   LEFT JOIN PA2022          ON nuts3_2021.GridNum     = PA2022.GridNum    
# MAGIC   LEFT JOIN lULUCF_2018     ON nuts3_2021.GridNum = lULUCF_2018.GridNum 
# MAGIC   LEFT JOIN GDMP_collection_1km     ON nuts3_2021.GridNum1km = GDMP_collection_1km.GridNum_1km
# MAGIC     
# MAGIC
# MAGIC    """)
# MAGIC S_cube1.createOrReplaceTempView("S_cube1")
# MAGIC

# COMMAND ----------

# MAGIC %md ### 2.2 CUBE with all DIMS needed
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql --- Build CUBE cube TEST
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC       nuts3_2021.ADM_ID
# MAGIC       ,nuts3_2021.ISO2
# MAGIC       ,nuts3_2021.GridNum10km
# MAGIC       ,PA2022.protected_area
# MAGIC       ,EnvZones.Category as env_zones
# MAGIC       ,lULUCF_2018.LULUCF_CODE
# MAGIC       ,lULUCF_2018.LULUCF_DESCRIPTION
# MAGIC
# MAGIC       ,SUM(fire_2000) as fire_2000
# MAGIC       ,SUM(fire_2001) as fire_2001
# MAGIC       ,SUM(fire_2002) as fire_2002
# MAGIC       ,SUM(fire_2003) as fire_2003
# MAGIC       ,SUM(fire_2004) as fire_2004
# MAGIC       ,SUM(fire_2005) as fire_2005
# MAGIC       ,SUM(fire_2006) as fire_2006
# MAGIC       ,SUM(fire_2007) as fire_2007
# MAGIC       ,SUM(fire_2008) as fire_2008
# MAGIC       ,SUM(fire_2009) as fire_2009
# MAGIC       ,SUM(fire_2010) as fire_2010
# MAGIC       ,SUM(fire_2011) as fire_2011
# MAGIC       ,SUM(fire_2012) as fire_2012
# MAGIC       ,SUM(fire_2013) as fire_2013
# MAGIC       ,SUM(fire_2014) as fire_2014
# MAGIC       ,SUM(fire_2015) as fire_2015
# MAGIC       ,SUM(fire_2016) as fire_2016
# MAGIC       ,SUM(fire_2017) as fire_2017
# MAGIC       ,SUM(fire_2018) as fire_2018
# MAGIC       ,SUM(fire_2019) as fire_2019
# MAGIC       ,SUM(fire_2020) as fire_2020
# MAGIC       ,SUM(fire_2021) as fire_2021
# MAGIC       ,SUM(fire_2022) as fire_2022
# MAGIC
# MAGIC  --,GDMP_collection_1km.gridnum_1km
# MAGIC ---,GDMP_collection_1km.GridNum10km
# MAGIC ,sum(nuts3_2021.AreaHa) as AreaHa
# MAGIC
# MAGIC
# MAGIC --- weighted avg: 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_1999
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2000*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2000
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2001*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2001
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2002*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2002
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2003*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2003
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2004*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2004
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2005*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2005
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2006*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2006
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2007*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2007
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2008*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2008
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2009*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2009
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2010*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2010
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2011*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2011
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2012*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2012
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2013*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2013
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2014*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2014
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2015*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2015
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2016*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2016
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2017*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2017
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2018*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2018
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2019*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2019
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2020*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2020
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2021*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2021
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2022*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2022
# MAGIC
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_1999 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2000*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2000 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2001*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2001 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2002*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2002 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2003*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2003 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2004*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2004 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2005*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2005 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2006*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2006 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2007*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2007 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2008*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2008 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2009*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2009 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2010*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2010 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2011*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2011 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2012*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2012 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2013*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2013 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2014*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2014 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2015*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2015 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2016*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2016 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2017*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2017 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2018*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2018 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2019*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2019 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2020*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2020 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2021*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2021 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2022*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2022 
# MAGIC
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_mean*  GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_mean
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_pvalue*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_pvalue
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_slope *GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_slope
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_std   *GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_std
# MAGIC
# MAGIC
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1999) as GDMP_1999
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2000) as GDMP_2000
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2001) as GDMP_2001
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2002) as GDMP_2002
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2003) as GDMP_2003
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2004) as GDMP_2004
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2005) as GDMP_2005
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2006) as GDMP_2006
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2007) as GDMP_2007
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2008) as GDMP_2008
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2009) as GDMP_2009
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2010) as GDMP_2010
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2011) as GDMP_2011
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2012) as GDMP_2012
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2013) as GDMP_2013
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2014) as GDMP_2014
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2015) as GDMP_2015
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2016) as GDMP_2016
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2017) as GDMP_2017
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2018) as GDMP_2018
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2019) as GDMP_2019
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2020) as GDMP_2020
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2021) as GDMP_2021
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2022) as GDMP_2022
# MAGIC
# MAGIC FROM nuts3_2021
# MAGIC   LEFT JOIN EnvZones        ON nuts3_2021.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN fire_year       ON nuts3_2021.GridNum    = fire_year.GridNum    
# MAGIC   LEFT JOIN PA2022          ON nuts3_2021.GridNum     = PA2022.GridNum    
# MAGIC   LEFT JOIN lULUCF_2018     ON nuts3_2021.GridNum = lULUCF_2018.GridNum 
# MAGIC   LEFT JOIN GDMP_collection_1km     ON nuts3_2021.GridNum1km = GDMP_collection_1km.GridNum_1km
# MAGIC
# MAGIC group by
# MAGIC
# MAGIC
# MAGIC        nuts3_2021.ADM_ID
# MAGIC       ,nuts3_2021.ISO2
# MAGIC       ,nuts3_2021.GridNum10km
# MAGIC       ,PA2022.protected_area
# MAGIC       ,EnvZones.Category 
# MAGIC       ,lULUCF_2018.LULUCF_CODE
# MAGIC       ,lULUCF_2018.LULUCF_DESCRIPTION
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC //  sub-cube 1: 
# MAGIC
# MAGIC val cube_1_fire_gdmp = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC       nuts3_2021.ADM_ID
# MAGIC       ,nuts3_2021.ISO2
# MAGIC       ,nuts3_2021.GridNum10km
# MAGIC       ,sum(nuts3_2021.AreaHa) as AreaHa
# MAGIC
# MAGIC       ,PA2022.protected_area
# MAGIC       ,EnvZones.Category as env_zones
# MAGIC       ,lULUCF_2018.LULUCF_CODE
# MAGIC       ,lULUCF_2018.LULUCF_DESCRIPTION
# MAGIC
# MAGIC       ,SUM(fire_2000) as fire_2000
# MAGIC       ,SUM(fire_2001) as fire_2001
# MAGIC       ,SUM(fire_2002) as fire_2002
# MAGIC       ,SUM(fire_2003) as fire_2003
# MAGIC       ,SUM(fire_2004) as fire_2004
# MAGIC       ,SUM(fire_2005) as fire_2005
# MAGIC       ,SUM(fire_2006) as fire_2006
# MAGIC       ,SUM(fire_2007) as fire_2007
# MAGIC       ,SUM(fire_2008) as fire_2008
# MAGIC       ,SUM(fire_2009) as fire_2009
# MAGIC       ,SUM(fire_2010) as fire_2010
# MAGIC       ,SUM(fire_2011) as fire_2011
# MAGIC       ,SUM(fire_2012) as fire_2012
# MAGIC       ,SUM(fire_2013) as fire_2013
# MAGIC       ,SUM(fire_2014) as fire_2014
# MAGIC       ,SUM(fire_2015) as fire_2015
# MAGIC       ,SUM(fire_2016) as fire_2016
# MAGIC       ,SUM(fire_2017) as fire_2017
# MAGIC       ,SUM(fire_2018) as fire_2018
# MAGIC       ,SUM(fire_2019) as fire_2019
# MAGIC       ,SUM(fire_2020) as fire_2020
# MAGIC       ,SUM(fire_2021) as fire_2021
# MAGIC       ,SUM(fire_2022) as fire_2022
# MAGIC
# MAGIC  --,GDMP_collection_1km.gridnum_1km
# MAGIC ---,GDMP_collection_1km.GridNum10km
# MAGIC
# MAGIC
# MAGIC --- weighted avg: 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_1999
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2000*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2000
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2001*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2001
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2002*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2002
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2003*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2003
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2004*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2004
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2005*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2005
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2006*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2006
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2007*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2007
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2008*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2008
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2009*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2009
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2010*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2010
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2011*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2011
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2012*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2012
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2013*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2013
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2014*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2014
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2015*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2015
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2016*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2016
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2017*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2017
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2018*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2018
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2019*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2019
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2020*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2020
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2021*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2021
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_anom_2022*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_2022
# MAGIC
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_1999 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2000*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2000 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2001*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2001 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2002*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2002 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2003*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2003 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2004*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2004 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2005*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2005 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2006*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2006 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2007*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2007 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2008*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2008 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2009*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2009 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2010*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2010 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2011*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2011 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2012*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2012 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2013*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2013 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2014*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2014 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2015*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2015 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2016*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2016 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2017*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2017 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2018*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2018 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2019*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2019 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2020*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2020 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2021*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2021 
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_deviation_2022*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_deviation_2022 
# MAGIC
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_mean*  GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_mean
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_pvalue*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_pvalue
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_slope *GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_slope
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1km_std   *GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_std
# MAGIC
# MAGIC
# MAGIC ,SUM(GDMP_collection_1km.GDMP_1999) as GDMP_1999
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2000) as GDMP_2000
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2001) as GDMP_2001
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2002) as GDMP_2002
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2003) as GDMP_2003
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2004) as GDMP_2004
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2005) as GDMP_2005
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2006) as GDMP_2006
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2007) as GDMP_2007
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2008) as GDMP_2008
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2009) as GDMP_2009
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2010) as GDMP_2010
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2011) as GDMP_2011
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2012) as GDMP_2012
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2013) as GDMP_2013
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2014) as GDMP_2014
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2015) as GDMP_2015
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2016) as GDMP_2016
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2017) as GDMP_2017
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2018) as GDMP_2018
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2019) as GDMP_2019
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2020) as GDMP_2020
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2021) as GDMP_2021
# MAGIC ,SUM(GDMP_collection_1km.GDMP_2022) as GDMP_2022
# MAGIC
# MAGIC FROM nuts3_2021
# MAGIC   LEFT JOIN EnvZones        ON nuts3_2021.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN fire_year       ON nuts3_2021.GridNum    = fire_year.GridNum    
# MAGIC   LEFT JOIN PA2022          ON nuts3_2021.GridNum     = PA2022.GridNum    
# MAGIC   LEFT JOIN lULUCF_2018     ON nuts3_2021.GridNum = lULUCF_2018.GridNum 
# MAGIC   LEFT JOIN GDMP_collection_1km     ON nuts3_2021.GridNum1km = GDMP_collection_1km.GridNum_1km
# MAGIC
# MAGIC group by
# MAGIC
# MAGIC
# MAGIC        nuts3_2021.ADM_ID
# MAGIC       ,nuts3_2021.ISO2
# MAGIC       ,nuts3_2021.GridNum10km
# MAGIC       ,PA2022.protected_area
# MAGIC       ,EnvZones.Category 
# MAGIC       ,lULUCF_2018.LULUCF_CODE
# MAGIC       ,lULUCF_2018.LULUCF_DESCRIPTION
# MAGIC
# MAGIC
# MAGIC     
# MAGIC
# MAGIC    """)
# MAGIC ////cube_1_fire_gdmp.createOrReplaceTempView("cube_1_fire_gdmp")
# MAGIC
# MAGIC cube_1_fire_gdmp
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_1_fire_gdmp")
# MAGIC
# MAGIC cube_1_fire_gdmp.createOrReplaceTempView("cube_1_fire_gdmp")

# COMMAND ----------

	### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_1_fire_gdmp"
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
# MAGIC --- QC...of the "mother table:"
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC GridNum10km,
# MAGIC sum(AreaHa) as Areaha
# MAGIC FROM cube_1_fire_gdmp
# MAGIC GROUP BY GridNum10km
# MAGIC HAVING
# MAGIC     sum(AreaHa) >10000
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql ---- Collection of all attributes: which in the next 
# MAGIC Select 
# MAGIC
# MAGIC  ADM_ID
# MAGIC ,ISO2
# MAGIC ,GridNum10km
# MAGIC ,protected_area
# MAGIC ,env_zones
# MAGIC ,LULUCF_CODE
# MAGIC ,LULUCF_DESCRIPTION
# MAGIC ,fire_2000
# MAGIC ,fire_2001
# MAGIC ,fire_2002
# MAGIC ,fire_2003
# MAGIC ,fire_2004
# MAGIC ,fire_2005
# MAGIC ,fire_2006
# MAGIC ,fire_2007
# MAGIC ,fire_2008
# MAGIC ,fire_2009
# MAGIC ,fire_2010
# MAGIC ,fire_2011
# MAGIC ,fire_2012
# MAGIC ,fire_2013
# MAGIC ,fire_2014
# MAGIC ,fire_2015
# MAGIC ,fire_2016
# MAGIC ,fire_2017
# MAGIC ,fire_2018
# MAGIC ,fire_2019
# MAGIC ,fire_2020
# MAGIC ,fire_2021
# MAGIC ,fire_2022
# MAGIC ,AreaHa
# MAGIC ,GDMP_1km_anom_1999
# MAGIC ,GDMP_1km_anom_2000
# MAGIC ,GDMP_1km_anom_2001
# MAGIC ,GDMP_1km_anom_2002
# MAGIC ,GDMP_1km_anom_2003
# MAGIC ,GDMP_1km_anom_2004
# MAGIC ,GDMP_1km_anom_2005
# MAGIC ,GDMP_1km_anom_2006
# MAGIC ,GDMP_1km_anom_2007
# MAGIC ,GDMP_1km_anom_2008
# MAGIC ,GDMP_1km_anom_2009
# MAGIC ,GDMP_1km_anom_2010
# MAGIC ,GDMP_1km_anom_2011
# MAGIC ,GDMP_1km_anom_2012
# MAGIC ,GDMP_1km_anom_2013
# MAGIC ,GDMP_1km_anom_2014
# MAGIC ,GDMP_1km_anom_2015
# MAGIC ,GDMP_1km_anom_2016
# MAGIC ,GDMP_1km_anom_2017
# MAGIC ,GDMP_1km_anom_2018
# MAGIC ,GDMP_1km_anom_2019
# MAGIC ,GDMP_1km_anom_2020
# MAGIC ,GDMP_1km_anom_2021
# MAGIC ,GDMP_1km_anom_2022
# MAGIC ,GDMP_1km_deviation_1999
# MAGIC ,GDMP_1km_deviation_2000
# MAGIC ,GDMP_1km_deviation_2001
# MAGIC ,GDMP_1km_deviation_2002
# MAGIC ,GDMP_1km_deviation_2003
# MAGIC ,GDMP_1km_deviation_2004
# MAGIC ,GDMP_1km_deviation_2005
# MAGIC ,GDMP_1km_deviation_2006
# MAGIC ,GDMP_1km_deviation_2007
# MAGIC ,GDMP_1km_deviation_2008
# MAGIC ,GDMP_1km_deviation_2009
# MAGIC ,GDMP_1km_deviation_2010
# MAGIC ,GDMP_1km_deviation_2011
# MAGIC ,GDMP_1km_deviation_2012
# MAGIC ,GDMP_1km_deviation_2013
# MAGIC ,GDMP_1km_deviation_2014
# MAGIC ,GDMP_1km_deviation_2015
# MAGIC ,GDMP_1km_deviation_2016
# MAGIC ,GDMP_1km_deviation_2017
# MAGIC ,GDMP_1km_deviation_2018
# MAGIC ,GDMP_1km_deviation_2019
# MAGIC ,GDMP_1km_deviation_2020
# MAGIC ,GDMP_1km_deviation_2021
# MAGIC ,GDMP_1km_deviation_2022
# MAGIC ,GDMP_1km_mean
# MAGIC ,GDMP_1km_pvalue
# MAGIC ,GDMP_1km_slope
# MAGIC ,GDMP_1km_std
# MAGIC ,GDMP_1999
# MAGIC ,GDMP_2000
# MAGIC ,GDMP_2001
# MAGIC ,GDMP_2002
# MAGIC ,GDMP_2003
# MAGIC ,GDMP_2004
# MAGIC ,GDMP_2005
# MAGIC ,GDMP_2006
# MAGIC ,GDMP_2007
# MAGIC ,GDMP_2008
# MAGIC ,GDMP_2009
# MAGIC ,GDMP_2010
# MAGIC ,GDMP_2011
# MAGIC ,GDMP_2012
# MAGIC ,GDMP_2013
# MAGIC ,GDMP_2014
# MAGIC ,GDMP_2015
# MAGIC ,GDMP_2016
# MAGIC ,GDMP_2017
# MAGIC ,GDMP_2018
# MAGIC ,GDMP_2019
# MAGIC ,GDMP_2020
# MAGIC ,GDMP_2021
# MAGIC ,GDMP_2022
# MAGIC
# MAGIC  from cube_1_fire_gdmp

# COMMAND ----------

# MAGIC %md ## 3 Import CUBE into dataframe (pandas)

# COMMAND ----------


# bring sql to pandas>
# (1) FIRE

sql_for_panda = spark.sql('''
Select  
        ADM_ID
        ,ISO2
        ,GridNum10km
        ,protected_area
        ,env_zones
        ,LULUCF_CODE
        ,LULUCF_DESCRIPTION
        ,fire_2000
        ,fire_2001
        ,fire_2002
        ,fire_2003
        ,fire_2004
        ,fire_2005
        ,fire_2006
        ,fire_2007
        ,fire_2008
        ,fire_2009
        ,fire_2010
        ,fire_2011
        ,fire_2012
        ,fire_2013
        ,fire_2014
        ,fire_2015
        ,fire_2016
        ,fire_2017
        ,fire_2018
        ,fire_2019
        ,fire_2020
        ,fire_2021
        ,fire_2022
        ,AreaHa
 from cube_1_fire_gdmp
''')

df_fire = sql_for_panda.select("*").toPandas()

#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_fire_stat =df_fire.melt(id_vars=[
'AreaHa',
'ADM_ID',
'ISO2',
'LULUCF_DESCRIPTION',
'LULUCF_CODE',
'GridNum10km',
'protected_area',
'env_zones'
 ], var_name="year", value_name="fire_area_ha")

df_transformed_fire_stat['statistic'] = df_transformed_fire_stat['year'].str[:4]
df_transformed_fire_stat['year_link'] = df_transformed_fire_stat['year'].str[-4:]
# set NaN to 0:
df_transformed_fire_stat= df_transformed_fire_stat.fillna(0)

# dataframe to tables:
df_transformed_fire_1 = spark.createDataFrame(df_transformed_fire_stat)
df_transformed_fire_1.createOrReplaceTempView("df_transformed_DB1_fire")

# COMMAND ----------

df_transformed_fire_stat

# COMMAND ----------


# bring sql to pandas>
# (2) gdmp phy values

sql_for_panda = spark.sql('''
Select  
        ADM_ID
        ,ISO2
        ,GridNum10km
        ,protected_area
        ,env_zones
        ,LULUCF_CODE
        ,LULUCF_DESCRIPTION
       ,GDMP_1999
        ,GDMP_2000
        ,GDMP_2001
        ,GDMP_2002
        ,GDMP_2003
        ,GDMP_2004
        ,GDMP_2005
        ,GDMP_2006
        ,GDMP_2007
        ,GDMP_2008
        ,GDMP_2009
        ,GDMP_2010
        ,GDMP_2011
        ,GDMP_2012
        ,GDMP_2013
        ,GDMP_2014
        ,GDMP_2015
        ,GDMP_2016
        ,GDMP_2017
        ,GDMP_2018
        ,GDMP_2019
        ,GDMP_2020
        ,GDMP_2021
        ,GDMP_2022
        ,AreaHa
 from cube_1_fire_gdmp
''')

df_gdmp = sql_for_panda.select("*").toPandas()
#	
df_transformed_gdmp =df_gdmp.melt(id_vars=[
'AreaHa',
'ADM_ID',
'ISO2',
'LULUCF_DESCRIPTION',
'LULUCF_CODE',
'GridNum10km',
'protected_area',
'env_zones'

 ], var_name="year", value_name="gdmp_ha")


df_transformed_gdmp['statistic'] = df_transformed_gdmp['year'].str[:4]
df_transformed_gdmp['year_link'] = df_transformed_gdmp['year'].str[-4:]
# set NaN to 0:
df_transformed_gdmp= df_transformed_gdmp.fillna(0)

# dataframe to tables:
df_transformed_gdmp_1 = spark.createDataFrame(df_transformed_gdmp)
df_transformed_gdmp_1.createOrReplaceTempView("df_transformed_DB2_gdmp")

# COMMAND ----------

df_transformed_gdmp

# COMMAND ----------

# bring sql to pandas>
# (3) gdmp GDMP_1km_anom

sql_for_panda = spark.sql('''
Select 
 ADM_ID
,ISO2
,GridNum10km
,protected_area
,env_zones
,LULUCF_CODE
,LULUCF_DESCRIPTION
,AreaHa
,GDMP_1km_anom_1999
,GDMP_1km_anom_2000
,GDMP_1km_anom_2001
,GDMP_1km_anom_2002
,GDMP_1km_anom_2003
,GDMP_1km_anom_2004
,GDMP_1km_anom_2005
,GDMP_1km_anom_2006
,GDMP_1km_anom_2007
,GDMP_1km_anom_2008
,GDMP_1km_anom_2009
,GDMP_1km_anom_2010
,GDMP_1km_anom_2011
,GDMP_1km_anom_2012
,GDMP_1km_anom_2013
,GDMP_1km_anom_2014
,GDMP_1km_anom_2015
,GDMP_1km_anom_2016
,GDMP_1km_anom_2017
,GDMP_1km_anom_2018
,GDMP_1km_anom_2019
,GDMP_1km_anom_2020
,GDMP_1km_anom_2021
,GDMP_1km_anom_2022
 from cube_1_fire_gdmp
''')



# COMMAND ----------

# bring sql to pandas>
# (4) gdmp deviation
sql_for_panda = spark.sql('''
Select 
 ADM_ID
,ISO2
,GridNum10km
,protected_area
,env_zones
,LULUCF_CODE
,LULUCF_DESCRIPTION
,AreaHa
,GDMP_1km_deviation_1999
,GDMP_1km_deviation_2000
,GDMP_1km_deviation_2001
,GDMP_1km_deviation_2002
,GDMP_1km_deviation_2003
,GDMP_1km_deviation_2004
,GDMP_1km_deviation_2005
,GDMP_1km_deviation_2006
,GDMP_1km_deviation_2007
,GDMP_1km_deviation_2008
,GDMP_1km_deviation_2009
,GDMP_1km_deviation_2010
,GDMP_1km_deviation_2011
,GDMP_1km_deviation_2012
,GDMP_1km_deviation_2013
,GDMP_1km_deviation_2014
,GDMP_1km_deviation_2015
,GDMP_1km_deviation_2016
,GDMP_1km_deviation_2017
,GDMP_1km_deviation_2018
,GDMP_1km_deviation_2019
,GDMP_1km_deviation_2020
,GDMP_1km_deviation_2021
,GDMP_1km_deviation_2022

 from cube_1_fire_gdmp
''')

# COMMAND ----------

# bring sql to pandas>
# (5) gdmp trend  ?????????????????
sql_for_panda = spark.sql('''
Select 
 ADM_ID
,ISO2
,GridNum10km
,protected_area
,env_zones
,LULUCF_CODE
,LULUCF_DESCRIPTION
,AreaHa
,GDMP_1km_mean
,GDMP_1km_pvalue
,GDMP_1km_slope
,GDMP_1km_std
 from cube_1_fire_gdmp
''')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## 4  Export tables to CWS

# COMMAND ----------

# MAGIC %scala
# MAGIC //// DATA export:
# MAGIC ////  cube 1------------------------------------------------------------------------------------------------------------
# MAGIC val cube_1_fire_export = spark.sql(""" 
# MAGIC             select * from 
# MAGIC             df_transformed_DB1_fire
# MAGIC    """)
# MAGIC // Exporting the final table
# MAGIC
# MAGIC cube_1_fire_export
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_1_fire")
# MAGIC
# MAGIC
# MAGIC ////  cube 2------------------------------------------------------------------------------------------------------------
# MAGIC val cube_2_gdmp_export = spark.sql(""" 
# MAGIC             select * from 
# MAGIC             df_transformed_DB2_gdmp
# MAGIC    """)
# MAGIC // Exporting the final table
# MAGIC
# MAGIC cube_2_gdmp_export
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_2_gdmp_export")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
## CUBE 1 html link:

folder ="dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_1_fire"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# info: Manuel

