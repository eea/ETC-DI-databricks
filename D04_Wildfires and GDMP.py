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
# MAGIC //// 13 (GDMP 1km   1999-2019)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2009&fileId=1031
# MAGIC //  absolute value and standard deviation The GDMP_annual is expressed in kg DM/ha    (DM= dry matter)
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_1031_2023724_1km
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_1031_2023724_1km/")
# MAGIC parquetFileDF_gdmp_1km.createOrReplaceTempView("GDMP_1km_99_19_raw")
# MAGIC
# MAGIC // we found GAPs in the time-series.. therefore we add. an attribute which shows the gaps [QC_gap_YES]
# MAGIC // if the attribute is 1, then this row should not be used for statistics OR a gab filling should be done:
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km_2 = spark.sql(""" 
# MAGIC
# MAGIC Select
# MAGIC gridnum,
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
# MAGIC 0)))))))))))))))))))))
# MAGIC    as QC_gap_YES
# MAGIC     from GDMP_1km_99_19_raw  
# MAGIC                                                   
# MAGIC                                                         """)                                  
# MAGIC parquetFileDF_gdmp_1km_2.createOrReplaceTempView("GDMP_1km_99_19")  
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
# MAGIC //// 13 (GDMP 1km  STATISTICS 1999-2019)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2013&fileId=1035
# MAGIC //  )
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_GDMP_1km_statistics_1035_2023727_1km
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_GDMP_1km_statistics_1035_2023727_1km/")
# MAGIC parquetFileDF_gdmp_1km.createOrReplaceTempView("GDMP_1km_statistics")
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
# MAGIC                   from CLC_2018   
# MAGIC                   LEFT JOIN   LUT_clc_classes  
# MAGIC                      ON  CLC_2018.Category  = LUT_clc_classes.LEVEL3_CODE where AreaHa = 1                                 
# MAGIC                                                         """)                                  
# MAGIC lULUCF_sq1.createOrReplaceTempView("lULUCF_2018")  
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2 Join Data (Approach 1 - by reference unit)
# MAGIC Here we join all three DIMS via the reference units and years and export the statistics.
# MAGIC

# COMMAND ----------

# MAGIC %md ### 2.1 Wildfires & all dims (NUTS, PA, MAES, EnvZones & LCF)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire sub-cube: 
# MAGIC
# MAGIC val wild_fire_sub_cube_1 = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as Fire_area_ha,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       PA2022.protected_area,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC       sum(fire_2000) as fire_2000,
# MAGIC       sum(fire_2001) as fire_2001,
# MAGIC       sum(fire_2002) as fire_2002,
# MAGIC       sum(fire_2003) as fire_2003,
# MAGIC       sum(fire_2004) as fire_2004,
# MAGIC       sum(fire_2005) as fire_2005,
# MAGIC       sum(fire_2006) as fire_2006,
# MAGIC       sum(fire_2007) as fire_2007,
# MAGIC       sum(fire_2008) as fire_2008,
# MAGIC       sum(fire_2009) as fire_2009,
# MAGIC       sum(fire_2010) as fire_2010,
# MAGIC       sum(fire_2011) as fire_2011,
# MAGIC       sum(fire_2012) as fire_2012,
# MAGIC       sum(fire_2013) as fire_2013,
# MAGIC       sum(fire_2014) as fire_2014,
# MAGIC       sum(fire_2015) as fire_2015,
# MAGIC       sum(fire_2016) as fire_2016,
# MAGIC       sum(fire_2017) as fire_2017,
# MAGIC       sum(fire_2018) as fire_2018,
# MAGIC       sum(fire_2019) as fire_2019,
# MAGIC       sum(fire_2020) as fire_2020,
# MAGIC       sum(fire_2021) as fire_2021,
# MAGIC       sum(fire_2022) as fire_2022
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC
# MAGIC   LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       PA2022.protected_area,
# MAGIC       EnvZones.Category
# MAGIC
# MAGIC     
# MAGIC
# MAGIC    """)
# MAGIC wild_fire_sub_cube_1.createOrReplaceTempView("wild_fire_sub_cube_1")
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from wild_fire_sub_cube_1
''')

df = sql_for_panda.select("*").toPandas()

df_transformed =df.melt(id_vars=['GridNum10km',
'Fire_area_ha',
'ADM_ID',
'ISO2',
'protected_area',
'MAES_CODE',
'env_zones' ], var_name="year", value_name="wild_fire")

# updapte year:
df_transformed['year_link'] = df_transformed['year'].str[-4:]

# dataframe to table:
df_transformed_gdmp_new1 = spark.createDataFrame(df_transformed)
df_transformed_gdmp_new1.createOrReplaceTempView("df_transformed_fire")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from df_transformed_fire
# MAGIC

# COMMAND ----------

# MAGIC %md ### 2.2 GDMP 1km

# COMMAND ----------

# MAGIC %md #### 2.2.1 GDMP  1km-time series & all dims (NUTS, PA, MAES, EnvZones & LCF)  ERROR!! check 1km SUM/AVG

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire GDMP 1km sub-cube:   
# MAGIC
# MAGIC /// problem: 1km gdmp should be AVG to 100m or divided by 100
# MAGIC
# MAGIC val wild_fire_GDMP_sub_cube_2 = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC  PA2022.protected_area,
# MAGIC       sum(GDMP_1999/100)as GDMP_1999,
# MAGIC       sum(GDMP_2000/100) as GDMP_2000,
# MAGIC       sum(GDMP_2001/100) as GDMP_2001,
# MAGIC       sum(GDMP_2002/100) as GDMP_2002,
# MAGIC       sum(GDMP_2003/100) as GDMP_2003,
# MAGIC       sum(GDMP_2004/100) as GDMP_2004,
# MAGIC       sum(GDMP_2005/100) as GDMP_2005,
# MAGIC       sum(GDMP_2006/100) as GDMP_2006,
# MAGIC       sum(GDMP_2007/100) as GDMP_2007,
# MAGIC       sum(GDMP_2008/100) as GDMP_2008,
# MAGIC       sum(GDMP_2009/100) as GDMP_2009,
# MAGIC       sum(GDMP_2010/100) as GDMP_2010,
# MAGIC       sum(GDMP_2011/100) as GDMP_2011,
# MAGIC       sum(GDMP_2012/100) as GDMP_2012,
# MAGIC       sum(GDMP_2013/100) as GDMP_2013,
# MAGIC       sum(GDMP_2014/100) as GDMP_2014,
# MAGIC       sum(GDMP_2015/100) as GDMP_2015,
# MAGIC       sum(GDMP_2016/100) as GDMP_2016,
# MAGIC       sum(GDMP_2017/100) as GDMP_2017,
# MAGIC       sum(GDMP_2018/100) as GDMP_2018,
# MAGIC       sum(GDMP_2019/100) as GDMP_2019
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN GDMP_1km_99_19  ON fire_year.GridNum1km = GDMP_1km_99_19.gridnum
# MAGIC     LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category,
# MAGIC      PA2022.protected_area
# MAGIC
# MAGIC    """)
# MAGIC wild_fire_GDMP_sub_cube_2.createOrReplaceTempView("wild_fire_GDMP_sub_cube_2")

# COMMAND ----------




# COMMAND ----------



# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from wild_fire_GDMP_sub_cube_2
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_gdmp =df_gdmp.melt(id_vars=['GridNum10km',
'AreaHa',
'ADM_ID',
'ISO2',
'MAES_CODE',
'protected_area',
'env_zones' ], var_name="year", value_name="gdmp")

# updapte year:
df_transformed_gdmp['year_link'] = df_transformed_gdmp['year'].str[-4:]

# dataframe to tables:
df_transformed_gdmp_new = spark.createDataFrame(df_transformed_gdmp)
df_transformed_gdmp_new.createOrReplaceTempView("df_transformed_gdmp")

# COMMAND ----------

# MAGIC %md #### 2.2.2 GDMP statistics 1km & all dims (NUTS, PA, MAES, EnvZones & LCF)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire GDMP STAT 1km sub-cube: 
# MAGIC
# MAGIC val wild_fire_GDMP_stat_sub_cube_3 = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC        PA2022.protected_area,
# MAGIC
# MAGIC MIN(GDMP_1km_anom_1999) as GDMP_1km_anom_1999_min,
# MAGIC MIN(GDMP_1km_anom_2000) as GDMP_1km_anom_2000_min,
# MAGIC MIN(GDMP_1km_anom_2001) as GDMP_1km_anom_2001_min,
# MAGIC MIN(GDMP_1km_anom_2002) as GDMP_1km_anom_2002_min,
# MAGIC MIN(GDMP_1km_anom_2003) as GDMP_1km_anom_2003_min,
# MAGIC MIN(GDMP_1km_anom_2004) as GDMP_1km_anom_2004_min,
# MAGIC MIN(GDMP_1km_anom_2005) as GDMP_1km_anom_2005_min,
# MAGIC MIN(GDMP_1km_anom_2006) as GDMP_1km_anom_2006_min,
# MAGIC MIN(GDMP_1km_anom_2007) as GDMP_1km_anom_2007_min,
# MAGIC MIN(GDMP_1km_anom_2008) as GDMP_1km_anom_2008_min,
# MAGIC MIN(GDMP_1km_anom_2009) as GDMP_1km_anom_2009_min,
# MAGIC MIN(GDMP_1km_anom_2010) as GDMP_1km_anom_2010_min,
# MAGIC MIN(GDMP_1km_anom_2011) as GDMP_1km_anom_2011_min,
# MAGIC MIN(GDMP_1km_anom_2012) as GDMP_1km_anom_2012_min,
# MAGIC MIN(GDMP_1km_anom_2013) as GDMP_1km_anom_2013_min,
# MAGIC MIN(GDMP_1km_anom_2014) as GDMP_1km_anom_2014_min,
# MAGIC MIN(GDMP_1km_anom_2015) as GDMP_1km_anom_2015_min,
# MAGIC MIN(GDMP_1km_anom_2016) as GDMP_1km_anom_2016_min,
# MAGIC MIN(GDMP_1km_anom_2017) as GDMP_1km_anom_2017_min,
# MAGIC MIN(GDMP_1km_anom_2018) as GDMP_1km_anom_2018_min,
# MAGIC MIN(GDMP_1km_anom_2019) as GDMP_1km_anom_2019_min,
# MAGIC
# MAGIC AVG(GDMP_1km_anom_1999) as GDMP_1km_anom_1999_AVG,
# MAGIC AVG(GDMP_1km_anom_2000) as GDMP_1km_anom_2000_AVG,
# MAGIC AVG(GDMP_1km_anom_2001) as GDMP_1km_anom_2001_AVG,
# MAGIC AVG(GDMP_1km_anom_2002) as GDMP_1km_anom_2002_AVG,
# MAGIC AVG(GDMP_1km_anom_2003) as GDMP_1km_anom_2003_AVG,
# MAGIC AVG(GDMP_1km_anom_2004) as GDMP_1km_anom_2004_AVG,
# MAGIC AVG(GDMP_1km_anom_2005) as GDMP_1km_anom_2005_AVG,
# MAGIC AVG(GDMP_1km_anom_2006) as GDMP_1km_anom_2006_AVG,
# MAGIC AVG(GDMP_1km_anom_2007) as GDMP_1km_anom_2007_AVG,
# MAGIC AVG(GDMP_1km_anom_2008) as GDMP_1km_anom_2008_AVG,
# MAGIC AVG(GDMP_1km_anom_2009) as GDMP_1km_anom_2009_AVG,
# MAGIC AVG(GDMP_1km_anom_2010) as GDMP_1km_anom_2010_AVG,
# MAGIC AVG(GDMP_1km_anom_2011) as GDMP_1km_anom_2011_AVG,
# MAGIC AVG(GDMP_1km_anom_2012) as GDMP_1km_anom_2012_AVG,
# MAGIC AVG(GDMP_1km_anom_2013) as GDMP_1km_anom_2013_AVG,
# MAGIC AVG(GDMP_1km_anom_2014) as GDMP_1km_anom_2014_AVG,
# MAGIC AVG(GDMP_1km_anom_2015) as GDMP_1km_anom_2015_AVG,
# MAGIC AVG(GDMP_1km_anom_2016) as GDMP_1km_anom_2016_AVG,
# MAGIC AVG(GDMP_1km_anom_2017) as GDMP_1km_anom_2017_AVG,
# MAGIC AVG(GDMP_1km_anom_2018) as GDMP_1km_anom_2018_AVG,
# MAGIC AVG(GDMP_1km_anom_2019) as GDMP_1km_anom_2019_AVG,
# MAGIC
# MAGIC MAX(GDMP_1km_anom_1999) as GDMP_1km_anom_1999_MAX,
# MAGIC MAX(GDMP_1km_anom_2000) as GDMP_1km_anom_2000_MAX,
# MAGIC MAX(GDMP_1km_anom_2001) as GDMP_1km_anom_2001_MAX,
# MAGIC MAX(GDMP_1km_anom_2002) as GDMP_1km_anom_2002_MAX,
# MAGIC MAX(GDMP_1km_anom_2003) as GDMP_1km_anom_2003_MAX,
# MAGIC MAX(GDMP_1km_anom_2004) as GDMP_1km_anom_2004_MAX,
# MAGIC MAX(GDMP_1km_anom_2005) as GDMP_1km_anom_2005_MAX,
# MAGIC MAX(GDMP_1km_anom_2006) as GDMP_1km_anom_2006_MAX,
# MAGIC MAX(GDMP_1km_anom_2007) as GDMP_1km_anom_2007_MAX,
# MAGIC MAX(GDMP_1km_anom_2008) as GDMP_1km_anom_2008_MAX,
# MAGIC MAX(GDMP_1km_anom_2009) as GDMP_1km_anom_2009_MAX,
# MAGIC MAX(GDMP_1km_anom_2010) as GDMP_1km_anom_2010_MAX,
# MAGIC MAX(GDMP_1km_anom_2011) as GDMP_1km_anom_2011_MAX,
# MAGIC MAX(GDMP_1km_anom_2012) as GDMP_1km_anom_2012_MAX,
# MAGIC MAX(GDMP_1km_anom_2013) as GDMP_1km_anom_2013_MAX,
# MAGIC MAX(GDMP_1km_anom_2014) as GDMP_1km_anom_2014_MAX,
# MAGIC MAX(GDMP_1km_anom_2015) as GDMP_1km_anom_2015_MAX,
# MAGIC MAX(GDMP_1km_anom_2016) as GDMP_1km_anom_2016_MAX,
# MAGIC MAX(GDMP_1km_anom_2017) as GDMP_1km_anom_2017_MAX,
# MAGIC MAX(GDMP_1km_anom_2018) as GDMP_1km_anom_2018_MAX,
# MAGIC MAX(GDMP_1km_anom_2019) as GDMP_1km_anom_2019_MAX
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN GDMP_1km_statistics  ON fire_year.GridNum1km = GDMP_1km_statistics.gridnum
# MAGIC  LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC        PA2022.protected_area,
# MAGIC       EnvZones.Category
# MAGIC    """)
# MAGIC wild_fire_GDMP_stat_sub_cube_3.createOrReplaceTempView("wild_fire_GDMP_STAT_sub_cube_3")

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire GDMP STAT 1km sub-cube: 
# MAGIC
# MAGIC val wild_fire_GDMP_stat_sub_cube_3 = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC        PA2022.protected_area,
# MAGIC
# MAGIC
# MAGIC
# MAGIC AVG(GDMP_1km_anom_1999) as GDMP_1km_anom_1999_AVG,
# MAGIC AVG(GDMP_1km_anom_2000) as GDMP_1km_anom_2000_AVG,
# MAGIC AVG(GDMP_1km_anom_2001) as GDMP_1km_anom_2001_AVG,
# MAGIC AVG(GDMP_1km_anom_2002) as GDMP_1km_anom_2002_AVG,
# MAGIC AVG(GDMP_1km_anom_2003) as GDMP_1km_anom_2003_AVG,
# MAGIC AVG(GDMP_1km_anom_2004) as GDMP_1km_anom_2004_AVG,
# MAGIC AVG(GDMP_1km_anom_2005) as GDMP_1km_anom_2005_AVG,
# MAGIC AVG(GDMP_1km_anom_2006) as GDMP_1km_anom_2006_AVG,
# MAGIC AVG(GDMP_1km_anom_2007) as GDMP_1km_anom_2007_AVG,
# MAGIC AVG(GDMP_1km_anom_2008) as GDMP_1km_anom_2008_AVG,
# MAGIC AVG(GDMP_1km_anom_2009) as GDMP_1km_anom_2009_AVG,
# MAGIC AVG(GDMP_1km_anom_2010) as GDMP_1km_anom_2010_AVG,
# MAGIC AVG(GDMP_1km_anom_2011) as GDMP_1km_anom_2011_AVG,
# MAGIC AVG(GDMP_1km_anom_2012) as GDMP_1km_anom_2012_AVG,
# MAGIC AVG(GDMP_1km_anom_2013) as GDMP_1km_anom_2013_AVG,
# MAGIC AVG(GDMP_1km_anom_2014) as GDMP_1km_anom_2014_AVG,
# MAGIC AVG(GDMP_1km_anom_2015) as GDMP_1km_anom_2015_AVG,
# MAGIC AVG(GDMP_1km_anom_2016) as GDMP_1km_anom_2016_AVG,
# MAGIC AVG(GDMP_1km_anom_2017) as GDMP_1km_anom_2017_AVG,
# MAGIC AVG(GDMP_1km_anom_2018) as GDMP_1km_anom_2018_AVG,
# MAGIC AVG(GDMP_1km_anom_2019) as GDMP_1km_anom_2019_AVG
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN GDMP_1km_statistics  ON fire_year.GridNum1km = GDMP_1km_statistics.gridnum
# MAGIC    LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC        PA2022.protected_area,
# MAGIC       EnvZones.Category
# MAGIC    """)
# MAGIC wild_fire_GDMP_stat_sub_cube_3.createOrReplaceTempView("wild_fire_GDMP_STAT_sub_cube_3")

# COMMAND ----------



# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from wild_fire_GDMP_STAT_sub_cube_3
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_gdmp_stat =df_gdmp.melt(id_vars=['GridNum10km',
'AreaHa',
'ADM_ID',
'ISO2',
'MAES_CODE',
'protected_area',
'env_zones' ], var_name="year", value_name="gdmp_stat")

# updapte statistics parameter:
df_transformed_gdmp_stat['statistic'] = df_transformed_gdmp_stat['year'].str[-3:]

# updapte year:
df_transformed_gdmp_stat['year_link'] = df_transformed_gdmp_stat['year'].str[-8:-4]


# dataframe to tables:
df_transformed_gdmp_new3 = spark.createDataFrame(df_transformed_gdmp_stat)
df_transformed_gdmp_new3.createOrReplaceTempView("df_transformed_gdmp_STAT")

# COMMAND ----------

# MAGIC %md #### 2.3.2 GDMP 1km statistics  (slope and p-value)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire GDMP STAT 1km sub-cube: 
# MAGIC
# MAGIC val wild_fire_GDMP_stat_sub_cube_3 = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC        PA2022.protected_area,
# MAGIC
# MAGIC AVG(GDMP_1km_slope ) as GDMP_1km_slope_avg
# MAGIC ----AVG(GDMP_1km_pvalue ) as GDMP_1km_pvalue_avg
# MAGIC
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN GDMP_1km_statistics  ON fire_year.GridNum1km = GDMP_1km_statistics.gridnum
# MAGIC  LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC        PA2022.protected_area,
# MAGIC       EnvZones.Category
# MAGIC    """)
# MAGIC wild_fire_GDMP_stat_sub_cube_3.createOrReplaceTempView("wild_fire_GDMP_STAT_slope_sub4")

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from wild_fire_GDMP_STAT_slope_sub4
''')

df_gdmp = sql_for_panda.select("*").toPandas()



df_transformed_gdmp_stat =df_gdmp.melt(id_vars=['GridNum10km',
'AreaHa',
'ADM_ID',
'ISO2',
'MAES_CODE',
'protected_area',
'env_zones' ], var_name="year", value_name="slope_avg")


# updapte year:
df_transformed_gdmp_stat['year_link'] = ''


# dataframe to tables:
df_transformed_gdmp_new3 = spark.createDataFrame(df_transformed_gdmp_stat)
df_transformed_gdmp_new3.createOrReplaceTempView("df_transformed_gdmp_STAT_SLOPE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_transformed_gdmp_STAT_SLOPE

# COMMAND ----------

# MAGIC %md ### 2.3 GMDP (300m) -100m DIM

# COMMAND ----------

# MAGIC %md #### 2.3.1 GMDP (300m) -100m DIM TIME-SERIES

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire GDMP 100m sub-cube:   GDMP_100m_14_22
# MAGIC
# MAGIC
# MAGIC val wild_fire_GDMP100m_sub_cube_2 = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC  PA2022.protected_area,
# MAGIC      
# MAGIC       sum(GDMP_2014) as GDMP_2014,
# MAGIC       sum(GDMP_2015) as GDMP_2015,
# MAGIC       sum(GDMP_2016) as GDMP_2016,
# MAGIC       sum(GDMP_2017) as GDMP_2017,
# MAGIC       sum(GDMP_2018) as GDMP_2018,
# MAGIC       sum(GDMP_2019) as GDMP_2019,
# MAGIC       sum(GDMP_2020) as GDMP_2020,
# MAGIC       sum(GDMP_2021) as GDMP_2021,
# MAGIC       sum(GDMP_2022) as GDMP_2022
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN GDMP_100m_14_22  ON fire_year.GridNum1km = GDMP_100m_14_22.gridnum
# MAGIC     LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category,
# MAGIC      PA2022.protected_area
# MAGIC
# MAGIC    """)
# MAGIC wild_fire_GDMP100m_sub_cube_2.createOrReplaceTempView("wild_fire_GDMP100m_sub_cube_2")

# COMMAND ----------


# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from wild_fire_GDMP100m_sub_cube_2
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_gdmp =df_gdmp.melt(id_vars=['GridNum10km',
'AreaHa',
'ADM_ID',
'ISO2',
'MAES_CODE',
'protected_area',
'env_zones' ], var_name="year", value_name="gdmp")

# updapte year:
df_transformed_gdmp['year_link'] = df_transformed_gdmp['year'].str[-4:]

# dataframe to tables:
df_transformed_gdmp_new = spark.createDataFrame(df_transformed_gdmp)
df_transformed_gdmp_new.createOrReplaceTempView("df_transformed_gdmp_100m")

# COMMAND ----------

# MAGIC %md #### 2.3.2 GMDP (300m) -100m DIM ANOMALY

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // wild fire GDMP STAT 100m sub-cube:   GDMP_100m_statistics
# MAGIC
# MAGIC val wild_fire_GDMP100m_stat_sub_cube_3 = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC        PA2022.protected_area,
# MAGIC
# MAGIC MIN(GDMP_100m_anom_2014) as GDMP_100m_anom_2014_min,
# MAGIC MIN(GDMP_100m_anom_2015) as GDMP_100m_anom_2015_min,
# MAGIC MIN(GDMP_100m_anom_2016) as GDMP_100m_anom_2016_min,
# MAGIC MIN(GDMP_100m_anom_2017) as GDMP_100m_anom_2017_min,
# MAGIC MIN(GDMP_100m_anom_2018) as GDMP_100m_anom_2018_min,
# MAGIC MIN(GDMP_100m_anom_2019) as GDMP_100m_anom_2019_min,
# MAGIC MIN(GDMP_100m_anom_2020) as GDMP_100m_anom_2020_min,
# MAGIC MIN(GDMP_100m_anom_2021) as GDMP_100m_anom_2021_min,
# MAGIC MIN(GDMP_100m_anom_2022) as GDMP_100m_anom_2022_min,
# MAGIC
# MAGIC
# MAGIC AVG(GDMP_100m_anom_2014) as GDMP_100m_anom_2014_AVG,
# MAGIC AVG(GDMP_100m_anom_2015) as GDMP_100m_anom_2015_AVG,
# MAGIC AVG(GDMP_100m_anom_2016) as GDMP_100m_anom_2016_AVG,
# MAGIC AVG(GDMP_100m_anom_2017) as GDMP_100m_anom_2017_AVG,
# MAGIC AVG(GDMP_100m_anom_2018) as GDMP_100m_anom_2018_AVG,
# MAGIC AVG(GDMP_100m_anom_2019) as GDMP_100m_anom_2019_AVG,
# MAGIC AVG(GDMP_100m_anom_2020) as GDMP_100m_anom_2020_AVG,
# MAGIC AVG(GDMP_100m_anom_2021) as GDMP_100m_anom_2021_AVG,
# MAGIC AVG(GDMP_100m_anom_2022) as GDMP_100m_anom_2022_AVG,
# MAGIC
# MAGIC
# MAGIC MAX(GDMP_100m_anom_2014) as GDMP_100m_anom_2014_MAX,
# MAGIC MAX(GDMP_100m_anom_2015) as GDMP_100m_anom_2015_MAX,
# MAGIC MAX(GDMP_100m_anom_2016) as GDMP_100m_anom_2016_MAX,
# MAGIC MAX(GDMP_100m_anom_2017) as GDMP_100m_anom_2017_MAX,
# MAGIC MAX(GDMP_100m_anom_2018) as GDMP_100m_anom_2018_MAX,
# MAGIC MAX(GDMP_100m_anom_2019) as GDMP_100m_anom_2019_MAX,
# MAGIC MAX(GDMP_100m_anom_2020) as GDMP_100m_anom_2020_MAX,
# MAGIC MAX(GDMP_100m_anom_2021) as GDMP_100m_anom_2021_MAX,
# MAGIC MAX(GDMP_100m_anom_2022) as GDMP_100m_anom_2022_MAX
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN GDMP_100m_statistics  ON fire_year.GridNum1km = GDMP_100m_statistics.gridnum
# MAGIC  LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC        PA2022.protected_area,
# MAGIC       EnvZones.Category
# MAGIC    """)
# MAGIC wild_fire_GDMP100m_stat_sub_cube_3.createOrReplaceTempView("wild_fire_GDMP100m_STAT_sub_cube_3")

# COMMAND ----------

# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from wild_fire_GDMP100m_STAT_sub_cube_3
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_gdmp =df_gdmp.melt(id_vars=['GridNum10km',
'AreaHa',
'ADM_ID',
'ISO2',
'MAES_CODE',
'protected_area',
'env_zones' ], var_name="year", value_name="gdmp")

# updapte year:
df_transformed_gdmp['year_link'] = df_transformed_gdmp['year'].str[15:19]

# dataframe to tables:
df_transformed_gdmp_new = spark.createDataFrame(df_transformed_gdmp)
df_transformed_gdmp_new.createOrReplaceTempView("df_transformed_gdmp_100m_STAT")

# COMMAND ----------

# MAGIC %md #### 2.3.3 GMDP (300m) -100m DIM SLOPE & p-value was NOT produced for this short time-series

# COMMAND ----------

# MAGIC %md ### 2.4 Combination GDMP 300m (100) and GDMP 1km:

# COMMAND ----------

# MAGIC %md #### 2.4.1 Compare both in on table with columns

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- Testing: 
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       nuts3_2021.GridNum10km,
# MAGIC       nuts3_2021.AreaHa,
# MAGIC       
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE,
# MAGIC
# MAGIC       GDMP_1km_99_19.GDMP_1999 / 100 as GDMP_1km_1999,
# MAGIC       GDMP_1km_99_19.GDMP_2000 / 100 as GDMP_1km_2000,
# MAGIC       GDMP_1km_99_19.GDMP_2001 / 100 as GDMP_1km_2001,
# MAGIC       GDMP_1km_99_19.GDMP_2002 / 100 as GDMP_1km_2002,
# MAGIC       GDMP_1km_99_19.GDMP_2003 / 100 as GDMP_1km_2003,
# MAGIC       GDMP_1km_99_19.GDMP_2004 / 100 as GDMP_1km_2004,
# MAGIC       GDMP_1km_99_19.GDMP_2005 / 100 as GDMP_1km_2005,
# MAGIC       GDMP_1km_99_19.GDMP_2006 / 100 as GDMP_1km_2006,
# MAGIC       GDMP_1km_99_19.GDMP_2007 / 100 as GDMP_1km_2007,
# MAGIC       GDMP_1km_99_19.GDMP_2008 / 100 as GDMP_1km_2008,
# MAGIC       GDMP_1km_99_19.GDMP_2009 / 100 as GDMP_1km_2009,
# MAGIC       GDMP_1km_99_19.GDMP_2010 / 100 as GDMP_1km_2010,
# MAGIC       GDMP_1km_99_19.GDMP_2011 / 100 as GDMP_1km_2011,
# MAGIC       GDMP_1km_99_19.GDMP_2012 / 100 as GDMP_1km_2012,
# MAGIC       GDMP_1km_99_19.GDMP_2013 / 100 as GDMP_1km_2013,
# MAGIC       GDMP_1km_99_19.GDMP_2014 / 100 as GDMP_1km_2014,
# MAGIC       GDMP_1km_99_19.GDMP_2015 / 100 as GDMP_1km_2015,
# MAGIC       GDMP_1km_99_19.GDMP_2016 / 100 as GDMP_1km_2016,
# MAGIC       GDMP_1km_99_19.GDMP_2017 / 100 as GDMP_1km_2017,
# MAGIC       GDMP_1km_99_19.GDMP_2018 / 100 as GDMP_1km_2018,
# MAGIC       GDMP_1km_99_19.GDMP_2019 / 100 as GDMP_1km_2019,
# MAGIC
# MAGIC       GDMP_100m_14_22.GDMP_2014 as GDMP_300m_2014,
# MAGIC       GDMP_100m_14_22.GDMP_2015 as GDMP_300m_2015,
# MAGIC       GDMP_100m_14_22.GDMP_2016 as GDMP_300m_2016,
# MAGIC       GDMP_100m_14_22.GDMP_2017 as GDMP_300m_2017,
# MAGIC       GDMP_100m_14_22.GDMP_2018 as GDMP_300m_2018,
# MAGIC       GDMP_100m_14_22.GDMP_2019 as GDMP_300m_2019,
# MAGIC       GDMP_100m_14_22.GDMP_2020 as GDMP_300m_2020,
# MAGIC       GDMP_100m_14_22.GDMP_2021 as GDMP_300m_2021,
# MAGIC       GDMP_100m_14_22.GDMP_2022 as GDMP_300m_2022
# MAGIC
# MAGIC  FROM nuts3_2021 
# MAGIC
# MAGIC  LEFT JOIN lULUCF_2018     on lULUCF_2018.GridNum          = nuts3_2021.GridNum
# MAGIC  LEFT JOIN GDMP_1km_99_19  ON GDMP_1km_99_19.GridNum       = nuts3_2021.GridNum1km  ---- 1km
# MAGIC  LEFT JOIN GDMP_100m_14_22  ON GDMP_100m_14_22.GridNum     = nuts3_2021.gridnum
# MAGIC  
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null and  nuts3_2021.ISO2='PT'

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // BURNT CARBON 10km sub-cube: 
# MAGIC
# MAGIC val gdmp_300_1000m_attribute = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       nuts3_2021.GridNum10km,
# MAGIC       
# MAGIC       
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE,
# MAGIC
# MAGIC       SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_1999 ) as GDMP_1km_1999,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2000 ) as GDMP_1km_2000,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2001 ) as GDMP_1km_2001,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2002 ) as GDMP_1km_2002,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2003 ) as GDMP_1km_2003,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2004 ) as GDMP_1km_2004,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2005 ) as GDMP_1km_2005,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2006 ) as GDMP_1km_2006,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2007 ) as GDMP_1km_2007,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2008 ) as GDMP_1km_2008,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2009 ) as GDMP_1km_2009,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2010 ) as GDMP_1km_2010,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2011 ) as GDMP_1km_2011,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2012 ) as GDMP_1km_2012,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2013 ) as GDMP_1km_2013,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2014 ) as GDMP_1km_2014,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2015 ) as GDMP_1km_2015,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2016 ) as GDMP_1km_2016,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2017 ) as GDMP_1km_2017,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2018 ) as GDMP_1km_2018,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2019 ) as GDMP_1km_2019,
# MAGIC
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2014) as GDMP_300m_2014,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2015) as GDMP_300m_2015,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2016) as GDMP_300m_2016,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2017) as GDMP_300m_2017,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2018) as GDMP_300m_2018,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2019) as GDMP_300m_2019,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2020) as GDMP_300m_2020,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2021) as GDMP_300m_2021,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2022) as GDMP_300m_2022
# MAGIC
# MAGIC  FROM nuts3_2021 
# MAGIC
# MAGIC  LEFT JOIN lULUCF_2018     on lULUCF_2018.GridNum          = nuts3_2021.GridNum
# MAGIC  LEFT JOIN GDMP_1km_99_19  ON GDMP_1km_99_19.GridNum       = nuts3_2021.GridNum1km  ---- 1km
# MAGIC  LEFT JOIN GDMP_100m_14_22  ON GDMP_100m_14_22.GridNum     = nuts3_2021.gridnum
# MAGIC  
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null ----and  nuts3_2021.ISO2='PT'
# MAGIC
# MAGIC group by 
# MAGIC      nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       nuts3_2021.GridNum10km,
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE
# MAGIC
# MAGIC
# MAGIC  """)
# MAGIC gdmp_300_1000m_attribute.createOrReplaceTempView("gdmp_300_1000m_attribute")
# MAGIC // Exporting the final table
# MAGIC
# MAGIC gdmp_300_1000m_attribute
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/gdmp_300m_1km")
# MAGIC
# MAGIC

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/gdmp_300m_1km"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC
# MAGIC %md #### 2.4.2 Compare both in transformed table - for tableau

# COMMAND ----------

# MAGIC %md ##### 2.4.2.1 10km

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // 10m sub-cube: 
# MAGIC
# MAGIC val gdmp_300_1000m_attribute = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       ----nuts3_2021.GridNum10km,
# MAGIC       
# MAGIC       
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE,
# MAGIC
# MAGIC       SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_1999 ) as GDMP_1km_1999,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2000 ) as GDMP_1km_2000,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2001 ) as GDMP_1km_2001,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2002 ) as GDMP_1km_2002,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2003 ) as GDMP_1km_2003,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2004 ) as GDMP_1km_2004,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2005 ) as GDMP_1km_2005,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2006 ) as GDMP_1km_2006,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2007 ) as GDMP_1km_2007,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2008 ) as GDMP_1km_2008,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2009 ) as GDMP_1km_2009,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2010 ) as GDMP_1km_2010,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2011 ) as GDMP_1km_2011,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2012 ) as GDMP_1km_2012,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2013 ) as GDMP_1km_2013,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2014 ) as GDMP_1km_2014,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2015 ) as GDMP_1km_2015,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2016 ) as GDMP_1km_2016,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2017 ) as GDMP_1km_2017,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2018 ) as GDMP_1km_2018,
# MAGIC       SUM(GDMP_1km_99_19.GDMP_2019 ) as GDMP_1km_2019
# MAGIC
# MAGIC
# MAGIC  FROM nuts3_2021 
# MAGIC
# MAGIC  LEFT JOIN lULUCF_2018     on lULUCF_2018.GridNum          = nuts3_2021.GridNum
# MAGIC  LEFT JOIN GDMP_1km_99_19  ON GDMP_1km_99_19.GridNum       = nuts3_2021.GridNum1km  ---- 1km
# MAGIC
# MAGIC  
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null ----and  nuts3_2021.ISO2='PT'
# MAGIC
# MAGIC group by 
# MAGIC      nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       ----nuts3_2021.GridNum10km,
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE
# MAGIC
# MAGIC
# MAGIC  """)
# MAGIC gdmp_300_1000m_attribute.createOrReplaceTempView("gdmp_1km_attribute")

# COMMAND ----------


# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from gdmp_1km_attribute
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_gdmp_stat =df_gdmp.melt(id_vars=[
'AreaHa',
'ADM_ID',
'ISO2',
'LULUCF_DESCRIPTION',
'LULUCF_CODE'
 ], var_name="year", value_name="gdmp_stat")


df_transformed_gdmp_stat['statistic'] = df_transformed_gdmp_stat['year'].str[-8:-5]
df_transformed_gdmp_stat['year_link'] = df_transformed_gdmp_stat['year'].str[-4:]

# dataframe to tables:
df_transformed_gdmp_1km_1 = spark.createDataFrame(df_transformed_gdmp_stat)
df_transformed_gdmp_1km_1.createOrReplaceTempView("df_transformed_gdmp_1km_compare_STAT")

# COMMAND ----------

df_transformed_gdmp_stat

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(ADM_ID) from df_transformed_gdmp_1km_compare_STAT 
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val gdmp_1km_compare_STAT  = spark.sql(""" 
# MAGIC
# MAGIC select * from  df_transformed_gdmp_1km_compare_STAT
# MAGIC
# MAGIC
# MAGIC    """)
# MAGIC gdmp_1km_compare_STAT.createOrReplaceTempView("gdmp_1km_compare_STAT")
# MAGIC
# MAGIC
# MAGIC gdmp_1km_compare_STAT 
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_comparea_gdmp_1km")
# MAGIC
# MAGIC

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder =("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_comparea_gdmp_1km")
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ##### 2.4.2.2 300m

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // 300m  sub-cube: 
# MAGIC
# MAGIC val gdmp_300_1000m_attribute = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC      --- nuts3_2021.GridNum10km,
# MAGIC       
# MAGIC       
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE,
# MAGIC
# MAGIC       SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC
# MAGIC
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2014) as GDMP_300m_2014,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2015) as GDMP_300m_2015,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2016) as GDMP_300m_2016,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2017) as GDMP_300m_2017,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2018) as GDMP_300m_2018,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2019) as GDMP_300m_2019,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2020) as GDMP_300m_2020,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2021) as GDMP_300m_2021,
# MAGIC       SUM(GDMP_100m_14_22.GDMP_2022) as GDMP_300m_2022
# MAGIC
# MAGIC  FROM nuts3_2021 
# MAGIC
# MAGIC  LEFT JOIN lULUCF_2018     on lULUCF_2018.GridNum          = nuts3_2021.GridNum
# MAGIC  LEFT JOIN GDMP_100m_14_22  ON GDMP_100m_14_22.GridNum     = nuts3_2021.gridnum
# MAGIC  
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null ----and  nuts3_2021.ISO2='PT'
# MAGIC
# MAGIC group by 
# MAGIC      nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC      ----- nuts3_2021.GridNum10km,
# MAGIC       lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC       lULUCF_2018.LULUCF_CODE
# MAGIC
# MAGIC
# MAGIC  """)
# MAGIC gdmp_300_1000m_attribute.createOrReplaceTempView("gdmp_300_attribute")

# COMMAND ----------


# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from gdmp_300_attribute
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_gdmp_stat =df_gdmp.melt(id_vars=[
'AreaHa',
'ADM_ID',
'ISO2',
'LULUCF_DESCRIPTION',
'LULUCF_CODE'
 ], var_name="year", value_name="gdmp_stat")

# updapte statistics parameter:
df_transformed_gdmp_stat['year_link'] = df_transformed_gdmp_stat['year'].str[-4:]

# updapte year:
df_transformed_gdmp_stat['statistic'] = df_transformed_gdmp_stat['year'].str[-9:-5]


# dataframe to tables:
df_transformed_gdmp_new3 = spark.createDataFrame(df_transformed_gdmp_stat)
df_transformed_gdmp_new3.createOrReplaceTempView("df_transformed_gdmp_STAT")

# COMMAND ----------

df_transformed_gdmp_stat


# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val gdmp_300m_compare_STAT  = spark.sql(""" 
# MAGIC
# MAGIC select * from  df_transformed_gdmp_STAT
# MAGIC
# MAGIC
# MAGIC    """)
# MAGIC gdmp_300m_compare_STAT.createOrReplaceTempView("gdmp_300_compare_STAT")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val gdmp_300m_compare_STAT  = spark.sql(""" 
# MAGIC
# MAGIC select * from  df_transformed_gdmp_STAT
# MAGIC
# MAGIC
# MAGIC    """)
# MAGIC gdmp_300m_compare_STAT.createOrReplaceTempView("gdmp_300_compare_STAT")
# MAGIC
# MAGIC
# MAGIC gdmp_300m_compare_STAT 
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_comparea_gdmp_300m")

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder =("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/cube_comparea_gdmp_300m")
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ##### 2.4.2.3 1km join with 300m 2014-2019

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val gdmp_1km_300m_compare_STAT  = spark.sql(""" 
# MAGIC
# MAGIC     Select 
# MAGIC     gdmp_300_compare_STAT.ADM_ID,
# MAGIC     gdmp_300_compare_STAT.LULUCF_DESCRIPTION,
# MAGIC     gdmp_300_compare_STAT.LULUCF_CODE ,
# MAGIC     gdmp_300_compare_STAT.year_link ,
# MAGIC     gdmp_300_compare_STAT.gdmp_stat  as gdmp_stat_300m,
# MAGIC     gdmp_1km_compare_STAT.gdmp_stat as gdmp_stat_1km,
# MAGIC     gdmp_1km_compare_STAT.gdmp_stat-gdmp_300_compare_STAT.gdmp_stat as gdmp_differecne_1km_minus_300m,
# MAGIC     gdmp_300_compare_STAT.statistic
# MAGIC
# MAGIC
# MAGIC     from gdmp_300_compare_STAT
# MAGIC
# MAGIC     left join gdmp_1km_compare_STAT  on gdmp_1km_compare_STAT.ADM_ID = gdmp_300_compare_STAT.ADM_ID and  
# MAGIC                                         gdmp_1km_compare_STAT.LULUCF_DESCRIPTION = gdmp_300_compare_STAT.LULUCF_DESCRIPTION  and
# MAGIC                                         gdmp_1km_compare_STAT.LULUCF_CODE = gdmp_300_compare_STAT.LULUCF_CODE and
# MAGIC                                         gdmp_1km_compare_STAT.year_link = gdmp_300_compare_STAT.year_link 
# MAGIC
# MAGIC     where gdmp_300_compare_STAT.year_link in ('2014','2016','2016','2017','2018','2019')
# MAGIC    """)
# MAGIC gdmp_1km_300m_compare_STAT.createOrReplaceTempView("gdmp_1km_300m_compare_STAT_2014_2019")
# MAGIC
# MAGIC
# MAGIC gdmp_1km_300m_compare_STAT 
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/gdmp_1km_300m_compare_STAT_2014_2019")
# MAGIC

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder =("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/gdmp_1km_300m_compare_STAT_2014_2019")
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ### 2.5 Wildfires burnt carbon & all dims (NUTS, PA, MAES, EnvZones & LCF)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // BURNT CARBON 10km sub-cube: 
# MAGIC
# MAGIC val burnt_carbon10km = spark.sql(""" 
# MAGIC
# MAGIC
# MAGIC select 
# MAGIC       fire_year.GridNum10km,
# MAGIC       SUM(fire_year.AreaHa) as AreaHa,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC        PA2022.protected_area,
# MAGIC
# MAGIC       SUM(cfire2003_Tnyear_3035) as cfire2003_Tnyear_3035 ,
# MAGIC       SUM(cfire2004_Tnyear_3035) as cfire2004_Tnyear_3035 ,
# MAGIC       SUM(cfire2005_Tnyear_3035) as cfire2005_Tnyear_3035 ,
# MAGIC       SUM(cfire2006_Tnyear_3035) as cfire2006_Tnyear_3035 ,
# MAGIC       SUM(cfire2007_Tnyear_3035) as cfire2007_Tnyear_3035 ,
# MAGIC       SUM(cfire2008_Tnyear_3035) as cfire2008_Tnyear_3035 ,
# MAGIC       SUM(cfire2009_Tnyear_3035) as cfire2009_Tnyear_3035 ,
# MAGIC       SUM(cfire2010_Tnyear_3035) as cfire2010_Tnyear_3035 ,
# MAGIC       SUM(cfire2011_Tnyear_3035) as cfire2011_Tnyear_3035 ,
# MAGIC       SUM(cfire2012_Tnyear_3035) as cfire2012_Tnyear_3035 ,
# MAGIC       SUM(cfire2013_Tnyear_3035) as cfire2013_Tnyear_3035 ,
# MAGIC       SUM(cfire2014_Tnyear_3035) as cfire2014_Tnyear_3035 ,
# MAGIC       SUM(cfire2015_Tnyear_3035) as cfire2015_Tnyear_3035 ,
# MAGIC       SUM(cfire2016_Tnyear_3035) as cfire2016_Tnyear_3035 ,
# MAGIC       SUM(cfire2017_Tnyear_3035) as cfire2017_Tnyear_3035 ,
# MAGIC       SUM(cfire2018_Tnyear_3035) as cfire2018_Tnyear_3035 ,
# MAGIC       SUM(cfire2019_Tnyear_3035) as cfire2019_Tnyear_3035 ,
# MAGIC       SUM(cfire2020_Tnyear_3035) as cfire2020_Tnyear_3035 ,
# MAGIC       SUM(cfire2021_Tnyear_3035) as cfire2021_Tnyear_3035 ,
# MAGIC       SUM(cfire2022_Tnyear_3035) as cfire2022_Tnyear_3035 
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC   LEFT JOIN burnt_carbon_10km  ON fire_year.GridNum10km = burnt_carbon_10km.GridNum10km
# MAGIC   LEFT JOIN PA2022 ON fire_year.GridNum = PA2022.GridNum    
# MAGIC
# MAGIC where nuts3_2021.ADM_ID is not null
# MAGIC
# MAGIC group by
# MAGIC      fire_year.GridNum10km,
# MAGIC      nuts3_2021.ADM_ID,
# MAGIC      nuts3_2021.ISO2,
# MAGIC      maes_sq1.MAES_CODE,
# MAGIC      EnvZones.Category,
# MAGIC       PA2022.protected_area
# MAGIC
# MAGIC
# MAGIC         """)
# MAGIC burnt_carbon10km.createOrReplaceTempView("burnt_carbon10km")

# COMMAND ----------



# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from burnt_carbon10km
''')

df_gdmp = sql_for_panda.select("*").toPandas()


#GridNum10km	AreaHa	ADM_ID	ISO2	MAES_CODE	env_zones	
df_transformed_carbon_burnt =df_gdmp.melt(id_vars=['GridNum10km',
'AreaHa',
'ADM_ID',
'ISO2',
'protected_area',
'MAES_CODE',
'env_zones' ], var_name="year", value_name="burnt_carbon_t")

# updapte year:
df_transformed_carbon_burnt['year_link'] = df_transformed_carbon_burnt['year'].str[5:9]


# dataframe to tables:
df_transformed_carbon_burnt_2 = spark.createDataFrame(df_transformed_carbon_burnt)
df_transformed_carbon_burnt_2.createOrReplaceTempView("df_transformed_carbon_burnt")

# COMMAND ----------




# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## 3 Building super-cube and export data (Approach 1)

# COMMAND ----------

# MAGIC %md ### 3.1 Building super-cube using 1km GDMP

# COMMAND ----------

# MAGIC %sql
# MAGIC  select 
# MAGIC
# MAGIC   df_transformed_gdmp.GridNum10km,
# MAGIC   df_transformed_gdmp.ADM_ID,
# MAGIC   df_transformed_gdmp.ISO2,
# MAGIC   df_transformed_gdmp.MAES_CODE,
# MAGIC   df_transformed_gdmp.env_zones,
# MAGIC   df_transformed_gdmp.AreaHa,
# MAGIC   df_transformed_gdmp.gdmp,
# MAGIC   df_transformed_gdmp.protected_area,
# MAGIC
# MAGIC   df_transformed_fire.wild_fire,
# MAGIC   df_transformed_gdmp_STAT.gdmp_stat,
# MAGIC   df_transformed_gdmp_STAT.statistic,
# MAGIC   df_transformed_carbon_burnt.burnt_carbon_t,
# MAGIC
# MAGIC   df_transformed_gdmp_STAT_SLOPE.slope_avg,  ----- for all years the same value!!
# MAGIC   df_transformed_gdmp.year_link 
# MAGIC
# MAGIC
# MAGIC
# MAGIC   from df_transformed_gdmp
# MAGIC
# MAGIC
# MAGIC   left join  df_transformed_fire on 
# MAGIC
# MAGIC              df_transformed_fire.year_link   =df_transformed_gdmp.year_link AND
# MAGIC               df_transformed_fire.ADM_ID     =df_transformed_gdmp.ADM_ID AND
# MAGIC               df_transformed_fire.ISO2       =df_transformed_gdmp.ISO2 AND
# MAGIC               df_transformed_fire.MAES_CODE  =df_transformed_gdmp.MAES_CODE  AND
# MAGIC               df_transformed_fire.GridNum10km  =df_transformed_gdmp.GridNum10km  AND
# MAGIC               df_transformed_fire.protected_area  =df_transformed_gdmp.protected_area  AND
# MAGIC               df_transformed_fire.env_zones  =df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC
# MAGIC   --df_transformed_gdmp_STAT
# MAGIC left join   df_transformed_gdmp_STAT on 
# MAGIC             df_transformed_gdmp_STAT.year_link    =df_transformed_gdmp.year_link AND
# MAGIC             df_transformed_gdmp_STAT.ADM_ID       =df_transformed_gdmp.ADM_ID AND
# MAGIC             df_transformed_gdmp_STAT.ISO2         =df_transformed_gdmp.ISO2 AND
# MAGIC             df_transformed_gdmp_STAT.MAES_CODE    =df_transformed_gdmp.MAES_CODE  AND
# MAGIC             df_transformed_gdmp_STAT.GridNum10km      =df_transformed_gdmp.GridNum10km  AND
# MAGIC             df_transformed_gdmp_STAT.protected_area      =df_transformed_gdmp.protected_area  AND
# MAGIC             df_transformed_gdmp_STAT.env_zones     = df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC --df_transformed_carbon_burnt
# MAGIC left join   df_transformed_carbon_burnt on 
# MAGIC             df_transformed_carbon_burnt.year_link    =df_transformed_gdmp.year_link AND
# MAGIC             df_transformed_carbon_burnt.ADM_ID       =df_transformed_gdmp.ADM_ID AND
# MAGIC             df_transformed_carbon_burnt.ISO2         =df_transformed_gdmp.ISO2 AND
# MAGIC             df_transformed_carbon_burnt.MAES_CODE    =df_transformed_gdmp.MAES_CODE  AND
# MAGIC             df_transformed_carbon_burnt.GridNum10km      =df_transformed_gdmp.GridNum10km  AND
# MAGIC             df_transformed_carbon_burnt.protected_area      =df_transformed_gdmp.protected_area  AND
# MAGIC             df_transformed_carbon_burnt.env_zones     = df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC --df_transformed_gdmp_STAT_SLOPE (without YEAR!!!)
# MAGIC
# MAGIC left join   df_transformed_gdmp_STAT_SLOPE on 
# MAGIC             ---wild_fire_GDMP_STAT_slope_sub4.year_link    =df_transformed_gdmp.year_link AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.ADM_ID       =df_transformed_gdmp.ADM_ID AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.ISO2         =df_transformed_gdmp.ISO2 AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.MAES_CODE    =df_transformed_gdmp.MAES_CODE  AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.GridNum10km      =df_transformed_gdmp.GridNum10km  AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.protected_area      =df_transformed_gdmp.protected_area  AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.env_zones     = df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC where df_transformed_gdmp.GridNum10km =13854422035595264
# MAGIC
# MAGIC ---9713519511470080
# MAGIC
# MAGIC
# MAGIC ---where df_transformed_gdmp.ISO2  = 'PT'  and wild_fire >0

# COMMAND ----------

# MAGIC %scala
# MAGIC //// DRAFT: cube of wild-fire pixel with GDMP:
# MAGIC /// gdmp: 1999-2019
# MAGIC //// fire: 2000-2022--
# MAGIC // 2000-2019 union year?
# MAGIC
# MAGIC val cube_c = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC   df_transformed_gdmp.GridNum10km,
# MAGIC   df_transformed_gdmp.ADM_ID,
# MAGIC   df_transformed_gdmp.ISO2,
# MAGIC   df_transformed_gdmp.MAES_CODE,
# MAGIC   df_transformed_gdmp.env_zones,
# MAGIC   df_transformed_gdmp.AreaHa,
# MAGIC   df_transformed_gdmp.gdmp,
# MAGIC   df_transformed_gdmp.protected_area,
# MAGIC
# MAGIC   df_transformed_fire.wild_fire,
# MAGIC   df_transformed_gdmp_STAT.gdmp_stat,
# MAGIC   df_transformed_gdmp_STAT.statistic,
# MAGIC   df_transformed_carbon_burnt.burnt_carbon_t,
# MAGIC
# MAGIC   df_transformed_gdmp_STAT_SLOPE.slope_avg,  ----- for all years the same value!!
# MAGIC   df_transformed_gdmp.year_link 
# MAGIC
# MAGIC
# MAGIC
# MAGIC   from df_transformed_gdmp
# MAGIC
# MAGIC
# MAGIC   left join  df_transformed_fire on 
# MAGIC
# MAGIC              df_transformed_fire.year_link   =df_transformed_gdmp.year_link AND
# MAGIC               df_transformed_fire.ADM_ID     =df_transformed_gdmp.ADM_ID AND
# MAGIC               df_transformed_fire.ISO2       =df_transformed_gdmp.ISO2 AND
# MAGIC               df_transformed_fire.MAES_CODE  =df_transformed_gdmp.MAES_CODE  AND
# MAGIC               df_transformed_fire.GridNum10km  =df_transformed_gdmp.GridNum10km  AND
# MAGIC               df_transformed_fire.protected_area  =df_transformed_gdmp.protected_area  AND
# MAGIC               df_transformed_fire.env_zones  =df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC
# MAGIC   --df_transformed_gdmp_STAT
# MAGIC left join   df_transformed_gdmp_STAT on 
# MAGIC             df_transformed_gdmp_STAT.year_link    =df_transformed_gdmp.year_link AND
# MAGIC             df_transformed_gdmp_STAT.ADM_ID       =df_transformed_gdmp.ADM_ID AND
# MAGIC             df_transformed_gdmp_STAT.ISO2         =df_transformed_gdmp.ISO2 AND
# MAGIC             df_transformed_gdmp_STAT.MAES_CODE    =df_transformed_gdmp.MAES_CODE  AND
# MAGIC             df_transformed_gdmp_STAT.GridNum10km      =df_transformed_gdmp.GridNum10km  AND
# MAGIC             df_transformed_gdmp_STAT.protected_area      =df_transformed_gdmp.protected_area  AND
# MAGIC             df_transformed_gdmp_STAT.env_zones     = df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC --df_transformed_carbon_burnt
# MAGIC left join   df_transformed_carbon_burnt on 
# MAGIC             df_transformed_carbon_burnt.year_link    =df_transformed_gdmp.year_link AND
# MAGIC             df_transformed_carbon_burnt.ADM_ID       =df_transformed_gdmp.ADM_ID AND
# MAGIC             df_transformed_carbon_burnt.ISO2         =df_transformed_gdmp.ISO2 AND
# MAGIC             df_transformed_carbon_burnt.MAES_CODE    =df_transformed_gdmp.MAES_CODE  AND
# MAGIC             df_transformed_carbon_burnt.GridNum10km      =df_transformed_gdmp.GridNum10km  AND
# MAGIC             df_transformed_carbon_burnt.protected_area      =df_transformed_gdmp.protected_area  AND
# MAGIC             df_transformed_carbon_burnt.env_zones     = df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC --df_transformed_gdmp_STAT_SLOPE (without YEAR!!!)
# MAGIC
# MAGIC left join   df_transformed_gdmp_STAT_SLOPE on 
# MAGIC             ---wild_fire_GDMP_STAT_slope_sub4.year_link    =df_transformed_gdmp.year_link AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.ADM_ID       =df_transformed_gdmp.ADM_ID AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.ISO2         =df_transformed_gdmp.ISO2 AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.MAES_CODE    =df_transformed_gdmp.MAES_CODE  AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.GridNum10km      =df_transformed_gdmp.GridNum10km  AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.protected_area      =df_transformed_gdmp.protected_area  AND
# MAGIC             df_transformed_gdmp_STAT_SLOPE.env_zones     = df_transformed_gdmp.env_zones 
# MAGIC
# MAGIC
# MAGIC
# MAGIC    """)
# MAGIC cube_c.createOrReplaceTempView("cube_c_1km")
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC // Exporting the final table
# MAGIC
# MAGIC cube_c
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/wildfires_ctable_1km_draft")
# MAGIC

# COMMAND ----------


### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/wildfires_ctable_1km_draft"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ### 3.2 Building super-cube using 100m GDMP

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_transformed_gdmp_100m_STAT
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  select 
# MAGIC
# MAGIC   df_transformed_gdmp_100m.GridNum10km,
# MAGIC   df_transformed_gdmp_100m.ADM_ID,
# MAGIC   df_transformed_gdmp_100m.ISO2,
# MAGIC   df_transformed_gdmp_100m.MAES_CODE,
# MAGIC   df_transformed_gdmp_100m.env_zones,
# MAGIC   df_transformed_gdmp_100m.AreaHa,
# MAGIC   df_transformed_gdmp_100m.gdmp /1000 as gdmp_t_per_ha,
# MAGIC   df_transformed_gdmp_100m.protected_area,
# MAGIC
# MAGIC   df_transformed_fire.wild_fire,
# MAGIC    df_transformed_gdmp_100m_STAT.gdmp as gdmp_anom,
# MAGIC  --- df_transformed_gdmp_100m_STAT.statistic,
# MAGIC   df_transformed_carbon_burnt.burnt_carbon_t,
# MAGIC
# MAGIC   df_transformed_gdmp_100m.year_link 
# MAGIC
# MAGIC
# MAGIC
# MAGIC   from df_transformed_gdmp_100m --100m
# MAGIC
# MAGIC
# MAGIC   left join  df_transformed_fire on 
# MAGIC
# MAGIC              df_transformed_fire.year_link   =df_transformed_gdmp_100m.year_link AND
# MAGIC               df_transformed_fire.ADM_ID     =df_transformed_gdmp_100m.ADM_ID AND
# MAGIC               df_transformed_fire.ISO2       =df_transformed_gdmp_100m.ISO2 AND
# MAGIC               df_transformed_fire.MAES_CODE  =df_transformed_gdmp_100m.MAES_CODE  AND
# MAGIC               df_transformed_fire.GridNum10km  =df_transformed_gdmp_100m.GridNum10km  AND
# MAGIC               df_transformed_fire.protected_area  =df_transformed_gdmp_100m.protected_area  AND
# MAGIC               df_transformed_fire.env_zones  =df_transformed_gdmp_100m.env_zones 
# MAGIC
# MAGIC
# MAGIC   --df_transformed_gdmp_100m_STAT-100m
# MAGIC left join   df_transformed_gdmp_100m_STAT on 
# MAGIC             df_transformed_gdmp_100m_STAT.year_link    =df_transformed_gdmp_100m.year_link AND
# MAGIC         df_transformed_gdmp_100m_STAT.ADM_ID       =df_transformed_gdmp_100m.ADM_ID AND
# MAGIC           df_transformed_gdmp_100m_STAT.ISO2         =df_transformed_gdmp_100m.ISO2 AND
# MAGIC            df_transformed_gdmp_100m_STAT.MAES_CODE    =df_transformed_gdmp_100m.MAES_CODE  AND
# MAGIC           df_transformed_gdmp_100m_STAT.GridNum10km      =df_transformed_gdmp_100m.GridNum10km  AND
# MAGIC             df_transformed_gdmp_100m_STAT.protected_area      =df_transformed_gdmp_100m.protected_area  AND
# MAGIC            df_transformed_gdmp_100m_STAT.env_zones     = df_transformed_gdmp_100m.env_zones 
# MAGIC
# MAGIC --df_transformed_carbon_burnt
# MAGIC left join   df_transformed_carbon_burnt on 
# MAGIC             df_transformed_carbon_burnt.year_link    =df_transformed_gdmp_100m.year_link AND
# MAGIC             df_transformed_carbon_burnt.ADM_ID       =df_transformed_gdmp_100m.ADM_ID AND
# MAGIC             df_transformed_carbon_burnt.ISO2         =df_transformed_gdmp_100m.ISO2 AND
# MAGIC             df_transformed_carbon_burnt.MAES_CODE    =df_transformed_gdmp_100m.MAES_CODE  AND
# MAGIC             df_transformed_carbon_burnt.GridNum10km      =df_transformed_gdmp_100m.GridNum10km  AND
# MAGIC             df_transformed_carbon_burnt.protected_area      =df_transformed_gdmp_100m.protected_area  AND
# MAGIC             df_transformed_carbon_burnt.env_zones     = df_transformed_gdmp_100m.env_zones 
# MAGIC
# MAGIC
# MAGIC where df_transformed_gdmp_100m.GridNum10km =13854422035595264
# MAGIC ---where df_transformed_gdmp_100m.ISO2  = 'PT'  and wild_fire >0

# COMMAND ----------

# MAGIC %scala
# MAGIC //// DRAFT: cube of wild-fire pixel with GDMP 100m
# MAGIC
# MAGIC val cube_c_100m = spark.sql(""" 
# MAGIC
# MAGIC select 
# MAGIC
# MAGIC   df_transformed_gdmp_100m.GridNum10km,
# MAGIC   df_transformed_gdmp_100m.ADM_ID,
# MAGIC   df_transformed_gdmp_100m.ISO2,
# MAGIC   df_transformed_gdmp_100m.MAES_CODE,
# MAGIC   df_transformed_gdmp_100m.env_zones,
# MAGIC   df_transformed_gdmp_100m.AreaHa,
# MAGIC   df_transformed_gdmp_100m.gdmp /1000 as gdmp_t_per_ha,
# MAGIC   df_transformed_gdmp_100m.protected_area,
# MAGIC
# MAGIC   df_transformed_fire.wild_fire,
# MAGIC    df_transformed_gdmp_100m_STAT.gdmp as gdmp_anom,
# MAGIC  --- df_transformed_gdmp_100m_STAT.statistic,
# MAGIC   df_transformed_carbon_burnt.burnt_carbon_t,
# MAGIC
# MAGIC   df_transformed_gdmp_100m.year_link 
# MAGIC
# MAGIC
# MAGIC
# MAGIC   from df_transformed_gdmp_100m --100m
# MAGIC
# MAGIC
# MAGIC   left join  df_transformed_fire on 
# MAGIC
# MAGIC              df_transformed_fire.year_link   =df_transformed_gdmp_100m.year_link AND
# MAGIC               df_transformed_fire.ADM_ID     =df_transformed_gdmp_100m.ADM_ID AND
# MAGIC               df_transformed_fire.ISO2       =df_transformed_gdmp_100m.ISO2 AND
# MAGIC               df_transformed_fire.MAES_CODE  =df_transformed_gdmp_100m.MAES_CODE  AND
# MAGIC               df_transformed_fire.GridNum10km  =df_transformed_gdmp_100m.GridNum10km  AND
# MAGIC               df_transformed_fire.protected_area  =df_transformed_gdmp_100m.protected_area  AND
# MAGIC               df_transformed_fire.env_zones  =df_transformed_gdmp_100m.env_zones 
# MAGIC
# MAGIC
# MAGIC   --df_transformed_gdmp_100m_STAT-100m
# MAGIC left join   df_transformed_gdmp_100m_STAT on 
# MAGIC             df_transformed_gdmp_100m_STAT.year_link    =df_transformed_gdmp_100m.year_link AND
# MAGIC         df_transformed_gdmp_100m_STAT.ADM_ID       =df_transformed_gdmp_100m.ADM_ID AND
# MAGIC           df_transformed_gdmp_100m_STAT.ISO2         =df_transformed_gdmp_100m.ISO2 AND
# MAGIC            df_transformed_gdmp_100m_STAT.MAES_CODE    =df_transformed_gdmp_100m.MAES_CODE  AND
# MAGIC           df_transformed_gdmp_100m_STAT.GridNum10km      =df_transformed_gdmp_100m.GridNum10km  AND
# MAGIC             df_transformed_gdmp_100m_STAT.protected_area      =df_transformed_gdmp_100m.protected_area  AND
# MAGIC            df_transformed_gdmp_100m_STAT.env_zones     = df_transformed_gdmp_100m.env_zones 
# MAGIC
# MAGIC --df_transformed_carbon_burnt
# MAGIC left join   df_transformed_carbon_burnt on 
# MAGIC             df_transformed_carbon_burnt.year_link    =df_transformed_gdmp_100m.year_link AND
# MAGIC             df_transformed_carbon_burnt.ADM_ID       =df_transformed_gdmp_100m.ADM_ID AND
# MAGIC             df_transformed_carbon_burnt.ISO2         =df_transformed_gdmp_100m.ISO2 AND
# MAGIC             df_transformed_carbon_burnt.MAES_CODE    =df_transformed_gdmp_100m.MAES_CODE  AND
# MAGIC             df_transformed_carbon_burnt.GridNum10km      =df_transformed_gdmp_100m.GridNum10km  AND
# MAGIC             df_transformed_carbon_burnt.protected_area      =df_transformed_gdmp_100m.protected_area  AND
# MAGIC             df_transformed_carbon_burnt.env_zones     = df_transformed_gdmp_100m.env_zones 
# MAGIC
# MAGIC
# MAGIC ----where df_transformed_gdmp_100m.GridNum10km =13854422035595264
# MAGIC where  df_transformed_gdmp_100m.ISO2 is not NULL
# MAGIC
# MAGIC    """)
# MAGIC cube_c_100m.createOrReplaceTempView("cube_c_100m")
# MAGIC
# MAGIC
# MAGIC
# MAGIC // Exporting the final table
# MAGIC
# MAGIC cube_c_100m
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/wildfires_ctable_100m")
# MAGIC
# MAGIC

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/wildfires_ctable_100m"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ## 4 Join Data (Approach 2 - by pixel by pixel)
# MAGIC
# MAGIC Here we first try to connect everything, and then select the corresponding GDMP values for pixels with fire. And only then to work out the statistics for the reference areas.

# COMMAND ----------

# MAGIC %md ### 4.1 Development

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nuts3_2021
# MAGIC where
# MAGIC
# MAGIC  nuts3_2021.ISO2 ='PT'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC
# MAGIC       fire_year.GridNum10km,
# MAGIC       fire_year.AreaHa as Fire_area_ha,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       PA2022.protected_area,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC     
# MAGIC       fire_2014 as fire_2014,
# MAGIC       fire_2015 as fire_2015,
# MAGIC       fire_2016 as fire_2016,
# MAGIC       fire_2017 as fire_2017,
# MAGIC       fire_2018 as fire_2018,
# MAGIC       fire_2019 as fire_2019,
# MAGIC       fire_2020 as fire_2020,
# MAGIC       fire_2021 as fire_2021,
# MAGIC       fire_2022 as fire_2022,
# MAGIC
# MAGIC       GDMP_2014 as GDMP_2014,
# MAGIC       GDMP_2015 as GDMP_2015,
# MAGIC       GDMP_2016 as GDMP_2016,
# MAGIC       GDMP_2017 as GDMP_2017,
# MAGIC       GDMP_2018 as GDMP_2018,
# MAGIC       GDMP_2019 as GDMP_2019,
# MAGIC       GDMP_2020 as GDMP_2020,
# MAGIC       GDMP_2021 as GDMP_2021,
# MAGIC       GDMP_2022 as GDMP_2022,
# MAGIC
# MAGIC       GDMP_100m_anom_2014 as GDMP_100m_anom_2014,
# MAGIC       GDMP_100m_anom_2015 as GDMP_100m_anom_2015,
# MAGIC       GDMP_100m_anom_2016 as GDMP_100m_anom_2016,
# MAGIC       GDMP_100m_anom_2017 as GDMP_100m_anom_2017,
# MAGIC       GDMP_100m_anom_2018 as GDMP_100m_anom_2018,
# MAGIC       GDMP_100m_anom_2019 as GDMP_100m_anom_2019,
# MAGIC       GDMP_100m_anom_2020 as GDMP_100m_anom_2020,
# MAGIC       GDMP_100m_anom_2021 as GDMP_100m_anom_2021,
# MAGIC       GDMP_100m_anom_2022 as GDMP_100m_anom_2022
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC
# MAGIC   LEFT JOIN PA2022                ON fire_year.GridNum = PA2022.GridNum  
# MAGIC   LEFT JOIN GDMP_100m_14_22       ON fire_year.GridNum = GDMP_100m_14_22.GridNum 
# MAGIC   LEFT JOIN GDMP_100m_statistics  ON fire_year.GridNum = GDMP_100m_statistics.GridNum 
# MAGIC
# MAGIC where nuts3_2021.GridNum10km =13854422035595264

# COMMAND ----------

# MAGIC %scala
# MAGIC //// DRAFT: test cube of wild-fire pixel with GDMP 100m
# MAGIC
# MAGIC val approach_2_test_cube = spark.sql(""" 
# MAGIC SELECT 
# MAGIC
# MAGIC       fire_year.GridNum10km,
# MAGIC       fire_year.AreaHa as Fire_area_ha,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       maes_sq1.MAES_CODE,
# MAGIC       PA2022.protected_area,
# MAGIC       EnvZones.Category as env_zones,
# MAGIC     
# MAGIC       fire_2014 as fire_2014,
# MAGIC       fire_2015 as fire_2015,
# MAGIC       fire_2016 as fire_2016,
# MAGIC       fire_2017 as fire_2017,
# MAGIC       fire_2018 as fire_2018,
# MAGIC       fire_2019 as fire_2019,
# MAGIC       fire_2020 as fire_2020,
# MAGIC       fire_2021 as fire_2021,
# MAGIC       fire_2022 as fire_2022,
# MAGIC
# MAGIC       GDMP_2014 as GDMP_2014,
# MAGIC       GDMP_2015 as GDMP_2015,
# MAGIC       GDMP_2016 as GDMP_2016,
# MAGIC       GDMP_2017 as GDMP_2017,
# MAGIC       GDMP_2018 as GDMP_2018,
# MAGIC       GDMP_2019 as GDMP_2019,
# MAGIC       GDMP_2020 as GDMP_2020,
# MAGIC       GDMP_2021 as GDMP_2021,
# MAGIC       GDMP_2022 as GDMP_2022,
# MAGIC
# MAGIC       GDMP_100m_anom_2014 as GDMP_100m_anom_2014,
# MAGIC       GDMP_100m_anom_2015 as GDMP_100m_anom_2015,
# MAGIC       GDMP_100m_anom_2016 as GDMP_100m_anom_2016,
# MAGIC       GDMP_100m_anom_2017 as GDMP_100m_anom_2017,
# MAGIC       GDMP_100m_anom_2018 as GDMP_100m_anom_2018,
# MAGIC       GDMP_100m_anom_2019 as GDMP_100m_anom_2019,
# MAGIC       GDMP_100m_anom_2020 as GDMP_100m_anom_2020,
# MAGIC       GDMP_100m_anom_2021 as GDMP_100m_anom_2021,
# MAGIC       GDMP_100m_anom_2022 as GDMP_100m_anom_2022
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC
# MAGIC   LEFT JOIN PA2022                ON fire_year.GridNum = PA2022.GridNum  
# MAGIC   LEFT JOIN GDMP_100m_14_22       ON fire_year.GridNum = GDMP_100m_14_22.GridNum 
# MAGIC   LEFT JOIN GDMP_100m_statistics  ON fire_year.GridNum = GDMP_100m_statistics.GridNum 
# MAGIC
# MAGIC ----where nuts3_2021.ISO2 ='PT' and nuts3_2021.GridNum = 5122646148644864
# MAGIC where nuts3_2021.GridNum10km =13854422035595264
# MAGIC -----where  df_transformed_gdmp_100m.ISO2 is not NULL
# MAGIC
# MAGIC    """)
# MAGIC approach_2_test_cube.createOrReplaceTempView("approach2_test_cube_c_100m")
# MAGIC
# MAGIC
# MAGIC // Exporting the final table
# MAGIC
# MAGIC //approach_2_test_cube.write.parquet("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/test_cube_approach2.parquet")
# MAGIC approach_2_test_cube
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/test_cube_approach2_parquet")
# MAGIC

# COMMAND ----------

##///### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/test_cube_approach2_parquet"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        #print ("Exported file:")
        #print(file.name)
        #print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Reading the LUT for CLC...:
# MAGIC val lut_clc  = spark.read.format("csv")
# MAGIC .options(Map("delimiter"->","))
# MAGIC  .option("header", "true")
# MAGIC    .load("dbfs:/mnt/trainingDatabricks/ExportTable/wildfires/test_cube_approach2_parquet")     ////------Lookup_CLC_07112022_4.csv   Lookup_CLC_24032021_4.csv
# MAGIC lut_clc.createOrReplaceTempView("LUT_clc_classes")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC SHOW COLUMNS IN burnt_carbon_10km;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC       fire_year.GridNum10km,
# MAGIC       sum(fire_year.AreaHa) as Fire_area_ha,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       --maes_sq1.MAES_CODE,
# MAGIC       --PA2022.protected_area,
# MAGIC       --EnvZones.Category as env_zones,
# MAGIC     
# MAGIC       --fire_2014 as fire_2014,
# MAGIC       --fire_2015 as fire_2015,
# MAGIC       --fire_2016 as fire_2016,
# MAGIC       sum(fire_2017) as fire_2017,
# MAGIC       --fire_2018 as fire_2018,
# MAGIC       --fire_2019 as fire_2019,
# MAGIC       --fire_2020 as fire_2020,
# MAGIC       --fire_2021 as fire_2021,
# MAGIC       --fire_2022 as fire_2022,
# MAGIC
# MAGIC       --fire_2022 + fire_2021+fire_2020+fire_2019+fire_2018+fire_2017+fire_2016+fire_2015+fire_2014 as total_fire_14_22,
# MAGIC
# MAGIC
# MAGIC       --GDMP_2014 as GDMP_2014,
# MAGIC       --GDMP_2015 as GDMP_2015,
# MAGIC       sum(GDMP_2016) as GDMP_2016,
# MAGIC       sum(GDMP_2017) as GDMP_2017,
# MAGIC       sum(GDMP_2018) as GDMP_2018,
# MAGIC       --GDMP_2019 as GDMP_2019,
# MAGIC       --GDMP_2020 as GDMP_2020,
# MAGIC       --GDMP_2021 as GDMP_2021,
# MAGIC       --GDMP_2022 as GDMP_2022,
# MAGIC
# MAGIC       --GDMP_100m_anom_2014 as GDMP_100m_anom_2014
# MAGIC       --GDMP_100m_anom_2015 as GDMP_100m_anom_2015,
# MAGIC       --GDMP_100m_anom_2016 as GDMP_100m_anom_2016,
# MAGIC       avg(GDMP_100m_anom_2017) as GDMP_100m_anom_2017
# MAGIC       --GDMP_100m_anom_2018 as GDMP_100m_anom_2018,
# MAGIC       --GDMP_100m_anom_2019 as GDMP_100m_anom_2019,
# MAGIC       --GDMP_100m_anom_2020 as GDMP_100m_anom_2020,
# MAGIC       --GDMP_100m_anom_2021 as GDMP_100m_anom_2021,
# MAGIC       --GDMP_100m_anom_2022 as GDMP_100m_anom_2022
# MAGIC      
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   --LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   --LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC
# MAGIC   --LEFT JOIN PA2022                ON fire_year.GridNum = PA2022.GridNum  
# MAGIC   LEFT JOIN GDMP_100m_14_22       ON fire_year.GridNum = GDMP_100m_14_22.GridNum 
# MAGIC   LEFT JOIN GDMP_100m_statistics  ON fire_year.GridNum = GDMP_100m_statistics.GridNum 
# MAGIC   ---left join   df_transformed_carbon_burnt on  fire_year.GridNum10km = df_transformed_carbon_burnt.GridNum10km  
# MAGIC
# MAGIC where fire_2017 >0 
# MAGIC
# MAGIC --nuts3_2021.GridNum10km =13854422035595264 ---AND fire_2022 + fire_2021+fire_2020+fire_2019+fire_2018+fire_2017+fire_2016+fire_2015+fire_2014  >0
# MAGIC
# MAGIC ---9713519511470080
# MAGIC
# MAGIC GROUP BY
# MAGIC
# MAGIC   fire_year.GridNum10km,
# MAGIC    nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ISO2
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC fire_year.GridNum,
# MAGIC       fire_year.GridNum10km,
# MAGIC       (fire_year.AreaHa) as Fire_area_ha,
# MAGIC
# MAGIC       nuts3_2021.ADM_ID,
# MAGIC       nuts3_2021.ISO2,
# MAGIC       --maes_sq1.MAES_CODE,
# MAGIC       --PA2022.protected_area,
# MAGIC       --EnvZones.Category as env_zones,
# MAGIC     
# MAGIC       fire_2014 as fire_2014,
# MAGIC       fire_2015 as fire_2015,
# MAGIC       fire_2016 as fire_2016,
# MAGIC       (fire_2017) as fire_2017,
# MAGIC       fire_2018 as fire_2018,
# MAGIC       fire_2019 as fire_2019,
# MAGIC       fire_2020 as fire_2020,
# MAGIC       fire_2021 as fire_2021,
# MAGIC       fire_2022 as fire_2022,
# MAGIC
# MAGIC       --fire_2022 + fire_2021+fire_2020+fire_2019+fire_2018+fire_2017+fire_2016+fire_2015+fire_2014 as total_fire_14_22,
# MAGIC
# MAGIC
# MAGIC       GDMP_2014 as GDMP_2014,
# MAGIC       GDMP_2015 as GDMP_2015,
# MAGIC       (GDMP_2016) as GDMP_2016,
# MAGIC       (GDMP_2017) as GDMP_2017,
# MAGIC       (GDMP_2018) as GDMP_2018,
# MAGIC       GDMP_2019 as GDMP_2019,
# MAGIC       GDMP_2020 as GDMP_2020,
# MAGIC       GDMP_2021 as GDMP_2021,
# MAGIC       GDMP_2022 as GDMP_2022,
# MAGIC
# MAGIC         cfire2014_Tnyear_3035,
# MAGIC         cfire2015_Tnyear_3035,
# MAGIC         cfire2016_Tnyear_3035,
# MAGIC         cfire2017_Tnyear_3035,
# MAGIC         cfire2018_Tnyear_3035,
# MAGIC         cfire2019_Tnyear_3035,
# MAGIC         cfire2020_Tnyear_3035,
# MAGIC         cfire2021_Tnyear_3035,
# MAGIC         cfire2022_Tnyear_3035
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC       --GDMP_100m_anom_2014 as GDMP_100m_anom_2014
# MAGIC       --GDMP_100m_anom_2015 as GDMP_100m_anom_2015,
# MAGIC       --GDMP_100m_anom_2016 as GDMP_100m_anom_2016,
# MAGIC       --GDMP_100m_anom_2017) as GDMP_100m_anom_2017
# MAGIC       --GDMP_100m_anom_2018 as GDMP_100m_anom_2018,
# MAGIC       --GDMP_100m_anom_2019 as GDMP_100m_anom_2019,
# MAGIC       --GDMP_100m_anom_2020 as GDMP_100m_anom_2020,
# MAGIC       --GDMP_100m_anom_2021 as GDMP_100m_anom_2021,
# MAGIC       --GDMP_100m_anom_2022 as GDMP_100m_anom_2022
# MAGIC      
# MAGIC
# MAGIC
# MAGIC  FROM fire_year 
# MAGIC
# MAGIC   --LEFT JOIN maes_sq1        ON fire_year.GridNum = maes_sq1.GridNum
# MAGIC   --LEFT JOIN EnvZones        ON fire_year.GridNum = EnvZones.GridNum
# MAGIC   LEFT JOIN nuts3_2021      ON fire_year.GridNum = nuts3_2021.GridNum    
# MAGIC
# MAGIC   --LEFT JOIN PA2022                ON fire_year.GridNum = PA2022.GridNum  
# MAGIC   LEFT JOIN GDMP_100m_14_22       ON fire_year.GridNum = GDMP_100m_14_22.GridNum 
# MAGIC   LEFT JOIN GDMP_100m_statistics  ON fire_year.GridNum = GDMP_100m_statistics.GridNum 
# MAGIC   left join   burnt_carbon_10km on  fire_year.GridNum10km = burnt_carbon_10km.GridNum10km  
# MAGIC
# MAGIC where fire_2017 >0  and  nuts3_2021.GridNum10km =9713519511470080
# MAGIC
# MAGIC --nuts3_2021.GridNum10km =13854422035595264 ---AND fire_2022 + fire_2021+fire_2020+fire_2019+fire_2018+fire_2017+fire_2016+fire_2015+fire_2014  >0
# MAGIC
# MAGIC ---9713519511470080
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 5 QC

# COMMAND ----------




