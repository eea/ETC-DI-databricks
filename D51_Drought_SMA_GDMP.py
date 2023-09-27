# Databricks notebook source
# MAGIC %md # Carbon mapping - drought
# MAGIC
# MAGIC by Manuel
# MAGIC
# MAGIC
# MAGIC Aim:
# MAGIC The SQL query under L:\drought_indicator\scripts\sql\AreaAggregate reads the F table "[Climate_Impact].[drought].[F_DroughtImpact2022]" which contains the SM (soil moisture) and LINT timer series. 
# MAGIC
# MAGIC Id like to replace the LINT time series in this query with the new GDMP time series, which combines the 300m and the 1km time series.
# MAGIC
# MAGIC https://eea1.sharepoint.com/:u:/r/teams/-EXT-ETCDI/Shared%20Documents/3.2.2%20Drought%20and%20fire%20impact%20on%20C-%20Em%20and%20rem/drought_and_fire_workflow-%20overview.vsdx?d=wd52733c0b652416e99d66c43fe59df67&csf=1&web=1&e=kXY90N
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![](https://upload.wikimedia.org/wikipedia/commons/thumb/d/d3/California_Drought_Dry_Lakebed_2009.jpg/157px-California_Drought_Dry_Lakebed_2009.jpg)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 1) Reading DIMs

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
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (3) ENV zones (Metzger) ################################################################################                 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC // https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1519&fileId=544&successMessage=true
# MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_EnvZones_544_2020528_100m
# MAGIC
# MAGIC val parquetFileDF_env_zones = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_EnvZones_544_2020528_100m/")
# MAGIC parquetFileDF_env_zones.createOrReplaceTempView("env_zones")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (4) Organic-mineral soils ---Tanneberger 2017 ###############################################################################   100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC //    https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1957&fileId=982
# MAGIC //    cwsblobstorage01/cwsblob01/Dimensions/D_organicsoil_982_2023313_1km
# MAGIC //      1 Mineral soils
# MAGIC //      2 Organic soils (peatlands)
# MAGIC
# MAGIC val parquetFileDF_organic_soil = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_organicsoil_982_2023313_1km/")
# MAGIC parquetFileDF_organic_soil.createOrReplaceTempView("organic_soil")
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (5) LCF ##############################################################################                 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (6) Protected AREA (PA)  ##############################################################################                 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //    https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1910&fileId=935
# MAGIC //    cwsblobstorage01/cwsblob01/Dimensions/D_PA2022_100m_935_20221111_100m
# MAGIC val parquetFileDF_PA2022 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_PA2022_100m_935_20221111_100m/")
# MAGIC parquetFileDF_PA2022.createOrReplaceTempView("PA2022")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 13 (GDMP 1km  physical values 1999-2022)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2020&fileId=1042
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_pv_1042_2023918_1km
# MAGIC val parquetFileDF_gdmp_1km_pv = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_pv_1042_2023918_1km/")
# MAGIC parquetFileDF_gdmp_1km_pv.createOrReplaceTempView("gdmp_1km_pv")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 14 (GDMP 1km  STATISTICS 1999-2022)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km
# MAGIC val parquetFileDF_gdmp_1km_statistics = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km/")
# MAGIC parquetFileDF_gdmp_1km_statistics.createOrReplaceTempView("gdmp_1km_statistics")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC // we found GAPs in the time-series.. therefore we add. an attribute which shows the gaps [QC_gap_YES]
# MAGIC // if the attribute is 1, then this row should not be used for statistics OR a gab filling should be done:
# MAGIC
# MAGIC //val parquetFileDF_gdmp_1km_2 = spark.sql(""" 
# MAGIC //
# MAGIC //Select
# MAGIC //gridnum,
# MAGIC //GridNum10km,
# MAGIC //x,
# MAGIC //y,
# MAGIC //AreaHa,
# MAGIC //
# MAGIC //GDMP_1999_pv_1000m_EPSG3035 as GDMP_1999 ,
# MAGIC //GDMP_2000_pv_1000m_EPSG3035 as GDMP_2000 ,
# MAGIC //GDMP_2001_pv_1000m_EPSG3035 as GDMP_2001 ,
# MAGIC //GDMP_2002_pv_1000m_EPSG3035 as GDMP_2002 ,
# MAGIC //GDMP_2003_pv_1000m_EPSG3035 as GDMP_2003 ,
# MAGIC //GDMP_2004_pv_1000m_EPSG3035 as GDMP_2004 ,
# MAGIC //GDMP_2005_pv_1000m_EPSG3035 as GDMP_2005 ,
# MAGIC //GDMP_2006_pv_1000m_EPSG3035 as GDMP_2006 ,
# MAGIC //GDMP_2007_pv_1000m_EPSG3035 as GDMP_2007 ,
# MAGIC //GDMP_2008_pv_1000m_EPSG3035 as GDMP_2008 ,
# MAGIC //GDMP_2009_pv_1000m_EPSG3035 as GDMP_2009 ,
# MAGIC //GDMP_2010_pv_1000m_EPSG3035 as GDMP_2010 ,
# MAGIC //GDMP_2011_pv_1000m_EPSG3035 as GDMP_2011 ,
# MAGIC //GDMP_2012_pv_1000m_EPSG3035 as GDMP_2012 ,
# MAGIC //GDMP_2013_pv_1000m_EPSG3035 as GDMP_2013 ,
# MAGIC //GDMP_2014_pv_1000m_EPSG3035 as GDMP_2014 ,
# MAGIC //GDMP_2015_pv_1000m_EPSG3035 as GDMP_2015 ,
# MAGIC //GDMP_2016_pv_1000m_EPSG3035 as GDMP_2016 ,
# MAGIC //GDMP_2017_pv_1000m_EPSG3035 as GDMP_2017 ,
# MAGIC //GDMP_2018_pv_1000m_EPSG3035 as GDMP_2018 ,
# MAGIC //GDMP_2019_pv_1000m_EPSG3035 as GDMP_2019 ,
# MAGIC //
# MAGIC //if(GDMP_1999_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2000_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2001_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2002_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2003_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2004_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2005_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2006_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2007_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2008_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2009_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2010_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2011_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2012_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2013_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2014_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2015_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2016_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2017_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2018_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //if(GDMP_2019_pv_1000m_EPSG3035= 0 , 1 , 
# MAGIC //0)))))))))))))))))))))
# MAGIC //   as QC_gap_YES
# MAGIC //    from GDMP_1km_99_19_raw  
# MAGIC //                                                  
# MAGIC //                                                        """)                                  
# MAGIC //parquetFileDF_gdmp_1km_2.createOrReplaceTempView("GDMP_1km_99_19")  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (15) Drought ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //--------------------------------------------------------------- 
# MAGIC //1 Average drought pressure intensity 
# MAGIC
# MAGIC val parquetFileDF_Drought_avg_pre_inn1 = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_Drought_avg_pre_inn_818_2021823_100m/")
# MAGIC parquetFileDF_Drought_avg_pre_inn1.createOrReplaceTempView("Drought_avg_pre_inn1")
# MAGIC
# MAGIC //--------------------------------------------------------------- 
# MAGIC //2 Yearly average drought impact area  Drought_avg_imp_are
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1794&fileId=819
# MAGIC
# MAGIC val parquetFileDF_Drought_avg_imp_are = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_Drought_avg_imp_are_819_2021824_100m/")
# MAGIC parquetFileDF_Drought_avg_imp_are.createOrReplaceTempView("Drought_avg_imp_are2")
# MAGIC
# MAGIC //--------------------------------------------------------------- 
# MAGIC //3 Long term average drought impact intensity  Drought_avg_imp_inn
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1795&fileId=820
# MAGIC val parquetFileDF_Drought_avg_imp_inn3 = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_Drought_avg_imp_inn_820_2021824_100m/")
# MAGIC parquetFileDF_Drought_avg_imp_inn3.createOrReplaceTempView("Drought_avg_imp_inn3")
# MAGIC //-----------------------------------------
# MAGIC //4 Yearly average drought pressure area  Drought_avg_pre_are
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1796&fileId=821
# MAGIC
# MAGIC val parquetFileDF_Drought_avg_pre_are = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_Drought_avg_pre_are_821_2021824_100m/")
# MAGIC parquetFileDF_Drought_avg_pre_are.createOrReplaceTempView("Drought_avg_pre_are4")
# MAGIC //--------------------------------------------------------------- 
# MAGIC //5 Number of drought pressure events NrOfDroughtEvents
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1797&fileId=822  
# MAGIC val parquetFileDF_NrOfDroughtEvents = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_NrOfDroughtEvents_822_2021824_100m/")
# MAGIC parquetFileDF_NrOfDroughtEvents.createOrReplaceTempView("NrOfDroughtEvents5")
# MAGIC
# MAGIC //---------------------------------------------------------------
# MAGIC //6 Number of drought impact events NrOfDroughtImpactEve  
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1798&fileId=823  -error
# MAGIC val parquetFileDF_NrOfDroughtImpactEve = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_NrOfDroughtImpactEve_823_2021825_100m/")
# MAGIC parquetFileDF_NrOfDroughtImpactEve.createOrReplaceTempView("NrOfDroughtImpactEvents6")
# MAGIC
# MAGIC
# MAGIC //---------------------------------------------------------------
# MAGIC //// 7 (test) Drought Indicators 2022
# MAGIC ////https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1931&fileId=956
# MAGIC
# MAGIC val parquetFileDF_Drought_Indicators_2022 = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_DroughtIndicators_956_20221222_1km/")
# MAGIC parquetFileDF_Drought_Indicators_2022.createOrReplaceTempView("Drought_indicator")
# MAGIC
# MAGIC
# MAGIC val drop_parquetFileDF_Drought_Indicators_2022 = parquetFileDF_Drought_Indicators_2022.dropDuplicates("gridnum")
# MAGIC //dropDisDF_ADMIN.show(false)
# MAGIC drop_parquetFileDF_Drought_Indicators_2022.createOrReplaceTempView("Drought_indicator")
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2) QC of the uploaded DIMs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from gdmp_1km_pv  ---gdmp_1km_statistics
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS IN gdmp_1km_pv 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from gdmp_1km_statistics

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS IN gdmp_1km_statistics 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ---- testing 
# MAGIC Select
# MAGIC
# MAGIC  SUM(IF( coalesce(sma_gs_avg_2000,0) < -0.5 AND coalesce(LINT_anom_2000,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2000
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2001,0) < -0.5 AND coalesce(LINT_anom_2001,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2001
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2002,0) < -0.5 AND coalesce(LINT_anom_2002,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2002
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2003,0) < -0.5 AND coalesce(LINT_anom_2003,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2003
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2004,0) < -0.5 AND coalesce(LINT_anom_2004,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2004
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2005,0) < -0.5 AND coalesce(LINT_anom_2005,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2005
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2006,0) < -0.5 AND coalesce(LINT_anom_2006,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2006
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2007,0) < -0.5 AND coalesce(LINT_anom_2007,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2007
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2008,0) < -0.5 AND coalesce(LINT_anom_2008,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2008
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2009,0) < -0.5 AND coalesce(LINT_anom_2009,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2009
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2010,0) < -0.5 AND coalesce(LINT_anom_2010,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2010
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2011,0) < -0.5 AND coalesce(LINT_anom_2011,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2011
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2012,0) < -0.5 AND coalesce(LINT_anom_2012,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2012
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2013,0) < -0.5 AND coalesce(LINT_anom_2013,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2013
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2014,0) < -0.5 AND coalesce(LINT_anom_2014,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2014
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2015,0) < -0.5 AND coalesce(LINT_anom_2015,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2015
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2016,0) < -0.5 AND coalesce(LINT_anom_2016,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2016
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2017,0) < -0.5 AND coalesce(LINT_anom_2017,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2017
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2018,0) < -0.5 AND coalesce(LINT_anom_2018,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2018
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2019,0) < -0.5 AND coalesce(LINT_anom_2019,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2019
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2020,0) < -0.5 AND coalesce(LINT_anom_2020,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2020
# MAGIC  ,SUM(IF( coalesce(sma_gs_avg_2021,0) < -0.5 AND coalesce(LINT_anom_2021,0) <-0.5 ,  AreaHa/200,   0 )) as criteria_1_sum_2021
# MAGIC
# MAGIC
# MAGIC
# MAGIC from Drought_indicator

# COMMAND ----------

# MAGIC %md ## 3) Construction of CUBES

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## ANNEX Tools

# COMMAND ----------

# MAGIC %md ### ANNEX (A.1) Print alls columns:

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS IN AGB_STOCK2_CL_GDMP_1km_2018;
# MAGIC
