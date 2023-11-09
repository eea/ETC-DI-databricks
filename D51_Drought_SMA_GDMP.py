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
# MAGIC
# MAGIC INFOS about all needed DIMS can be found in our TEAMS:
# MAGIC https://eea1.sharepoint.com/:o:/r/teams/-EXT-ETCDI/Shared%20Documents/CSC2_T25%20Mapping%20European%20Carbon%20pools/TODO/to_do_list?d=wf9ce8765cd484eb89a7b2ef06d324bfc&csf=1&web=1&e=C9ielr
# MAGIC
# MAGIC Analytical units: 
# MAGIC - NUTS3,  
# MAGIC - 10km grid,  
# MAGIC - env zones, 
# MAGIC - LULUCF 
# MAGIC - Protected AREAS N2k 
# MAGIC - Protected AREAS NET
# MAGIC
# MAGIC USED DIMS:
# MAGIC - Annual (in growing season) drought pressure intensity  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2023&fileId=1045 
# MAGIC - Annual (in growing season) soil moisture anomaly https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2022&fileId=1044 
# MAGIC - annual nr of drought events (drought pressure occurrence) https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2023&fileId=1045 
# MAGIC - SMA trend https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2033&fileId=1055&successMessage=true 
# MAGIC - SMA pvalue https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2033&fileId=1055&successMessage=true 
# MAGIC - SMA relative change https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2033&fileId=1055&successMessage=true 
# MAGIC - SMA LTA (2001-2020) https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2034&fileId=1056&successMessage=true 
# MAGIC - annual drought impact intensity https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2035&fileId=1057&successMessage=true 
# MAGIC - GDMP physical values https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2009&fileId=1031 
# MAGIC - GDMP anomalies https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043 
# MAGIC - GDMP deviation https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043 
# MAGIC - GDMP LTA https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043 
# MAGIC - GDMP LTSD https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043 
# MAGIC - GDMP trend https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true 
# MAGIC - GDMP pvalue https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true 
# MAGIC - GDMP slope https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true 
# MAGIC - GDMP relative change https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true 
# MAGIC - LTA drought pressure intensity https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2032&fileId=1054&successMessage=true 
# MAGIC - LTA drought pressure occurrence https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2032&fileId=1054&successMessage=true 
# MAGIC - LTA drought impact intensity https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2032&fileId=1054&successMessage=true 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC
# MAGIC

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
# MAGIC //// (1) CLC 2018 ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the lULUCF classes:
# MAGIC // Reading CLC2018 100m DIM:.....
# MAGIC
# MAGIC val parquetFileDF_clc18 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_A_CLC_18_210_20181129_100m/")
# MAGIC parquetFileDF_clc18.createOrReplaceTempView("CLC_2018")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (2) ClULUCF classes ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the lULUCF classes:
# MAGIC // Reading CLC2018 100m DIM:.....
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
# MAGIC
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
# MAGIC //D_PA2022_100m_935_20221111_100m
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1910&fileId=935
# MAGIC
# MAGIC //D_PA2022_100m_935_20221111_100m
# MAGIC /// PA2022:
# MAGIC
# MAGIC val PA2022_100m_v2 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_PA2022_100m_935_20221111_100m/")             /// use load
# MAGIC PA2022_100m_v2.createOrReplaceTempView("PA2022_100m_v2")
# MAGIC
# MAGIC // ------------------------------------------------
# MAGIC // code	protection type
# MAGIC // 1	Natura 2000 only  -n2k
# MAGIC // 10	CDDA only
# MAGIC // 11	Natura2000 and CDDA -nk2
# MAGIC // 100 Emerald only
# MAGIC // 101 Emerald and Natura 2000 * -n2k
# MAGIC // 110	CDDA and Emerald
# MAGIC // 111	CDDA, Natura2000 and Emerald * -n2k
# MAGIC // -----------------------------------------
# MAGIC
# MAGIC val parquetFileDF_natura2000 = spark.sql(""" 
# MAGIC Select 
# MAGIC   gridnum,
# MAGIC   GridNum10km,
# MAGIC   IF(ProtectedArea2022_10m in (1,11,101,111), 'Natura2000' ,'not proteced') as natura2000_protection,
# MAGIC   AreaHa
# MAGIC from PA2022_100m_v2
# MAGIC where ProtectedArea2022_10m in (1,11,101,111)
# MAGIC """)
# MAGIC parquetFileDF_natura2000.createOrReplaceTempView("Natura2000_100m_NET")  
# MAGIC
# MAGIC //// also set up a full PA dim net: protected and not protected:
# MAGIC
# MAGIC val parquetFileDF_pa_all = spark.sql(""" 
# MAGIC Select 
# MAGIC   gridnum,
# MAGIC   GridNum10km,
# MAGIC   IF(ProtectedArea2022_10m >0, 'protected' ,'not proteced') as PA_2022_protection,
# MAGIC   AreaHa
# MAGIC from PA2022_100m_v2
# MAGIC
# MAGIC """)
# MAGIC parquetFileDF_pa_all.createOrReplaceTempView("Pa2022_100m_NET")  
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////19 SMA growing seasion gs - timerseries 1km 
# MAGIC //##########################################################################################################################################
# MAGIC //SMA_gs_1km_annual
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2022&fileId=1044
# MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_SMA_gs_1km_annual_1044_2023920_1km
# MAGIC val SMA_gs_1km_annual = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_SMA_gs_1km_annual_1044_2023920_1km/")
# MAGIC SMA_gs_1km_annual.createOrReplaceTempView("SMA_gs_1km_annual")
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
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////21 SMA_statistics 1km  
# MAGIC //##########################################################################################################################################
# MAGIC ///SMA trend https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2033&fileId=1055&successMessage=true
# MAGIC ///SMA pvalue https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2033&fileId=1055&successMessage=true
# MAGIC ///SMA relative change https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2033&fileId=1055&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_SMA_statistics_1055_2023105_1km 
# MAGIC val SMA_statistics = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_SMA_statistics_1055_2023105_1km/")
# MAGIC SMA_statistics.createOrReplaceTempView("SMA_statistics")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////22 LTA_SMA_background 1km  
# MAGIC //##########################################################################################################################################
# MAGIC ///SMA LTA (2001-2020) https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2034&fileId=1056&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_LTA_SMA_background_1056_2023105_1km
# MAGIC val LTA_SMA_background = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_LTA_SMA_background_1056_2023105_1km/")
# MAGIC LTA_SMA_background.createOrReplaceTempView("LTA_SMA_background")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////23 annual drought impact intensity 1 km  -time series
# MAGIC //##########################################################################################################################################
# MAGIC ///annual drought impact intensity https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2035&fileId=1057&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_a_drought_imp_inten_1057_2023105_1km
# MAGIC val a_drought_imp_inten  = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_a_drought_imp_inten_1057_2023105_1km/")
# MAGIC a_drought_imp_inten.createOrReplaceTempView("a_drought_imp_inten")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////24 GDMP anomalies  1 km  -time series
# MAGIC //##########################################################################################################################################
# MAGIC ///GDMP anomalies https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC ///GDMP deviation https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC ///GDMP LTA https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC ///GDMP LTSD https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km
# MAGIC val gdmp_1km_statistic_c  = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km/")
# MAGIC gdmp_1km_statistic_c.createOrReplaceTempView("gdmp_1km_statistic_c")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////25 GDMP 1999-2019 1km -analysis  1 km 
# MAGIC //##########################################################################################################################################
# MAGIC ///GDMP trend           https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true
# MAGIC ///GDMP pvalue          https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true
# MAGIC ///GDMP slope           https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true
# MAGIC ///GDMP relative change https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_GDMP_1km_trend_analy_1051_2023928_1km
# MAGIC val GDMP_1km_trend_analy  = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_GDMP_1km_trend_analy_1051_2023928_1km/")
# MAGIC GDMP_1km_trend_analy.createOrReplaceTempView("GDMP_1km_trend_analy")
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC ////26  LTA  1 km 
# MAGIC //##########################################################################################################################################
# MAGIC ///LTA drought pressure intensity   https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2032&fileId=1054&successMessage=true
# MAGIC ///LTA drought pressure occurrence  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2032&fileId=1054&successMessage=true
# MAGIC ///LTA drought impact intensity     https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2032&fileId=1054&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_LTA_analysis_1054_2023103_1km
# MAGIC val LTA_analysis  = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_LTA_analysis_1054_2023103_1km/")
# MAGIC LTA_analysis.createOrReplaceTempView("LTA_analysis")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 10 (GDMP 1km  physical values 1999-2022)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2020&fileId=1042
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_pv_1042_2023918_1km
# MAGIC val parquetFileDF_gdmp_1km_pv = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_pv_1042_2023918_1km/")
# MAGIC parquetFileDF_gdmp_1km_pv.createOrReplaceTempView("gdmp_1km_pv")
# MAGIC ///GDMP physical values https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2009&fileId=1031
# MAGIC
# MAGIC
# MAGIC
# MAGIC ///overview:..........................................................................................
# MAGIC /// 0 D_drought_press_occure - Annual (in growing season) drought pressure intensity
# MAGIC /// 0 D_drought_press_occure - Annual (in growing season) soil moisture anomaly 
# MAGIC /// 0 D_drought_press_occure -	annual nr of drought events (drought pressure occurrence) 
# MAGIC /// 
# MAGIC /// 1 SMA_statistics - Annual SMA trend 
# MAGIC /// 1 SMA_statistics - Annual SMA pvalue
# MAGIC /// 1 SMA_statistics - Annual SMA relative change 
# MAGIC /// 
# MAGIC /// 2 LTA_SMA_background - SMA LTA (2001-2020) 
# MAGIC /// 
# MAGIC /// 3 a_drought_imp_inten - annual drought impact intensity 
# MAGIC /// 
# MAGIC /// 4 gdmp_1km_statistic_c - GDMP anomalies
# MAGIC /// 4 gdmp_1km_statistic_c - GDMP deviation
# MAGIC /// 4 gdmp_1km_statistic_c - GDMP LTA
# MAGIC /// 4 gdmp_1km_statistic_c - GDMP LTSD 
# MAGIC /// 
# MAGIC /// 5 GDMP_1km_trend_analy - GDMP trend 
# MAGIC /// 5 GDMP_1km_trend_analy - GDMP pvalue 
# MAGIC /// 5 GDMP_1km_trend_analy - GDMP slope 
# MAGIC /// 5 GDMP_1km_trend_analy - GDMP relative change
# MAGIC /// 
# MAGIC /// 6 LTA_analysis - LTA drought pressure intensity
# MAGIC /// 6 LTA_analysis - LTA drought pressure occurrence 
# MAGIC /// 6 LTA_analysis - LTA drought impact intensity
# MAGIC /// 
# MAGIC /// 0 gdmp_1km_pv - GDMP physical values
# MAGIC /// SMA growing seasion gs - timerseries 1km 

# COMMAND ----------

# MAGIC %md ### 1.2) QC of the uploaded DIMs

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### 1.2) Build the MAIN Referencedataset
# MAGIC -combination of NUTS3, 10km, LULUCF2018, PA , N2k and  evn.Zones

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC /// REFCUBE . with 1km >
# MAGIC //
# MAGIC val ref_cube = spark.sql("""
# MAGIC                 SELECT 
# MAGIC   
# MAGIC                     nuts3_2021.Category as admin_category, ----FOR ADMIN
# MAGIC                     nuts3_2021.GridNum10km,
# MAGIC                     nuts3_2021.GridNum1km,
# MAGIC                     if(lULUCF_2018.LULUCF_CODE is null, 'none',lULUCF_2018.LULUCF_CODE) as LULUCF_CODE,
# MAGIC                     if(env_zones.Category is null, 'none',env_zones.Category) as env_zones,
# MAGIC                     if(natura2000_protection is null, 'none Nature 2000 protection',natura2000_protection) as natura2000_protection,
# MAGIC                     ----,if(PA_2022_protection == 'protected','protected', 'not protected') as Pa2022_100m_NET
# MAGIC                      SUM(nuts3_2021.AreaHa) as AreaHa
# MAGIC
# MAGIC                     from nuts3_2021
# MAGIC                     LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC                     LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC                     LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC                   ----  LEFT JOIN Pa2022_100m_NET     on nuts3_2021.GridNum = Pa2022_100m_NET.GridNum
# MAGIC
# MAGIC                     where nuts3_2021.ISO2 is not null
# MAGIC
# MAGIC                         group by 
# MAGIC                         nuts3_2021.Category,
# MAGIC                         nuts3_2021.GridNum1km,
# MAGIC                         nuts3_2021.GridNum10km,
# MAGIC                         lULUCF_2018.LULUCF_CODE,
# MAGIC                         env_zones.Category ,
# MAGIC                         natura2000_protection 
# MAGIC                         --,Pa2022_100m_NET
# MAGIC
# MAGIC             """)
# MAGIC ref_cube
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube")
# MAGIC
# MAGIC  ref_cube.createOrReplaceTempView("ref_cube")
# MAGIC
# MAGIC  
# MAGIC /// REFCUBE . without 1km >
# MAGIC //
# MAGIC val ref_cube2 = spark.sql("""
# MAGIC                 SELECT 
# MAGIC   
# MAGIC                     nuts3_2021.Category as admin_category, ----FOR ADMIN
# MAGIC                     nuts3_2021.GridNum10km,
# MAGIC     
# MAGIC                     if(lULUCF_2018.LULUCF_CODE is null, 'none',lULUCF_2018.LULUCF_CODE) as LULUCF_CODE,
# MAGIC                     if(env_zones.Category is null, 'none',env_zones.Category) as env_zones,
# MAGIC                     if(natura2000_protection is null, 'none Nature 2000 protection',natura2000_protection) as natura2000_protection,
# MAGIC                     ----,if(PA_2022_protection == 'protected','protected', 'not protected') as Pa2022_100m_NET
# MAGIC                      SUM(nuts3_2021.AreaHa) as AreaHa
# MAGIC
# MAGIC                     from nuts3_2021
# MAGIC                     LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC                     LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC                     LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC                   ----  LEFT JOIN Pa2022_100m_NET     on nuts3_2021.GridNum = Pa2022_100m_NET.GridNum
# MAGIC
# MAGIC                     where nuts3_2021.ISO2 is not null
# MAGIC
# MAGIC                         group by 
# MAGIC                         nuts3_2021.Category,
# MAGIC                   
# MAGIC                         nuts3_2021.GridNum10km,
# MAGIC                         lULUCF_2018.LULUCF_CODE,
# MAGIC                         env_zones.Category ,
# MAGIC                         natura2000_protection 
# MAGIC                         --,Pa2022_100m_NET
# MAGIC
# MAGIC             """)
# MAGIC ref_cube2
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube2")
# MAGIC
# MAGIC  ref_cube2.createOrReplaceTempView("ref_cube_2")
# MAGIC
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
print ("-------------------------------------------------------------------------------------")
print ("Ref-cube 1 - with 1km:")
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)


print ("-------------------------------------------------------------------------------------")
print ("Ref-cube 1 - without 1km:")
### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube2"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)


# COMMAND ----------

# MAGIC %scala
# MAGIC /// If you have prodcued the REF-table allready..you can also use the following READ script:
# MAGIC val ref_cube_read  = spark.read.format("csv")
# MAGIC   .options(Map("delimiter"->"|"))
# MAGIC   .option("header", "true")  /// first row = header
# MAGIC   .load("https://cwsblobstorage01.blob.core.windows.net/cwsblob01/ExportTable/Carbon_mapping/ref_cube/part-00000-tid-2913451643038522271-31183585-a954-4730-8b69-98b75712bc1a-506-1-c000.csv.gz") 
# MAGIC ref_cube_read.createOrReplaceTempView("ref_cube")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ref_cube
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2) Construction of CUBES

# COMMAND ----------

# MAGIC %md ### 2.1) Construction of CUBE 1
# MAGIC - annual drought pressure area = nr of gridcells expressed in km2 with SMA< -1    
# MAGIC --  [D_drought_press_occure] 20 
# MAGIC - annual drought pressure intensity = AVR SMA when SMA<-1                        
# MAGIC --  [D_drought_press_occure] 20
# MAGIC - annual nr of drought events = SUM of event                                      
# MAGIC --  [D_drought_press_occure] 20
# MAGIC - annual SM condition = AVR of SMA      
# MAGIC -- [SMA_gs_1km_annual]      19                                      
# MAGIC - LTA SMA (LTRA derived from 2001-2020)                                           
# MAGIC -- [LTA_SMA_background] 22
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------




# COMMAND ----------

# MAGIC %sql ----TESTING
# MAGIC SELECT  
# MAGIC  ref_cube.admin_category
# MAGIC ,ref_cube.GridNum10km
# MAGIC ,ref_cube.GridNum1km
# MAGIC ,ref_cube.LULUCF_CODE
# MAGIC ,ref_cube.env_zones
# MAGIC ,ref_cube.natura2000_protection
# MAGIC ,ref_cube.AreaHa 
# MAGIC --, if(drought_pressure_intensity_gs_1km_2022 IS NOT NULL, ref_cube.AreaHa ,0) as annual_drought_pressure_area_2022 ---test
# MAGIC ---,if(drought_pressure_intensity_gs_1km_2000 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2000
# MAGIC -- Question from Eva: why is the above line duplicated?
# MAGIC
# MAGIC ,if(drought_pressure_intensity_gs_1km_2000 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2000
# MAGIC ,if(drought_pressure_intensity_gs_1km_2001 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2001
# MAGIC ,if(drought_pressure_intensity_gs_1km_2002 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2002
# MAGIC ,if(drought_pressure_intensity_gs_1km_2003 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2003
# MAGIC ,if(drought_pressure_intensity_gs_1km_2004 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2004
# MAGIC ,if(drought_pressure_intensity_gs_1km_2005 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2005
# MAGIC ,if(drought_pressure_intensity_gs_1km_2006 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2006
# MAGIC ,if(drought_pressure_intensity_gs_1km_2007 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2007
# MAGIC ,if(drought_pressure_intensity_gs_1km_2008 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2008
# MAGIC ,if(drought_pressure_intensity_gs_1km_2009 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2009
# MAGIC ,if(drought_pressure_intensity_gs_1km_2010 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2010
# MAGIC ,if(drought_pressure_intensity_gs_1km_2011 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2011
# MAGIC ,if(drought_pressure_intensity_gs_1km_2012 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2012
# MAGIC ,if(drought_pressure_intensity_gs_1km_2013 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2013
# MAGIC ,if(drought_pressure_intensity_gs_1km_2014 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2014
# MAGIC ,if(drought_pressure_intensity_gs_1km_2015 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2015
# MAGIC ,if(drought_pressure_intensity_gs_1km_2016 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2016
# MAGIC ,if(drought_pressure_intensity_gs_1km_2017 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2017
# MAGIC ,if(drought_pressure_intensity_gs_1km_2018 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2018
# MAGIC ,if(drought_pressure_intensity_gs_1km_2019 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2019
# MAGIC ,if(drought_pressure_intensity_gs_1km_2020 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2020
# MAGIC ,if(drought_pressure_intensity_gs_1km_2021 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2021
# MAGIC ,if(drought_pressure_intensity_gs_1km_2022 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2022
# MAGIC
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2000, 0) as drought_pressure_intensity_gs_1km_2000
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2001, 0) as drought_pressure_intensity_gs_1km_2001
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2002, 0) as drought_pressure_intensity_gs_1km_2002
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2003, 0) as drought_pressure_intensity_gs_1km_2003
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2004, 0) as drought_pressure_intensity_gs_1km_2004
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2005, 0) as drought_pressure_intensity_gs_1km_2005
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2006, 0) as drought_pressure_intensity_gs_1km_2006
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2007, 0) as drought_pressure_intensity_gs_1km_2007
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2008, 0) as drought_pressure_intensity_gs_1km_2008
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2009, 0) as drought_pressure_intensity_gs_1km_2009
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2010, 0) as drought_pressure_intensity_gs_1km_2010
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2011, 0) as drought_pressure_intensity_gs_1km_2011
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2012, 0) as drought_pressure_intensity_gs_1km_2012
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2013, 0) as drought_pressure_intensity_gs_1km_2013
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2014, 0) as drought_pressure_intensity_gs_1km_2014
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2015, 0) as drought_pressure_intensity_gs_1km_2015
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2016, 0) as drought_pressure_intensity_gs_1km_2016
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2017, 0) as drought_pressure_intensity_gs_1km_2017
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2018, 0) as drought_pressure_intensity_gs_1km_2018
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2019, 0) as drought_pressure_intensity_gs_1km_2019
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2020, 0) as drought_pressure_intensity_gs_1km_2020
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2021, 0) as drought_pressure_intensity_gs_1km_2021
# MAGIC ,ifnull(drought_pressure_intensity_gs_1km_2022, 0) as drought_pressure_intensity_gs_1km_2022
# MAGIC
# MAGIC
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2000, 0) as drought_pressure_occurrence_gs_1km_2000
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2001, 0) as drought_pressure_occurrence_gs_1km_2001
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2002, 0) as drought_pressure_occurrence_gs_1km_2002
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2003, 0) as drought_pressure_occurrence_gs_1km_2003
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2004, 0) as drought_pressure_occurrence_gs_1km_2004
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2005, 0) as drought_pressure_occurrence_gs_1km_2005
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2006, 0) as drought_pressure_occurrence_gs_1km_2006
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2007, 0) as drought_pressure_occurrence_gs_1km_2007
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2008, 0) as drought_pressure_occurrence_gs_1km_2008
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2009, 0) as drought_pressure_occurrence_gs_1km_2009
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2010, 0) as drought_pressure_occurrence_gs_1km_2010
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2011, 0) as drought_pressure_occurrence_gs_1km_2011
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2012, 0) as drought_pressure_occurrence_gs_1km_2012
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2013, 0) as drought_pressure_occurrence_gs_1km_2013
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2014, 0) as drought_pressure_occurrence_gs_1km_2014
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2015, 0) as drought_pressure_occurrence_gs_1km_2015
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2016, 0) as drought_pressure_occurrence_gs_1km_2016
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2017, 0) as drought_pressure_occurrence_gs_1km_2017
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2018, 0) as drought_pressure_occurrence_gs_1km_2018
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2019, 0) as drought_pressure_occurrence_gs_1km_2019
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2020, 0) as drought_pressure_occurrence_gs_1km_2020
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2021, 0) as drought_pressure_occurrence_gs_1km_2021
# MAGIC ,ifnull(drought_pressure_occurrence_gs_1km_2022, 0) as drought_pressure_occurrence_gs_1km_2022
# MAGIC
# MAGIC ,ifnull(SMA_gs_1km_annual_2000, 0) as SMA_gs_1km_annual_2000
# MAGIC ,ifnull(SMA_gs_1km_annual_2001, 0) as SMA_gs_1km_annual_2001
# MAGIC ,ifnull(SMA_gs_1km_annual_2002, 0) as SMA_gs_1km_annual_2002
# MAGIC ,ifnull(SMA_gs_1km_annual_2003, 0) as SMA_gs_1km_annual_2003
# MAGIC ,ifnull(SMA_gs_1km_annual_2004, 0) as SMA_gs_1km_annual_2004
# MAGIC ,ifnull(SMA_gs_1km_annual_2005, 0) as SMA_gs_1km_annual_2005
# MAGIC ,ifnull(SMA_gs_1km_annual_2006, 0) as SMA_gs_1km_annual_2006
# MAGIC ,ifnull(SMA_gs_1km_annual_2007, 0) as SMA_gs_1km_annual_2007
# MAGIC ,ifnull(SMA_gs_1km_annual_2008, 0) as SMA_gs_1km_annual_2008
# MAGIC ,ifnull(SMA_gs_1km_annual_2009, 0) as SMA_gs_1km_annual_2009
# MAGIC ,ifnull(SMA_gs_1km_annual_2010, 0) as SMA_gs_1km_annual_2010
# MAGIC ,ifnull(SMA_gs_1km_annual_2011, 0) as SMA_gs_1km_annual_2011
# MAGIC ,ifnull(SMA_gs_1km_annual_2012, 0) as SMA_gs_1km_annual_2012
# MAGIC ,ifnull(SMA_gs_1km_annual_2013, 0) as SMA_gs_1km_annual_2013
# MAGIC ,ifnull(SMA_gs_1km_annual_2014, 0) as SMA_gs_1km_annual_2014
# MAGIC ,ifnull(SMA_gs_1km_annual_2015, 0) as SMA_gs_1km_annual_2015
# MAGIC ,ifnull(SMA_gs_1km_annual_2016, 0) as SMA_gs_1km_annual_2016
# MAGIC ,ifnull(SMA_gs_1km_annual_2017, 0) as SMA_gs_1km_annual_2017
# MAGIC ,ifnull(SMA_gs_1km_annual_2018, 0) as SMA_gs_1km_annual_2018
# MAGIC ,ifnull(SMA_gs_1km_annual_2019, 0) as SMA_gs_1km_annual_2019
# MAGIC ,ifnull(SMA_gs_1km_annual_2020, 0) as SMA_gs_1km_annual_2020
# MAGIC ,ifnull(SMA_gs_1km_annual_2021, 0) as SMA_gs_1km_annual_2021
# MAGIC ,ifnull(SMA_gs_1km_annual_2022, 0) as SMA_gs_1km_annual_2022
# MAGIC
# MAGIC
# MAGIC
# MAGIC           
# MAGIC ,ifnull(LTA_SMA_2001to2020, 0) as  LTA_SMA_2001to2020
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC from
# MAGIC   ref_cube
# MAGIC   LEFT JOIN D_drought_press_occure on D_drought_press_occure.gridnum   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC   LEFT JOIN SMA_gs_1km_annual on SMA_gs_1km_annual.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC   LEFT JOIN LTA_SMA_background on LTA_SMA_background.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %scala
# MAGIC /// Set up F-CUBE (1)
# MAGIC val cube_1_f = spark.sql("""              
# MAGIC                         SELECT  
# MAGIC                             ref_cube.admin_category
# MAGIC                             ,ref_cube.GridNum10km
# MAGIC                             ,ref_cube.GridNum1km
# MAGIC                             ,ref_cube.LULUCF_CODE
# MAGIC                             ,ref_cube.env_zones
# MAGIC                             ,ref_cube.natura2000_protection
# MAGIC                             ,ref_cube.AreaHa 
# MAGIC                             --, if(drought_pressure_intensity_gs_1km_2022 IS NOT NULL, ref_cube.AreaHa ,0) as annual_drought_pressure_area_2022 ---test
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2000 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2000
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2001 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2001
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2002 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2002
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2003 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2003
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2004 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2004
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2005 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2005
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2006 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2006
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2007 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2007
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2008 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2008
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2009 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2009
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2010 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2010
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2011 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2011
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2012 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2012
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2013 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2013
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2014 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2014
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2015 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2015
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2016 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2016
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2017 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2017
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2018 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2018
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2019 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2019
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2020 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2020
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2021 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2021
# MAGIC                             ,if(drought_pressure_intensity_gs_1km_2022 IS NOT NULL, ref_cube.AreaHa, 0)  as annual_drought_pressure_area_2022
# MAGIC
# MAGIC                             ,drought_pressure_intensity_gs_1km_2000
# MAGIC                             ,drought_pressure_intensity_gs_1km_2001
# MAGIC                             ,drought_pressure_intensity_gs_1km_2002
# MAGIC                             ,drought_pressure_intensity_gs_1km_2003
# MAGIC                             ,drought_pressure_intensity_gs_1km_2004
# MAGIC                             ,drought_pressure_intensity_gs_1km_2005
# MAGIC                             ,drought_pressure_intensity_gs_1km_2006
# MAGIC                             ,drought_pressure_intensity_gs_1km_2007
# MAGIC                             ,drought_pressure_intensity_gs_1km_2008
# MAGIC                             ,drought_pressure_intensity_gs_1km_2009
# MAGIC                             ,drought_pressure_intensity_gs_1km_2010
# MAGIC                             ,drought_pressure_intensity_gs_1km_2011
# MAGIC                             ,drought_pressure_intensity_gs_1km_2012
# MAGIC                             ,drought_pressure_intensity_gs_1km_2013
# MAGIC                             ,drought_pressure_intensity_gs_1km_2014
# MAGIC                             ,drought_pressure_intensity_gs_1km_2015
# MAGIC                             ,drought_pressure_intensity_gs_1km_2016
# MAGIC                             ,drought_pressure_intensity_gs_1km_2017
# MAGIC                             ,drought_pressure_intensity_gs_1km_2018
# MAGIC                             ,drought_pressure_intensity_gs_1km_2019
# MAGIC                             ,drought_pressure_intensity_gs_1km_2020
# MAGIC                             ,drought_pressure_intensity_gs_1km_2021
# MAGIC                             ,drought_pressure_intensity_gs_1km_2022
# MAGIC
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2000
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2001
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2002
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2003
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2004
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2005
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2006
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2007
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2008
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2009
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2010
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2011
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2012
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2013
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2014
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2015
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2016
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2017
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2018
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2019
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2020
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2021
# MAGIC                             ,drought_pressure_occurrence_gs_1km_2022
# MAGIC
# MAGIC                             ,SMA_gs_1km_annual_2000
# MAGIC                             ,SMA_gs_1km_annual_2001
# MAGIC                             ,SMA_gs_1km_annual_2002
# MAGIC                             ,SMA_gs_1km_annual_2003
# MAGIC                             ,SMA_gs_1km_annual_2004
# MAGIC                             ,SMA_gs_1km_annual_2005
# MAGIC                             ,SMA_gs_1km_annual_2006
# MAGIC                             ,SMA_gs_1km_annual_2007
# MAGIC                             ,SMA_gs_1km_annual_2008
# MAGIC                             ,SMA_gs_1km_annual_2009
# MAGIC                             ,SMA_gs_1km_annual_2010
# MAGIC                             ,SMA_gs_1km_annual_2011
# MAGIC                             ,SMA_gs_1km_annual_2012
# MAGIC                             ,SMA_gs_1km_annual_2013
# MAGIC                             ,SMA_gs_1km_annual_2014
# MAGIC                             ,SMA_gs_1km_annual_2015
# MAGIC                             ,SMA_gs_1km_annual_2016
# MAGIC                             ,SMA_gs_1km_annual_2017
# MAGIC                             ,SMA_gs_1km_annual_2018
# MAGIC                             ,SMA_gs_1km_annual_2019
# MAGIC                             ,SMA_gs_1km_annual_2020
# MAGIC                             ,SMA_gs_1km_annual_2021
# MAGIC                             ,SMA_gs_1km_annual_2022
# MAGIC                     
# MAGIC                             ,LTA_SMA_2001to2020 ---ifnull(LTA_SMA_2001to2020, 0) as  LTA_SMA_2001to2020
# MAGIC                             from
# MAGIC                             ref_cube
# MAGIC                             LEFT JOIN D_drought_press_occure on D_drought_press_occure.gridnum   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC                             LEFT JOIN SMA_gs_1km_annual on SMA_gs_1km_annual.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC                             LEFT JOIN LTA_SMA_background on LTA_SMA_background.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC                           
# MAGIC
# MAGIC                                                       """)                                  
# MAGIC cube_1_f.createOrReplaceTempView("cube_1_f")  
# MAGIC
# MAGIC
# MAGIC /// 
# MAGIC val cube_1_c = spark.sql("""                                 
# MAGIC   select
# MAGIC    admin_category
# MAGIC   ,LULUCF_CODE
# MAGIC   
# MAGIC   ,GridNum10km
# MAGIC   ,natura2000_protection
# MAGIC   ,env_zones  
# MAGIC
# MAGIC   --- weighted avg: 
# MAGIC   --,---SUM(GDMP_collection_1km.GDMP_1km_anom_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_1999 
# MAGIC
# MAGIC
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2022* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2022
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2021* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2021
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2020* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2020
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2019* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2019
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2018* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2018
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2017* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2017
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2016* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2016
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2015* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2015
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2014* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2014
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2013* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2013
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2012* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2012
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2011* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2011
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2010* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2010
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2009* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2009
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2008* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2008
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2007* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2007
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2006* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2006
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2005* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2005
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2004* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2004
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2003* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2003
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2002* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2002
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2001* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2001
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2000* AreaHa )/ SUM(AreaHa)  as drought_pressure_occurrence_gs_1km_2000  
# MAGIC
# MAGIC
# MAGIC
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2022* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2022
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2021* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2021
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2020* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2020
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2019* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2019
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2018* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2018
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2017* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2017
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2016* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2016
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2015* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2015
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2014* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2014
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2013* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2013
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2012* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2012
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2011* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2011
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2010* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2010
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2009* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2009
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2008* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2008
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2007* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2007
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2006* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2006
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2005* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2005
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2004* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2004
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2003* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2003
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2002* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2002
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2001* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2001
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2000* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2000 
# MAGIC
# MAGIC   ,SUM(annual_drought_pressure_area_2022) as annual_drought_pressure_area_2022
# MAGIC   ,SUM(annual_drought_pressure_area_2021) as annual_drought_pressure_area_2021
# MAGIC   ,SUM(annual_drought_pressure_area_2020) as annual_drought_pressure_area_2020
# MAGIC   ,SUM(annual_drought_pressure_area_2019) as annual_drought_pressure_area_2019
# MAGIC   ,SUM(annual_drought_pressure_area_2018) as annual_drought_pressure_area_2018
# MAGIC   ,SUM(annual_drought_pressure_area_2017) as annual_drought_pressure_area_2017
# MAGIC   ,SUM(annual_drought_pressure_area_2016) as annual_drought_pressure_area_2016
# MAGIC   ,SUM(annual_drought_pressure_area_2015) as annual_drought_pressure_area_2015
# MAGIC   ,SUM(annual_drought_pressure_area_2014) as annual_drought_pressure_area_2014
# MAGIC   ,SUM(annual_drought_pressure_area_2013) as annual_drought_pressure_area_2013
# MAGIC   ,SUM(annual_drought_pressure_area_2012) as annual_drought_pressure_area_2012
# MAGIC   ,SUM(annual_drought_pressure_area_2011) as annual_drought_pressure_area_2011
# MAGIC   ,SUM(annual_drought_pressure_area_2010) as annual_drought_pressure_area_2010
# MAGIC   ,SUM(annual_drought_pressure_area_2009) as annual_drought_pressure_area_2009
# MAGIC   ,SUM(annual_drought_pressure_area_2008) as annual_drought_pressure_area_2008
# MAGIC   ,SUM(annual_drought_pressure_area_2007) as annual_drought_pressure_area_2007
# MAGIC   ,SUM(annual_drought_pressure_area_2006) as annual_drought_pressure_area_2006
# MAGIC   ,SUM(annual_drought_pressure_area_2005) as annual_drought_pressure_area_2005
# MAGIC   ,SUM(annual_drought_pressure_area_2004) as annual_drought_pressure_area_2004
# MAGIC   ,SUM(annual_drought_pressure_area_2003) as annual_drought_pressure_area_2003
# MAGIC   ,SUM(annual_drought_pressure_area_2002) as annual_drought_pressure_area_2002
# MAGIC   ,SUM(annual_drought_pressure_area_2001) as annual_drought_pressure_area_2001
# MAGIC   ,SUM(annual_drought_pressure_area_2000) as annual_drought_pressure_area_2000  
# MAGIC
# MAGIC   ,SUM(SMA_gs_1km_annual_2022 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2022
# MAGIC   ,SUM(SMA_gs_1km_annual_2021 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2021
# MAGIC   ,SUM(SMA_gs_1km_annual_2020 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2020
# MAGIC   ,SUM(SMA_gs_1km_annual_2019 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2019
# MAGIC   ,SUM(SMA_gs_1km_annual_2018 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2018
# MAGIC   ,SUM(SMA_gs_1km_annual_2017 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2017
# MAGIC   ,SUM(SMA_gs_1km_annual_2016 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2016
# MAGIC   ,SUM(SMA_gs_1km_annual_2015 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2015
# MAGIC   ,SUM(SMA_gs_1km_annual_2014 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2014
# MAGIC   ,SUM(SMA_gs_1km_annual_2013 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2013
# MAGIC   ,SUM(SMA_gs_1km_annual_2012 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2012
# MAGIC   ,SUM(SMA_gs_1km_annual_2011 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2011
# MAGIC   ,SUM(SMA_gs_1km_annual_2010 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2010
# MAGIC   ,SUM(SMA_gs_1km_annual_2009 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2009
# MAGIC   ,SUM(SMA_gs_1km_annual_2008 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2008
# MAGIC   ,SUM(SMA_gs_1km_annual_2007 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2007
# MAGIC   ,SUM(SMA_gs_1km_annual_2006 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2006
# MAGIC   ,SUM(SMA_gs_1km_annual_2005 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2005
# MAGIC   ,SUM(SMA_gs_1km_annual_2004 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2004
# MAGIC   ,SUM(SMA_gs_1km_annual_2003 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2003
# MAGIC   ,SUM(SMA_gs_1km_annual_2002 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2002
# MAGIC   ,SUM(SMA_gs_1km_annual_2001 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2001
# MAGIC   ,SUM(SMA_gs_1km_annual_2000 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2000  
# MAGIC   ,AVG(LTA_SMA_2001to2020) as LTA_SMA_2001to2020
# MAGIC   ,SUM(AreaHa) as AreaHa  
# MAGIC
# MAGIC
# MAGIC   from  cube_1_f  
# MAGIC
# MAGIC   group by
# MAGIC    admin_category
# MAGIC   ,LULUCF_CODE
# MAGIC   
# MAGIC   ,GridNum10km
# MAGIC   ,natura2000_protection
# MAGIC   ,env_zones  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC                                                       """)                                  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC cube_1_c
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/drought_sma_gdmp/cube_1_c")
# MAGIC
# MAGIC     //cube_1_c.createOrReplaceTempView("cube_1_c")  
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/drought_sma_gdmp/cube_1_c"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print ("-------------------------------------")
        print ("CUBE drought -sma -gdmp - 1c can be downloaded using following link:")
        print (URL)

# COMMAND ----------

# MAGIC %sql
# MAGIC ----testing
# MAGIC select
# MAGIC admin_category
# MAGIC   ,LULUCF_CODE
# MAGIC
# MAGIC   ,GridNum10km
# MAGIC   ,natura2000_protection
# MAGIC   ,env_zones  
# MAGIC
# MAGIC   --- weighted avg: 
# MAGIC   --,---SUM(GDMP_collection_1km.GDMP_1km_anom_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_1999 
# MAGIC
# MAGIC
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2022) as drought_pressure_occurrence_gs_1km_2022
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2021) as drought_pressure_occurrence_gs_1km_2021
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2020) as drought_pressure_occurrence_gs_1km_2020
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2019) as drought_pressure_occurrence_gs_1km_2019
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2018) as drought_pressure_occurrence_gs_1km_2018
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2017) as drought_pressure_occurrence_gs_1km_2017
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2016) as drought_pressure_occurrence_gs_1km_2016
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2015) as drought_pressure_occurrence_gs_1km_2015
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2014) as drought_pressure_occurrence_gs_1km_2014
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2013) as drought_pressure_occurrence_gs_1km_2013
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2012) as drought_pressure_occurrence_gs_1km_2012
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2011) as drought_pressure_occurrence_gs_1km_2011
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2010) as drought_pressure_occurrence_gs_1km_2010
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2009) as drought_pressure_occurrence_gs_1km_2009
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2008) as drought_pressure_occurrence_gs_1km_2008
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2007) as drought_pressure_occurrence_gs_1km_2007
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2006) as drought_pressure_occurrence_gs_1km_2006
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2005) as drought_pressure_occurrence_gs_1km_2005
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2004) as drought_pressure_occurrence_gs_1km_2004
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2003) as drought_pressure_occurrence_gs_1km_2003
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2002) as drought_pressure_occurrence_gs_1km_2002
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2001) as drought_pressure_occurrence_gs_1km_2001
# MAGIC   ,SUM(drought_pressure_occurrence_gs_1km_2000) as drought_pressure_occurrence_gs_1km_2000  
# MAGIC
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2022* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2022
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2021* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2021
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2020* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2020
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2019* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2019
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2018* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2018
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2017* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2017
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2016* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2016
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2015* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2015
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2014* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2014
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2013* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2013
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2012* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2012
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2011* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2011
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2010* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2010
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2009* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2009
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2008* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2008
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2007* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2007
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2006* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2006
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2005* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2005
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2004* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2004
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2003* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2003
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2002* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2002
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2001* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2001
# MAGIC   ,SUM(drought_pressure_intensity_gs_1km_2000* AreaHa )/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2000 
# MAGIC
# MAGIC   ,SUM(annual_drought_pressure_area_2022) as annual_drought_pressure_area_2022
# MAGIC   ,SUM(annual_drought_pressure_area_2021) as annual_drought_pressure_area_2021
# MAGIC   ,SUM(annual_drought_pressure_area_2020) as annual_drought_pressure_area_2020
# MAGIC   ,SUM(annual_drought_pressure_area_2019) as annual_drought_pressure_area_2019
# MAGIC   ,SUM(annual_drought_pressure_area_2018) as annual_drought_pressure_area_2018
# MAGIC   ,SUM(annual_drought_pressure_area_2017) as annual_drought_pressure_area_2017
# MAGIC   ,SUM(annual_drought_pressure_area_2016) as annual_drought_pressure_area_2016
# MAGIC   ,SUM(annual_drought_pressure_area_2015) as annual_drought_pressure_area_2015
# MAGIC   ,SUM(annual_drought_pressure_area_2014) as annual_drought_pressure_area_2014
# MAGIC   ,SUM(annual_drought_pressure_area_2013) as annual_drought_pressure_area_2013
# MAGIC   ,SUM(annual_drought_pressure_area_2012) as annual_drought_pressure_area_2012
# MAGIC   ,SUM(annual_drought_pressure_area_2011) as annual_drought_pressure_area_2011
# MAGIC   ,SUM(annual_drought_pressure_area_2010) as annual_drought_pressure_area_2010
# MAGIC   ,SUM(annual_drought_pressure_area_2009) as annual_drought_pressure_area_2009
# MAGIC   ,SUM(annual_drought_pressure_area_2008) as annual_drought_pressure_area_2008
# MAGIC   ,SUM(annual_drought_pressure_area_2007) as annual_drought_pressure_area_2007
# MAGIC   ,SUM(annual_drought_pressure_area_2006) as annual_drought_pressure_area_2006
# MAGIC   ,SUM(annual_drought_pressure_area_2005) as annual_drought_pressure_area_2005
# MAGIC   ,SUM(annual_drought_pressure_area_2004) as annual_drought_pressure_area_2004
# MAGIC   ,SUM(annual_drought_pressure_area_2003) as annual_drought_pressure_area_2003
# MAGIC   ,SUM(annual_drought_pressure_area_2002) as annual_drought_pressure_area_2002
# MAGIC   ,SUM(annual_drought_pressure_area_2001) as annual_drought_pressure_area_2001
# MAGIC   ,SUM(annual_drought_pressure_area_2000) as annual_drought_pressure_area_2000  
# MAGIC
# MAGIC   ,SUM(SMA_gs_1km_annual_2022 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2022
# MAGIC   ,SUM(SMA_gs_1km_annual_2021 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2021
# MAGIC   ,SUM(SMA_gs_1km_annual_2020 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2020
# MAGIC   ,SUM(SMA_gs_1km_annual_2019 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2019
# MAGIC   ,SUM(SMA_gs_1km_annual_2018 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2018
# MAGIC   ,SUM(SMA_gs_1km_annual_2017 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2017
# MAGIC   ,SUM(SMA_gs_1km_annual_2016 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2016
# MAGIC   ,SUM(SMA_gs_1km_annual_2015 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2015
# MAGIC   ,SUM(SMA_gs_1km_annual_2014 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2014
# MAGIC   ,SUM(SMA_gs_1km_annual_2013 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2013
# MAGIC   ,SUM(SMA_gs_1km_annual_2012 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2012
# MAGIC   ,SUM(SMA_gs_1km_annual_2011 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2011
# MAGIC   ,SUM(SMA_gs_1km_annual_2010 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2010
# MAGIC   ,SUM(SMA_gs_1km_annual_2009 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2009
# MAGIC   ,SUM(SMA_gs_1km_annual_2008 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2008
# MAGIC   ,SUM(SMA_gs_1km_annual_2007 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2007
# MAGIC   ,SUM(SMA_gs_1km_annual_2006 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2006
# MAGIC   ,SUM(SMA_gs_1km_annual_2005 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2005
# MAGIC   ,SUM(SMA_gs_1km_annual_2004 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2004
# MAGIC   ,SUM(SMA_gs_1km_annual_2003 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2003
# MAGIC   ,SUM(SMA_gs_1km_annual_2002 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2002
# MAGIC   ,SUM(SMA_gs_1km_annual_2001 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2001
# MAGIC   ,SUM(SMA_gs_1km_annual_2000 * AreaHa) / SUM(AreaHa) as SMA_gs_1km_annual_2000  
# MAGIC
# MAGIC   ,SUM(AreaHa) as AreaHa  
# MAGIC
# MAGIC
# MAGIC   from  cube_1_f  
# MAGIC
# MAGIC   group by
# MAGIC    admin_category
# MAGIC   ,LULUCF_CODE
# MAGIC   ,LTA_SMA_2001to2020
# MAGIC   ,GridNum10km
# MAGIC   ,natura2000_protection
# MAGIC   ,env_zones  
# MAGIC  
# MAGIC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC --testing 
# MAGIC select
# MAGIC  admin_category
# MAGIC ,LULUCF_CODE
# MAGIC ,LTA_SMA_2001to2020
# MAGIC ,GridNum10km
# MAGIC ,natura2000_protection
# MAGIC ,env_zones
# MAGIC
# MAGIC --- weighted avg: 
# MAGIC --,---SUM(GDMP_collection_1km.GDMP_1km_anom_1999*GDMP_collection_1km.AreaHa)/ sum(GDMP_collection_1km.AreaHa)   as GDMP_1km_anom_1999
# MAGIC
# MAGIC
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2022) as drought_pressure_occurrence_gs_1km_2022
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2021) as drought_pressure_occurrence_gs_1km_2021
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2020) as drought_pressure_occurrence_gs_1km_2020
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2019) as drought_pressure_occurrence_gs_1km_2019
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2018) as drought_pressure_occurrence_gs_1km_2018
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2017) as drought_pressure_occurrence_gs_1km_2017
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2016) as drought_pressure_occurrence_gs_1km_2016
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2015) as drought_pressure_occurrence_gs_1km_2015
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2014) as drought_pressure_occurrence_gs_1km_2014
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2013) as drought_pressure_occurrence_gs_1km_2013
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2012) as drought_pressure_occurrence_gs_1km_2012
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2011) as drought_pressure_occurrence_gs_1km_2011
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2010) as drought_pressure_occurrence_gs_1km_2010
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2009) as drought_pressure_occurrence_gs_1km_2009
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2008) as drought_pressure_occurrence_gs_1km_2008
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2007) as drought_pressure_occurrence_gs_1km_2007
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2006) as drought_pressure_occurrence_gs_1km_2006
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2005) as drought_pressure_occurrence_gs_1km_2005
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2004) as drought_pressure_occurrence_gs_1km_2004
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2003) as drought_pressure_occurrence_gs_1km_2003
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2002) as drought_pressure_occurrence_gs_1km_2002
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2001) as drought_pressure_occurrence_gs_1km_2001
# MAGIC ,SUM(drought_pressure_occurrence_gs_1km_2000) as drought_pressure_occurrence_gs_1km_2000
# MAGIC
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2022)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2022
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2021)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2021
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2020)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2020
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2019)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2019
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2018)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2018
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2017)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2017
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2016)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2016
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2015)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2015
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2014)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2014
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2013)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2013
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2012)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2012
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2011)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2011
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2010)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2010
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2009)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2009
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2008)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2008
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2007)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2007
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2006)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2006
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2005)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2005
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2004)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2004
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2003)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2003
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2002)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2002
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2001)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2001
# MAGIC ,SUM(drought_pressure_intensity_gs_1km_2000)/ SUM(AreaHa) as drought_pressure_intensity_gs_1km_2000
# MAGIC
# MAGIC ,SUM(annual_drought_pressure_area_2022) as annual_drought_pressure_area_2022
# MAGIC ,SUM(annual_drought_pressure_area_2021) as annual_drought_pressure_area_2021
# MAGIC ,SUM(annual_drought_pressure_area_2020) as annual_drought_pressure_area_2020
# MAGIC ,SUM(annual_drought_pressure_area_2019) as annual_drought_pressure_area_2019
# MAGIC ,SUM(annual_drought_pressure_area_2018) as annual_drought_pressure_area_2018
# MAGIC ,SUM(annual_drought_pressure_area_2017) as annual_drought_pressure_area_2017
# MAGIC ,SUM(annual_drought_pressure_area_2016) as annual_drought_pressure_area_2016
# MAGIC ,SUM(annual_drought_pressure_area_2015) as annual_drought_pressure_area_2015
# MAGIC ,SUM(annual_drought_pressure_area_2014) as annual_drought_pressure_area_2014
# MAGIC ,SUM(annual_drought_pressure_area_2013) as annual_drought_pressure_area_2013
# MAGIC ,SUM(annual_drought_pressure_area_2012) as annual_drought_pressure_area_2012
# MAGIC ,SUM(annual_drought_pressure_area_2011) as annual_drought_pressure_area_2011
# MAGIC ,SUM(annual_drought_pressure_area_2010) as annual_drought_pressure_area_2010
# MAGIC ,SUM(annual_drought_pressure_area_2009) as annual_drought_pressure_area_2009
# MAGIC ,SUM(annual_drought_pressure_area_2008) as annual_drought_pressure_area_2008
# MAGIC ,SUM(annual_drought_pressure_area_2007) as annual_drought_pressure_area_2007
# MAGIC ,SUM(annual_drought_pressure_area_2006) as annual_drought_pressure_area_2006
# MAGIC ,SUM(annual_drought_pressure_area_2005) as annual_drought_pressure_area_2005
# MAGIC ,SUM(annual_drought_pressure_area_2004) as annual_drought_pressure_area_2004
# MAGIC ,SUM(annual_drought_pressure_area_2003) as annual_drought_pressure_area_2003
# MAGIC ,SUM(annual_drought_pressure_area_2002) as annual_drought_pressure_area_2002
# MAGIC ,SUM(annual_drought_pressure_area_2001) as annual_drought_pressure_area_2001
# MAGIC ,SUM(annual_drought_pressure_area_2000) as annual_drought_pressure_area_2000
# MAGIC
# MAGIC ,SUM(SMA_gs_1km_annual_2022) / SUM(AreaHa) as SMA_gs_1km_annual_2022
# MAGIC ,SUM(SMA_gs_1km_annual_2021) / SUM(AreaHa) as SMA_gs_1km_annual_2021
# MAGIC ,SUM(SMA_gs_1km_annual_2020) / SUM(AreaHa) as SMA_gs_1km_annual_2020
# MAGIC ,SUM(SMA_gs_1km_annual_2019) / SUM(AreaHa) as SMA_gs_1km_annual_2019
# MAGIC ,SUM(SMA_gs_1km_annual_2018) / SUM(AreaHa) as SMA_gs_1km_annual_2018
# MAGIC ,SUM(SMA_gs_1km_annual_2017) / SUM(AreaHa) as SMA_gs_1km_annual_2017
# MAGIC ,SUM(SMA_gs_1km_annual_2016) / SUM(AreaHa) as SMA_gs_1km_annual_2016
# MAGIC ,SUM(SMA_gs_1km_annual_2015) / SUM(AreaHa) as SMA_gs_1km_annual_2015
# MAGIC ,SUM(SMA_gs_1km_annual_2014) / SUM(AreaHa) as SMA_gs_1km_annual_2014
# MAGIC ,SUM(SMA_gs_1km_annual_2013) / SUM(AreaHa) as SMA_gs_1km_annual_2013
# MAGIC ,SUM(SMA_gs_1km_annual_2012) / SUM(AreaHa) as SMA_gs_1km_annual_2012
# MAGIC ,SUM(SMA_gs_1km_annual_2011) / SUM(AreaHa) as SMA_gs_1km_annual_2011
# MAGIC ,SUM(SMA_gs_1km_annual_2010) / SUM(AreaHa) as SMA_gs_1km_annual_2010
# MAGIC ,SUM(SMA_gs_1km_annual_2009) / SUM(AreaHa) as SMA_gs_1km_annual_2009
# MAGIC ,SUM(SMA_gs_1km_annual_2008) / SUM(AreaHa) as SMA_gs_1km_annual_2008
# MAGIC ,SUM(SMA_gs_1km_annual_2007) / SUM(AreaHa) as SMA_gs_1km_annual_2007
# MAGIC ,SUM(SMA_gs_1km_annual_2006) / SUM(AreaHa) as SMA_gs_1km_annual_2006
# MAGIC ,SUM(SMA_gs_1km_annual_2005) / SUM(AreaHa) as SMA_gs_1km_annual_2005
# MAGIC ,SUM(SMA_gs_1km_annual_2004) / SUM(AreaHa) as SMA_gs_1km_annual_2004
# MAGIC ,SUM(SMA_gs_1km_annual_2003) / SUM(AreaHa) as SMA_gs_1km_annual_2003
# MAGIC ,SUM(SMA_gs_1km_annual_2002) / SUM(AreaHa) as SMA_gs_1km_annual_2002
# MAGIC ,SUM(SMA_gs_1km_annual_2001) / SUM(AreaHa) as SMA_gs_1km_annual_2001
# MAGIC ,SUM(SMA_gs_1km_annual_2000) / SUM(AreaHa) as SMA_gs_1km_annual_2000
# MAGIC
# MAGIC ,SUM(AreaHa) as AreaHa
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC from  cube_1_f
# MAGIC
# MAGIC group by
# MAGIC  admin_category
# MAGIC ,LULUCF_CODE
# MAGIC ,LTA_SMA_2001to2020
# MAGIC ,GridNum10km
# MAGIC ,natura2000_protection
# MAGIC ,env_zones
# MAGIC
# MAGIC
# MAGIC limit 100
# MAGIC

# COMMAND ----------

# MAGIC %md ### 2.2) Construction of CUBE 2
# MAGIC - annual productivity condition = SUM of GDMP values   DIM: gdmp_1km_pv
# MAGIC - annual drought impact area = nr of gridcells expressed in km2 where SMA<-1 and GDMPaomalies <0 (GDMPa = GDMP anomalies)  
# MAGIC - annual drought impact intensity, relative = AVR of GDMPGDMPaomalies where SMA < -1 AND GDMPGDMPaomalies <0  
# MAGIC - annual drought impact intensity, absolute = SUM of GDMP where SMA <-1 AND GDMPGDMPaomalies

# COMMAND ----------

# MAGIC %sql ----TESTING CUBE 2
# MAGIC SELECT  
# MAGIC  ref_cube.admin_category
# MAGIC ,ref_cube.GridNum10km
# MAGIC ,ref_cube.GridNum1km
# MAGIC ,ref_cube.LULUCF_CODE
# MAGIC ,ref_cube.env_zones
# MAGIC ,ref_cube.natura2000_protection
# MAGIC ,ref_cube.AreaHa 
# MAGIC
# MAGIC ,if(GDMP_2000_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2000_pv_1000m_EPSG3035,0)           as GDMP_2000_pv
# MAGIC ,if(GDMP_2001_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2001_pv_1000m_EPSG3035,0)           as GDMP_2001_pv
# MAGIC ,if(GDMP_2002_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2002_pv_1000m_EPSG3035,0)           as GDMP_2002_pv
# MAGIC ,if(GDMP_2003_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2003_pv_1000m_EPSG3035,0)           as GDMP_2003_pv
# MAGIC ,if(GDMP_2004_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2004_pv_1000m_EPSG3035,0)           as GDMP_2004_pv
# MAGIC ,if(GDMP_2005_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2005_pv_1000m_EPSG3035,0)           as GDMP_2005_pv
# MAGIC ,if(GDMP_2006_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2006_pv_1000m_EPSG3035,0)           as GDMP_2006_pv
# MAGIC ,if(GDMP_2007_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2007_pv_1000m_EPSG3035,0)           as GDMP_2007_pv
# MAGIC ,if(GDMP_2008_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2008_pv_1000m_EPSG3035,0)           as GDMP_2008_pv
# MAGIC ,if(GDMP_2009_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2009_pv_1000m_EPSG3035,0)           as GDMP_2009_pv
# MAGIC ,if(GDMP_2010_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2010_pv_1000m_EPSG3035,0)           as GDMP_2010_pv
# MAGIC ,if(GDMP_2011_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2011_pv_1000m_EPSG3035,0)           as GDMP_2011_pv
# MAGIC ,if(GDMP_2012_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2012_pv_1000m_EPSG3035,0)           as GDMP_2012_pv
# MAGIC ,if(GDMP_2013_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2013_pv_1000m_EPSG3035,0)           as GDMP_2013_pv
# MAGIC ,if(GDMP_2014_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2014_pv_1000m_EPSG3035,0)           as GDMP_2014_pv
# MAGIC ,if(GDMP_2015_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2015_pv_1000m_EPSG3035,0)           as GDMP_2015_pv
# MAGIC ,if(GDMP_2016_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2016_pv_1000m_EPSG3035,0)           as GDMP_2016_pv
# MAGIC ,if(GDMP_2017_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2017_pv_1000m_EPSG3035,0)           as GDMP_2017_pv
# MAGIC ,if(GDMP_2018_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2018_pv_1000m_EPSG3035,0)           as GDMP_2018_pv
# MAGIC ,if(GDMP_2019_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2019_pv_1000m_EPSG3035,0)           as GDMP_2019_pv
# MAGIC ,if(GDMP_2020_pv_300to1000m_EPSG3035 IS NOT NULL,GDMP_2020_pv_300to1000m_EPSG3035,0)  as GDMP_2020_pv
# MAGIC ,if(GDMP_2021_pv_300to1000m_EPSG3035 IS NOT NULL,GDMP_2021_pv_300to1000m_EPSG3035,0)  as GDMP_2021_pv
# MAGIC ,if(GDMP_2022_pv_300to1000m_EPSG3035 IS NOT NULL,GDMP_2022_pv_300to1000m_EPSG3035,0)  as GDMP_2022_pv
# MAGIC
# MAGIC ,IF(SMA_gs_1km_annual_2000<-1 AND GDMP_1km_anom_2000 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2000
# MAGIC ,IF(SMA_gs_1km_annual_2001<-1 AND GDMP_1km_anom_2001 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2001
# MAGIC ,IF(SMA_gs_1km_annual_2002<-1 AND GDMP_1km_anom_2002 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2002
# MAGIC ,IF(SMA_gs_1km_annual_2003<-1 AND GDMP_1km_anom_2003 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2003
# MAGIC ,IF(SMA_gs_1km_annual_2004<-1 AND GDMP_1km_anom_2004 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2004
# MAGIC ,IF(SMA_gs_1km_annual_2005<-1 AND GDMP_1km_anom_2005 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2005
# MAGIC ,IF(SMA_gs_1km_annual_2006<-1 AND GDMP_1km_anom_2006 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2006
# MAGIC ,IF(SMA_gs_1km_annual_2007<-1 AND GDMP_1km_anom_2007 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2007
# MAGIC ,IF(SMA_gs_1km_annual_2008<-1 AND GDMP_1km_anom_2008 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2008
# MAGIC ,IF(SMA_gs_1km_annual_2009<-1 AND GDMP_1km_anom_2009 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2009
# MAGIC ,IF(SMA_gs_1km_annual_2010<-1 AND GDMP_1km_anom_2010 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2010
# MAGIC ,IF(SMA_gs_1km_annual_2011<-1 AND GDMP_1km_anom_2011 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2011
# MAGIC ,IF(SMA_gs_1km_annual_2012<-1 AND GDMP_1km_anom_2012 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2012
# MAGIC ,IF(SMA_gs_1km_annual_2013<-1 AND GDMP_1km_anom_2013 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2013
# MAGIC ,IF(SMA_gs_1km_annual_2014<-1 AND GDMP_1km_anom_2014 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2014
# MAGIC ,IF(SMA_gs_1km_annual_2015<-1 AND GDMP_1km_anom_2015 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2015
# MAGIC ,IF(SMA_gs_1km_annual_2016<-1 AND GDMP_1km_anom_2016 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2016
# MAGIC ,IF(SMA_gs_1km_annual_2017<-1 AND GDMP_1km_anom_2017 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2017
# MAGIC ,IF(SMA_gs_1km_annual_2018<-1 AND GDMP_1km_anom_2018 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2018
# MAGIC ,IF(SMA_gs_1km_annual_2019<-1 AND GDMP_1km_anom_2019 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2019
# MAGIC ,IF(SMA_gs_1km_annual_2020<-1 AND GDMP_1km_anom_2020 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2020
# MAGIC ,IF(SMA_gs_1km_annual_2021<-1 AND GDMP_1km_anom_2021 <0, ref_cube.AreaHa ,0)      			        as annual_drought_impact_area_2021
# MAGIC ,IF(SMA_gs_1km_annual_2022<-1 AND GDMP_1km_anom_2022 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2022
# MAGIC
# MAGIC
# MAGIC
# MAGIC ,IF(SMA_gs_1km_annual_2000<-1 AND GDMP_1km_anom_2000<0, GDMP_1km_anom_2000 ,0 ) as annual_drought_impact_intensity_relative_2000 
# MAGIC ,IF(SMA_gs_1km_annual_2001<-1 AND GDMP_1km_anom_2001<0, GDMP_1km_anom_2001 ,0 ) as annual_drought_impact_intensity_relative_2001  
# MAGIC ,IF(SMA_gs_1km_annual_2002<-1 AND GDMP_1km_anom_2002<0, GDMP_1km_anom_2002 ,0 ) as annual_drought_impact_intensity_relative_2002 
# MAGIC ,IF(SMA_gs_1km_annual_2003<-1 AND GDMP_1km_anom_2003<0, GDMP_1km_anom_2003 ,0 ) as annual_drought_impact_intensity_relative_2003   
# MAGIC ,IF(SMA_gs_1km_annual_2004<-1 AND GDMP_1km_anom_2004<0, GDMP_1km_anom_2004 ,0 ) as annual_drought_impact_intensity_relative_2004 
# MAGIC ,IF(SMA_gs_1km_annual_2005<-1 AND GDMP_1km_anom_2005<0, GDMP_1km_anom_2005 ,0 ) as annual_drought_impact_intensity_relative_2005   
# MAGIC ,IF(SMA_gs_1km_annual_2006<-1 AND GDMP_1km_anom_2006<0, GDMP_1km_anom_2006 ,0 ) as annual_drought_impact_intensity_relative_2006 
# MAGIC ,IF(SMA_gs_1km_annual_2007<-1 AND GDMP_1km_anom_2007<0, GDMP_1km_anom_2007 ,0 ) as annual_drought_impact_intensity_relative_2007   
# MAGIC ,IF(SMA_gs_1km_annual_2008<-1 AND GDMP_1km_anom_2008<0, GDMP_1km_anom_2008 ,0 ) as annual_drought_impact_intensity_relative_2008 
# MAGIC ,IF(SMA_gs_1km_annual_2009<-1 AND GDMP_1km_anom_2009<0, GDMP_1km_anom_2009 ,0 ) as annual_drought_impact_intensity_relative_2009   
# MAGIC ,IF(SMA_gs_1km_annual_2010<-1 AND GDMP_1km_anom_2010<0, GDMP_1km_anom_2010 ,0 ) as annual_drought_impact_intensity_relative_2010 
# MAGIC ,IF(SMA_gs_1km_annual_2011<-1 AND GDMP_1km_anom_2011<0, GDMP_1km_anom_2011 ,0 ) as annual_drought_impact_intensity_relative_2011   
# MAGIC ,IF(SMA_gs_1km_annual_2012<-1 AND GDMP_1km_anom_2012<0, GDMP_1km_anom_2012 ,0 ) as annual_drought_impact_intensity_relative_2012 
# MAGIC ,IF(SMA_gs_1km_annual_2013<-1 AND GDMP_1km_anom_2013<0, GDMP_1km_anom_2013 ,0 ) as annual_drought_impact_intensity_relative_2013   
# MAGIC ,IF(SMA_gs_1km_annual_2014<-1 AND GDMP_1km_anom_2014<0, GDMP_1km_anom_2014 ,0 ) as annual_drought_impact_intensity_relative_2014 
# MAGIC ,IF(SMA_gs_1km_annual_2015<-1 AND GDMP_1km_anom_2015<0, GDMP_1km_anom_2015 ,0 ) as annual_drought_impact_intensity_relative_2015   
# MAGIC ,IF(SMA_gs_1km_annual_2016<-1 AND GDMP_1km_anom_2016<0, GDMP_1km_anom_2016 ,0 ) as annual_drought_impact_intensity_relative_2016 
# MAGIC ,IF(SMA_gs_1km_annual_2017<-1 AND GDMP_1km_anom_2017<0, GDMP_1km_anom_2017 ,0 ) as annual_drought_impact_intensity_relative_2017   
# MAGIC ,IF(SMA_gs_1km_annual_2018<-1 AND GDMP_1km_anom_2018<0, GDMP_1km_anom_2018 ,0 ) as annual_drought_impact_intensity_relative_2018 
# MAGIC ,IF(SMA_gs_1km_annual_2019<-1 AND GDMP_1km_anom_2019<0, GDMP_1km_anom_2019 ,0 ) as annual_drought_impact_intensity_relative_2019   
# MAGIC ,IF(SMA_gs_1km_annual_2020<-1 AND GDMP_1km_anom_2020<0, GDMP_1km_anom_2020 ,0 ) as annual_drought_impact_intensity_relative_2020 
# MAGIC ,IF(SMA_gs_1km_annual_2021<-1 AND GDMP_1km_anom_2021<0, GDMP_1km_anom_2021 ,0 ) as annual_drought_impact_intensity_relative_2021   
# MAGIC ,IF(SMA_gs_1km_annual_2022<-1 AND GDMP_1km_anom_2022<0, GDMP_1km_anom_2022 ,0 ) as annual_drought_impact_intensity_relative_2022 
# MAGIC
# MAGIC
# MAGIC ,IF(SMA_gs_1km_annual_2000<-1 AND GDMP_1km_anom_2000 <0, GDMP_2000_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2000 
# MAGIC ,IF(SMA_gs_1km_annual_2001<-1 AND GDMP_1km_anom_2001 <0, GDMP_2001_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2001 
# MAGIC ,IF(SMA_gs_1km_annual_2002<-1 AND GDMP_1km_anom_2002 <0, GDMP_2002_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2002 
# MAGIC ,IF(SMA_gs_1km_annual_2003<-1 AND GDMP_1km_anom_2003 <0, GDMP_2003_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2003 
# MAGIC ,IF(SMA_gs_1km_annual_2004<-1 AND GDMP_1km_anom_2004 <0, GDMP_2004_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2004 
# MAGIC ,IF(SMA_gs_1km_annual_2005<-1 AND GDMP_1km_anom_2005 <0, GDMP_2005_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2005 
# MAGIC ,IF(SMA_gs_1km_annual_2006<-1 AND GDMP_1km_anom_2006 <0, GDMP_2006_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2006 
# MAGIC ,IF(SMA_gs_1km_annual_2007<-1 AND GDMP_1km_anom_2007 <0, GDMP_2007_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2007 
# MAGIC ,IF(SMA_gs_1km_annual_2008<-1 AND GDMP_1km_anom_2008 <0, GDMP_2008_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2008 
# MAGIC ,IF(SMA_gs_1km_annual_2009<-1 AND GDMP_1km_anom_2009 <0, GDMP_2009_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2009 
# MAGIC ,IF(SMA_gs_1km_annual_2010<-1 AND GDMP_1km_anom_2010 <0, GDMP_2010_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2010 
# MAGIC ,IF(SMA_gs_1km_annual_2011<-1 AND GDMP_1km_anom_2011 <0, GDMP_2011_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2011 
# MAGIC ,IF(SMA_gs_1km_annual_2012<-1 AND GDMP_1km_anom_2012 <0, GDMP_2012_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2012 
# MAGIC ,IF(SMA_gs_1km_annual_2013<-1 AND GDMP_1km_anom_2013 <0, GDMP_2013_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2013 
# MAGIC ,IF(SMA_gs_1km_annual_2014<-1 AND GDMP_1km_anom_2014 <0, GDMP_2014_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2014 
# MAGIC ,IF(SMA_gs_1km_annual_2015<-1 AND GDMP_1km_anom_2015 <0, GDMP_2015_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2015 
# MAGIC ,IF(SMA_gs_1km_annual_2016<-1 AND GDMP_1km_anom_2016 <0, GDMP_2016_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2016 
# MAGIC ,IF(SMA_gs_1km_annual_2017<-1 AND GDMP_1km_anom_2017 <0, GDMP_2017_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2017 
# MAGIC ,IF(SMA_gs_1km_annual_2018<-1 AND GDMP_1km_anom_2018 <0, GDMP_2018_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2018 
# MAGIC ,IF(SMA_gs_1km_annual_2019<-1 AND GDMP_1km_anom_2019 <0, GDMP_2019_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2019 
# MAGIC ,IF(SMA_gs_1km_annual_2020<-1 AND GDMP_1km_anom_2020 <0, GDMP_2020_pv_300to1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2020 
# MAGIC ,IF(SMA_gs_1km_annual_2021<-1 AND GDMP_1km_anom_2021 <0, GDMP_2021_pv_300to1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2021 
# MAGIC ,IF(SMA_gs_1km_annual_2022<-1 AND GDMP_1km_anom_2022 <0, GDMP_2022_pv_300to1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2022 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ----annual productivity condition = SUM of GDMP values DIM: gdmp_1km_pv
# MAGIC ----annual drought impact area = nr of gridcells expressed in km2 where SMA<-1 and GDMPaomalies <0 (GDMPa = GDMP anomalies)
# MAGIC ----annual drought impact intensity, relative = AVR of GDMPGDMPaomalies where SMA < -1 AND GDMPGDMPaomalies <0
# MAGIC ----annual drought impact intensity, absolute = SUM of GDMP where SMA <-1 AND GDMPGDMPaomalies <0
# MAGIC
# MAGIC
# MAGIC from
# MAGIC   ref_cube
# MAGIC LEFT JOIN gdmp_1km_pv on gdmp_1km_pv.gridnum   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC LEFT JOIN SMA_gs_1km_annual on SMA_gs_1km_annual.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC LEFT JOIN gdmp_1km_statistic_c on gdmp_1km_statistic_c.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC
# MAGIC limit 10

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC /// Set up F-CUBE (2)
# MAGIC val cube_2_f = spark.sql("""              
# MAGIC SELECT  
# MAGIC  ref_cube.admin_category
# MAGIC ,ref_cube.GridNum10km
# MAGIC ,ref_cube.GridNum1km
# MAGIC ,ref_cube.LULUCF_CODE
# MAGIC ,ref_cube.env_zones
# MAGIC ,ref_cube.natura2000_protection
# MAGIC ,ref_cube.AreaHa 
# MAGIC
# MAGIC ,if(GDMP_2000_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2000_pv_1000m_EPSG3035,0)           as GDMP_2000_pv
# MAGIC ,if(GDMP_2001_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2001_pv_1000m_EPSG3035,0)           as GDMP_2001_pv
# MAGIC ,if(GDMP_2002_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2002_pv_1000m_EPSG3035,0)           as GDMP_2002_pv
# MAGIC ,if(GDMP_2003_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2003_pv_1000m_EPSG3035,0)           as GDMP_2003_pv
# MAGIC ,if(GDMP_2004_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2004_pv_1000m_EPSG3035,0)           as GDMP_2004_pv
# MAGIC ,if(GDMP_2005_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2005_pv_1000m_EPSG3035,0)           as GDMP_2005_pv
# MAGIC ,if(GDMP_2006_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2006_pv_1000m_EPSG3035,0)           as GDMP_2006_pv
# MAGIC ,if(GDMP_2007_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2007_pv_1000m_EPSG3035,0)           as GDMP_2007_pv
# MAGIC ,if(GDMP_2008_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2008_pv_1000m_EPSG3035,0)           as GDMP_2008_pv
# MAGIC ,if(GDMP_2009_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2009_pv_1000m_EPSG3035,0)           as GDMP_2009_pv
# MAGIC ,if(GDMP_2010_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2010_pv_1000m_EPSG3035,0)           as GDMP_2010_pv
# MAGIC ,if(GDMP_2011_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2011_pv_1000m_EPSG3035,0)           as GDMP_2011_pv
# MAGIC ,if(GDMP_2012_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2012_pv_1000m_EPSG3035,0)           as GDMP_2012_pv
# MAGIC ,if(GDMP_2013_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2013_pv_1000m_EPSG3035,0)           as GDMP_2013_pv
# MAGIC ,if(GDMP_2014_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2014_pv_1000m_EPSG3035,0)           as GDMP_2014_pv
# MAGIC ,if(GDMP_2015_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2015_pv_1000m_EPSG3035,0)           as GDMP_2015_pv
# MAGIC ,if(GDMP_2016_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2016_pv_1000m_EPSG3035,0)           as GDMP_2016_pv
# MAGIC ,if(GDMP_2017_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2017_pv_1000m_EPSG3035,0)           as GDMP_2017_pv
# MAGIC ,if(GDMP_2018_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2018_pv_1000m_EPSG3035,0)           as GDMP_2018_pv
# MAGIC ,if(GDMP_2019_pv_1000m_EPSG3035  IS NOT NULL,GDMP_2019_pv_1000m_EPSG3035,0)           as GDMP_2019_pv
# MAGIC ,if(GDMP_2020_pv_300to1000m_EPSG3035 IS NOT NULL,GDMP_2020_pv_300to1000m_EPSG3035,0)  as GDMP_2020_pv
# MAGIC ,if(GDMP_2021_pv_300to1000m_EPSG3035 IS NOT NULL,GDMP_2021_pv_300to1000m_EPSG3035,0)  as GDMP_2021_pv
# MAGIC ,if(GDMP_2022_pv_300to1000m_EPSG3035 IS NOT NULL,GDMP_2022_pv_300to1000m_EPSG3035,0)  as GDMP_2022_pv
# MAGIC
# MAGIC ,IF(SMA_gs_1km_annual_2000<-1 AND GDMP_1km_anom_2000 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2000
# MAGIC ,IF(SMA_gs_1km_annual_2001<-1 AND GDMP_1km_anom_2001 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2001
# MAGIC ,IF(SMA_gs_1km_annual_2002<-1 AND GDMP_1km_anom_2002 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2002
# MAGIC ,IF(SMA_gs_1km_annual_2003<-1 AND GDMP_1km_anom_2003 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2003
# MAGIC ,IF(SMA_gs_1km_annual_2004<-1 AND GDMP_1km_anom_2004 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2004
# MAGIC ,IF(SMA_gs_1km_annual_2005<-1 AND GDMP_1km_anom_2005 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2005
# MAGIC ,IF(SMA_gs_1km_annual_2006<-1 AND GDMP_1km_anom_2006 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2006
# MAGIC ,IF(SMA_gs_1km_annual_2007<-1 AND GDMP_1km_anom_2007 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2007
# MAGIC ,IF(SMA_gs_1km_annual_2008<-1 AND GDMP_1km_anom_2008 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2008
# MAGIC ,IF(SMA_gs_1km_annual_2009<-1 AND GDMP_1km_anom_2009 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2009
# MAGIC ,IF(SMA_gs_1km_annual_2010<-1 AND GDMP_1km_anom_2010 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2010
# MAGIC ,IF(SMA_gs_1km_annual_2011<-1 AND GDMP_1km_anom_2011 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2011
# MAGIC ,IF(SMA_gs_1km_annual_2012<-1 AND GDMP_1km_anom_2012 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2012
# MAGIC ,IF(SMA_gs_1km_annual_2013<-1 AND GDMP_1km_anom_2013 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2013
# MAGIC ,IF(SMA_gs_1km_annual_2014<-1 AND GDMP_1km_anom_2014 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2014
# MAGIC ,IF(SMA_gs_1km_annual_2015<-1 AND GDMP_1km_anom_2015 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2015
# MAGIC ,IF(SMA_gs_1km_annual_2016<-1 AND GDMP_1km_anom_2016 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2016
# MAGIC ,IF(SMA_gs_1km_annual_2017<-1 AND GDMP_1km_anom_2017 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2017
# MAGIC ,IF(SMA_gs_1km_annual_2018<-1 AND GDMP_1km_anom_2018 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2018
# MAGIC ,IF(SMA_gs_1km_annual_2019<-1 AND GDMP_1km_anom_2019 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2019
# MAGIC ,IF(SMA_gs_1km_annual_2020<-1 AND GDMP_1km_anom_2020 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2020
# MAGIC ,IF(SMA_gs_1km_annual_2021<-1 AND GDMP_1km_anom_2021 <0, ref_cube.AreaHa ,0)      			        as annual_drought_impact_area_2021
# MAGIC ,IF(SMA_gs_1km_annual_2022<-1 AND GDMP_1km_anom_2022 <0, ref_cube.AreaHa ,0)                    as annual_drought_impact_area_2022
# MAGIC
# MAGIC
# MAGIC
# MAGIC ,IF(SMA_gs_1km_annual_2000<-1 AND GDMP_1km_anom_2000<0, GDMP_1km_anom_2000 ,0 ) as annual_drought_impact_intensity_relative_2000 
# MAGIC ,IF(SMA_gs_1km_annual_2001<-1 AND GDMP_1km_anom_2001<0, GDMP_1km_anom_2001 ,0 ) as annual_drought_impact_intensity_relative_2001  
# MAGIC ,IF(SMA_gs_1km_annual_2002<-1 AND GDMP_1km_anom_2002<0, GDMP_1km_anom_2002 ,0 ) as annual_drought_impact_intensity_relative_2002 
# MAGIC ,IF(SMA_gs_1km_annual_2003<-1 AND GDMP_1km_anom_2003<0, GDMP_1km_anom_2003 ,0 ) as annual_drought_impact_intensity_relative_2003   
# MAGIC ,IF(SMA_gs_1km_annual_2004<-1 AND GDMP_1km_anom_2004<0, GDMP_1km_anom_2004 ,0 ) as annual_drought_impact_intensity_relative_2004 
# MAGIC ,IF(SMA_gs_1km_annual_2005<-1 AND GDMP_1km_anom_2005<0, GDMP_1km_anom_2005 ,0 ) as annual_drought_impact_intensity_relative_2005   
# MAGIC ,IF(SMA_gs_1km_annual_2006<-1 AND GDMP_1km_anom_2006<0, GDMP_1km_anom_2006 ,0 ) as annual_drought_impact_intensity_relative_2006 
# MAGIC ,IF(SMA_gs_1km_annual_2007<-1 AND GDMP_1km_anom_2007<0, GDMP_1km_anom_2007 ,0 ) as annual_drought_impact_intensity_relative_2007   
# MAGIC ,IF(SMA_gs_1km_annual_2008<-1 AND GDMP_1km_anom_2008<0, GDMP_1km_anom_2008 ,0 ) as annual_drought_impact_intensity_relative_2008 
# MAGIC ,IF(SMA_gs_1km_annual_2009<-1 AND GDMP_1km_anom_2009<0, GDMP_1km_anom_2009 ,0 ) as annual_drought_impact_intensity_relative_2009   
# MAGIC ,IF(SMA_gs_1km_annual_2010<-1 AND GDMP_1km_anom_2010<0, GDMP_1km_anom_2010 ,0 ) as annual_drought_impact_intensity_relative_2010 
# MAGIC ,IF(SMA_gs_1km_annual_2011<-1 AND GDMP_1km_anom_2011<0, GDMP_1km_anom_2011 ,0 ) as annual_drought_impact_intensity_relative_2011   
# MAGIC ,IF(SMA_gs_1km_annual_2012<-1 AND GDMP_1km_anom_2012<0, GDMP_1km_anom_2012 ,0 ) as annual_drought_impact_intensity_relative_2012 
# MAGIC ,IF(SMA_gs_1km_annual_2013<-1 AND GDMP_1km_anom_2013<0, GDMP_1km_anom_2013 ,0 ) as annual_drought_impact_intensity_relative_2013   
# MAGIC ,IF(SMA_gs_1km_annual_2014<-1 AND GDMP_1km_anom_2014<0, GDMP_1km_anom_2014 ,0 ) as annual_drought_impact_intensity_relative_2014 
# MAGIC ,IF(SMA_gs_1km_annual_2015<-1 AND GDMP_1km_anom_2015<0, GDMP_1km_anom_2015 ,0 ) as annual_drought_impact_intensity_relative_2015   
# MAGIC ,IF(SMA_gs_1km_annual_2016<-1 AND GDMP_1km_anom_2016<0, GDMP_1km_anom_2016 ,0 ) as annual_drought_impact_intensity_relative_2016 
# MAGIC ,IF(SMA_gs_1km_annual_2017<-1 AND GDMP_1km_anom_2017<0, GDMP_1km_anom_2017 ,0 ) as annual_drought_impact_intensity_relative_2017   
# MAGIC ,IF(SMA_gs_1km_annual_2018<-1 AND GDMP_1km_anom_2018<0, GDMP_1km_anom_2018 ,0 ) as annual_drought_impact_intensity_relative_2018 
# MAGIC ,IF(SMA_gs_1km_annual_2019<-1 AND GDMP_1km_anom_2019<0, GDMP_1km_anom_2019 ,0 ) as annual_drought_impact_intensity_relative_2019   
# MAGIC ,IF(SMA_gs_1km_annual_2020<-1 AND GDMP_1km_anom_2020<0, GDMP_1km_anom_2020 ,0 ) as annual_drought_impact_intensity_relative_2020 
# MAGIC ,IF(SMA_gs_1km_annual_2021<-1 AND GDMP_1km_anom_2021<0, GDMP_1km_anom_2021 ,0 ) as annual_drought_impact_intensity_relative_2021   
# MAGIC ,IF(SMA_gs_1km_annual_2022<-1 AND GDMP_1km_anom_2022<0, GDMP_1km_anom_2022 ,0 ) as annual_drought_impact_intensity_relative_2022 
# MAGIC
# MAGIC
# MAGIC ,IF(SMA_gs_1km_annual_2000<-1 AND GDMP_1km_anom_2000 <0, GDMP_2000_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2000 
# MAGIC ,IF(SMA_gs_1km_annual_2001<-1 AND GDMP_1km_anom_2001 <0, GDMP_2001_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2001 
# MAGIC ,IF(SMA_gs_1km_annual_2002<-1 AND GDMP_1km_anom_2002 <0, GDMP_2002_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2002 
# MAGIC ,IF(SMA_gs_1km_annual_2003<-1 AND GDMP_1km_anom_2003 <0, GDMP_2003_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2003 
# MAGIC ,IF(SMA_gs_1km_annual_2004<-1 AND GDMP_1km_anom_2004 <0, GDMP_2004_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2004 
# MAGIC ,IF(SMA_gs_1km_annual_2005<-1 AND GDMP_1km_anom_2005 <0, GDMP_2005_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2005 
# MAGIC ,IF(SMA_gs_1km_annual_2006<-1 AND GDMP_1km_anom_2006 <0, GDMP_2006_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2006 
# MAGIC ,IF(SMA_gs_1km_annual_2007<-1 AND GDMP_1km_anom_2007 <0, GDMP_2007_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2007 
# MAGIC ,IF(SMA_gs_1km_annual_2008<-1 AND GDMP_1km_anom_2008 <0, GDMP_2008_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2008 
# MAGIC ,IF(SMA_gs_1km_annual_2009<-1 AND GDMP_1km_anom_2009 <0, GDMP_2009_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2009 
# MAGIC ,IF(SMA_gs_1km_annual_2010<-1 AND GDMP_1km_anom_2010 <0, GDMP_2010_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2010 
# MAGIC ,IF(SMA_gs_1km_annual_2011<-1 AND GDMP_1km_anom_2011 <0, GDMP_2011_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2011 
# MAGIC ,IF(SMA_gs_1km_annual_2012<-1 AND GDMP_1km_anom_2012 <0, GDMP_2012_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2012 
# MAGIC ,IF(SMA_gs_1km_annual_2013<-1 AND GDMP_1km_anom_2013 <0, GDMP_2013_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2013 
# MAGIC ,IF(SMA_gs_1km_annual_2014<-1 AND GDMP_1km_anom_2014 <0, GDMP_2014_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2014 
# MAGIC ,IF(SMA_gs_1km_annual_2015<-1 AND GDMP_1km_anom_2015 <0, GDMP_2015_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2015 
# MAGIC ,IF(SMA_gs_1km_annual_2016<-1 AND GDMP_1km_anom_2016 <0, GDMP_2016_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2016 
# MAGIC ,IF(SMA_gs_1km_annual_2017<-1 AND GDMP_1km_anom_2017 <0, GDMP_2017_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2017 
# MAGIC ,IF(SMA_gs_1km_annual_2018<-1 AND GDMP_1km_anom_2018 <0, GDMP_2018_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2018 
# MAGIC ,IF(SMA_gs_1km_annual_2019<-1 AND GDMP_1km_anom_2019 <0, GDMP_2019_pv_1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2019 
# MAGIC ,IF(SMA_gs_1km_annual_2020<-1 AND GDMP_1km_anom_2020 <0, GDMP_2020_pv_300to1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2020 
# MAGIC ,IF(SMA_gs_1km_annual_2021<-1 AND GDMP_1km_anom_2021 <0, GDMP_2021_pv_300to1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2021 
# MAGIC ,IF(SMA_gs_1km_annual_2022<-1 AND GDMP_1km_anom_2022 <0, GDMP_2022_pv_300to1000m_EPSG3035 ,0 )  as annual_drought_impact_intensity_absolute_2022 
# MAGIC ----annual productivity condition = SUM of GDMP values DIM: gdmp_1km_pv
# MAGIC ----annual drought impact area = nr of gridcells expressed in km2 where SMA<-1 and GDMPaomalies <0 (GDMPa = GDMP anomalies)
# MAGIC ----annual drought impact intensity, relative = AVR of GDMPGDMPaomalies where SMA < -1 AND GDMPGDMPaomalies <0
# MAGIC ----annual drought impact intensity, absolute = SUM of GDMP where SMA <-1 AND GDMPGDMPaomalies <0
# MAGIC from
# MAGIC   ref_cube
# MAGIC LEFT JOIN gdmp_1km_pv on gdmp_1km_pv.gridnum   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC LEFT JOIN SMA_gs_1km_annual on SMA_gs_1km_annual.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC LEFT JOIN gdmp_1km_statistic_c on gdmp_1km_statistic_c.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!          
# MAGIC                                                       """)        
# MAGIC
# MAGIC
# MAGIC cube_2_f.createOrReplaceTempView("cube_2_f")  
# MAGIC
# MAGIC
# MAGIC
# MAGIC // CUBE 2 (2c)
# MAGIC
# MAGIC val cube_2_c = spark.sql("""  
# MAGIC SELECT 
# MAGIC  admin_category
# MAGIC ,GridNum10km
# MAGIC ,LULUCF_CODE
# MAGIC ,env_zones
# MAGIC ,natura2000_protection
# MAGIC ,SUM(AreaHa) as Areaha
# MAGIC --Sum:
# MAGIC ,SUM(GDMP_2000_pv) as GDMP_2000_pv
# MAGIC ,SUM(GDMP_2001_pv) as GDMP_2001_pv
# MAGIC ,SUM(GDMP_2002_pv) as GDMP_2002_pv
# MAGIC ,SUM(GDMP_2003_pv) as GDMP_2003_pv
# MAGIC ,SUM(GDMP_2004_pv) as GDMP_2004_pv
# MAGIC ,SUM(GDMP_2005_pv) as GDMP_2005_pv
# MAGIC ,SUM(GDMP_2006_pv) as GDMP_2006_pv
# MAGIC ,SUM(GDMP_2007_pv) as GDMP_2007_pv
# MAGIC ,SUM(GDMP_2008_pv) as GDMP_2008_pv
# MAGIC ,SUM(GDMP_2009_pv) as GDMP_2009_pv
# MAGIC ,SUM(GDMP_2010_pv) as GDMP_2010_pv
# MAGIC ,SUM(GDMP_2011_pv) as GDMP_2011_pv
# MAGIC ,SUM(GDMP_2012_pv) as GDMP_2012_pv
# MAGIC ,SUM(GDMP_2013_pv) as GDMP_2013_pv
# MAGIC ,SUM(GDMP_2014_pv) as GDMP_2014_pv
# MAGIC ,SUM(GDMP_2015_pv) as GDMP_2015_pv
# MAGIC ,SUM(GDMP_2016_pv) as GDMP_2016_pv
# MAGIC ,SUM(GDMP_2017_pv) as GDMP_2017_pv
# MAGIC ,SUM(GDMP_2018_pv) as GDMP_2018_pv
# MAGIC ,SUM(GDMP_2019_pv) as GDMP_2019_pv
# MAGIC ,SUM(GDMP_2020_pv) as GDMP_2020_pv
# MAGIC ,SUM(GDMP_2021_pv) as GDMP_2021_pv
# MAGIC ,SUM(GDMP_2022_pv) as GDMP_2022_pv
# MAGIC
# MAGIC --SUM:
# MAGIC ,SUM(annual_drought_impact_area_2000) as annual_drought_impact_area_2000
# MAGIC ,SUM(annual_drought_impact_area_2001) as annual_drought_impact_area_2001
# MAGIC ,SUM(annual_drought_impact_area_2002) as annual_drought_impact_area_2002
# MAGIC ,SUM(annual_drought_impact_area_2003) as annual_drought_impact_area_2003
# MAGIC ,SUM(annual_drought_impact_area_2004) as annual_drought_impact_area_2004
# MAGIC ,SUM(annual_drought_impact_area_2005) as annual_drought_impact_area_2005
# MAGIC ,SUM(annual_drought_impact_area_2006) as annual_drought_impact_area_2006
# MAGIC ,SUM(annual_drought_impact_area_2007) as annual_drought_impact_area_2007
# MAGIC ,SUM(annual_drought_impact_area_2008) as annual_drought_impact_area_2008
# MAGIC ,SUM(annual_drought_impact_area_2009) as annual_drought_impact_area_2009
# MAGIC ,SUM(annual_drought_impact_area_2010) as annual_drought_impact_area_2010
# MAGIC ,SUM(annual_drought_impact_area_2011) as annual_drought_impact_area_2011
# MAGIC ,SUM(annual_drought_impact_area_2012) as annual_drought_impact_area_2012
# MAGIC ,SUM(annual_drought_impact_area_2013) as annual_drought_impact_area_2013
# MAGIC ,SUM(annual_drought_impact_area_2014) as annual_drought_impact_area_2014
# MAGIC ,SUM(annual_drought_impact_area_2015) as annual_drought_impact_area_2015
# MAGIC ,SUM(annual_drought_impact_area_2016) as annual_drought_impact_area_2016
# MAGIC ,SUM(annual_drought_impact_area_2017) as annual_drought_impact_area_2017
# MAGIC ,SUM(annual_drought_impact_area_2018) as annual_drought_impact_area_2018
# MAGIC ,SUM(annual_drought_impact_area_2019) as annual_drought_impact_area_2019
# MAGIC ,SUM(annual_drought_impact_area_2020) as annual_drought_impact_area_2020
# MAGIC ,SUM(annual_drought_impact_area_2021) as annual_drought_impact_area_2021
# MAGIC ,SUM(annual_drought_impact_area_2022) as annual_drought_impact_area_2022
# MAGIC
# MAGIC
# MAGIC --Weighted avg:
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2000* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2000
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2001* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2001
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2002* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2002
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2003* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2003
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2004* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2004
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2005* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2005
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2006* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2006
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2007* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2007
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2008* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2008
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2009* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2009
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2010* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2010
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2011* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2011
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2012* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2012
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2013* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2013
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2014* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2014
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2015* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2015
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2016* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2016
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2017* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2017
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2018* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2018
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2019* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2019
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2020* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2020
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2021* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2021
# MAGIC ,SUM(annual_drought_impact_intensity_relative_2022* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2022
# MAGIC --SUM: (absolute)
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2000) as annual_drought_impact_intensity_absolute_2000
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2001) as annual_drought_impact_intensity_absolute_2001
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2002) as annual_drought_impact_intensity_absolute_2002
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2003) as annual_drought_impact_intensity_absolute_2003
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2004) as annual_drought_impact_intensity_absolute_2004
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2005) as annual_drought_impact_intensity_absolute_2005
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2006) as annual_drought_impact_intensity_absolute_2006
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2007) as annual_drought_impact_intensity_absolute_2007
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2008) as annual_drought_impact_intensity_absolute_2008
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2009) as annual_drought_impact_intensity_absolute_2009
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2010) as annual_drought_impact_intensity_absolute_2010
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2011) as annual_drought_impact_intensity_absolute_2011
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2012) as annual_drought_impact_intensity_absolute_2012
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2013) as annual_drought_impact_intensity_absolute_2013
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2014) as annual_drought_impact_intensity_absolute_2014
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2015) as annual_drought_impact_intensity_absolute_2015
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2016) as annual_drought_impact_intensity_absolute_2016
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2017) as annual_drought_impact_intensity_absolute_2017
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2018) as annual_drought_impact_intensity_absolute_2018
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2019) as annual_drought_impact_intensity_absolute_2019
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2020) as annual_drought_impact_intensity_absolute_2020
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2021) as annual_drought_impact_intensity_absolute_2021
# MAGIC ,SUM(annual_drought_impact_intensity_absolute_2022) as annual_drought_impact_intensity_absolute_2022
# MAGIC
# MAGIC from cube_2_f
# MAGIC
# MAGIC GROUP BY
# MAGIC  admin_category
# MAGIC ,GridNum10km
# MAGIC ,LULUCF_CODE
# MAGIC ,env_zones
# MAGIC ,natura2000_protection
# MAGIC
# MAGIC
# MAGIC                  """)    
# MAGIC
# MAGIC cube_2_c
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/drought_sma_gdmp/cube_2_c")
# MAGIC
# MAGIC   
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/drought_sma_gdmp/cube_2_c"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print ("-------------------------------------")
        print ("CUBE drought -sma -gdmp - 1c can be downloaded using following link:")
        print (URL)

# COMMAND ----------

# MAGIC %md ### 2.3) Construction of CUBE 3
# MAGIC - SMA Mann Kendall 
# MAGIC - SMA p value 
# MAGIC - SMA rel change 
# MAGIC - GDMP Mann Kendall 
# MAGIC - GDMP p value 
# MAGIC - GDMP slope 
# MAGIC - GDMP rel change 

# COMMAND ----------

# MAGIC %sql ----TESTING CUBE 3
# MAGIC SELECT  
# MAGIC  ref_cube.admin_category
# MAGIC ,ref_cube.GridNum10km
# MAGIC ,ref_cube.GridNum1km
# MAGIC ,ref_cube.LULUCF_CODE
# MAGIC ,ref_cube.env_zones
# MAGIC ,ref_cube.natura2000_protection
# MAGIC ,ref_cube.AreaHa 
# MAGIC
# MAGIC ,MK_trend_SMA_annual
# MAGIC ,p_value_SMA_annual
# MAGIC ,relative_change_SMA_annual
# MAGIC ,GDMP_1km_00_22_pvalue
# MAGIC ,GDMP_1km_00_22_relative_change
# MAGIC ,GDMP_1km_00_22_slope
# MAGIC ,GDMP_1km_00_22_trend
# MAGIC
# MAGIC from
# MAGIC   ref_cube
# MAGIC LEFT JOIN SMA_statistics        on SMA_statistics.gridnum                     =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC LEFT JOIN GDMP_1km_trend_analy  on GDMP_1km_trend_analy.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC
# MAGIC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select (GDMP_1km_00_22_relative_change)  from GDMP_1km_trend_analy   order by GDMP_1km_00_22_relative_change desc

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select (GDMP_1km_00_22_relative_change)  , IF(GDMP_1km_00_22_relative_change='Infinity', NULL, GDMP_1km_00_22_relative_change) as GDMP_1km_00_22_relative_change from GDMP_1km_trend_analy  
# MAGIC where GDMP_1km_00_22_relative_change ='Infinity'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC /// Set up F-CUBE (3)
# MAGIC val cube_3_f = spark.sql("""              
# MAGIC   SELECT  
# MAGIC   ref_cube.admin_category
# MAGIC   ,ref_cube.GridNum10km
# MAGIC   ,ref_cube.GridNum1km
# MAGIC   ,ref_cube.LULUCF_CODE
# MAGIC   ,ref_cube.env_zones
# MAGIC   ,ref_cube.natura2000_protection
# MAGIC   ,ref_cube.AreaHa 
# MAGIC   ,MK_trend_SMA_annual
# MAGIC   ,p_value_SMA_annual
# MAGIC   ,relative_change_SMA_annual
# MAGIC   ,GDMP_1km_00_22_pvalue
# MAGIC   ,IF(GDMP_1km_00_22_relative_change='Infinity', NULL, GDMP_1km_00_22_relative_change) as GDMP_1km_00_22_relative_change
# MAGIC   ,GDMP_1km_00_22_slope
# MAGIC   ,GDMP_1km_00_22_trend
# MAGIC   from
# MAGIC     ref_cube
# MAGIC   LEFT JOIN SMA_statistics        on SMA_statistics.gridnum                     =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC   LEFT JOIN GDMP_1km_trend_analy  on GDMP_1km_trend_analy.gridnum              =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC                                                       """)        
# MAGIC cube_3_f.createOrReplaceTempView("cube_3_f")  
# MAGIC
# MAGIC // CUBE 3 (3c)
# MAGIC val cube_3_c = spark.sql("""  
# MAGIC       Select 
# MAGIC       admin_category
# MAGIC       ,env_zones
# MAGIC       ,natura2000_protection
# MAGIC       ,GridNum10km
# MAGIC       ,LULUCF_CODE
# MAGIC
# MAGIC       ,SUM(AreaHa) as AreaHa
# MAGIC       ,SUM(GDMP_1km_00_22_pvalue* AreaHa )/ SUM(AreaHa)             as GDMP_1km_00_22_pvalue
# MAGIC       ,SUM(GDMP_1km_00_22_relative_change* AreaHa )/ SUM(AreaHa)    as GDMP_1km_00_22_relative_change
# MAGIC       ,SUM(GDMP_1km_00_22_slope* AreaHa )/ SUM(AreaHa)              as GDMP_1km_00_22_slope
# MAGIC       ,SUM(GDMP_1km_00_22_trend* AreaHa )/ SUM(AreaHa)              as GDMP_1km_00_22_trend
# MAGIC
# MAGIC       ,SUM(MK_trend_SMA_annual* AreaHa )/ SUM(AreaHa)               as MK_trend_SMA_annual
# MAGIC       ,SUM(p_value_SMA_annual* AreaHa )/ SUM(AreaHa)                as p_value_SMA_annual
# MAGIC       ,SUM(relative_change_SMA_annual* AreaHa )/ SUM(AreaHa)        as relative_change_SMA_annual
# MAGIC       
# MAGIC       from cube_3_f
# MAGIC       group by 
# MAGIC       admin_category
# MAGIC       ,env_zones
# MAGIC       ,natura2000_protection
# MAGIC       ,GridNum10km
# MAGIC       ,LULUCF_CODE
# MAGIC                  """)    
# MAGIC cube_3_c
# MAGIC     .coalesce(1) //be careful with this
# MAGIC     .write.format("com.databricks.spark.csv")
# MAGIC     .mode(SaveMode.Overwrite)
# MAGIC     .option("sep","|")
# MAGIC     .option("overwriteSchema", "true")
# MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC     .option("emptyValue", "")
# MAGIC     .option("header","true")
# MAGIC     ///.option("encoding", "UTF-16")  /// check ENCODING
# MAGIC     .option("treatEmptyValuesAsNulls", "true")  
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/drought_sma_gdmp/cube_3_c")
# MAGIC
# MAGIC   
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/drought_sma_gdmp/cube_3_c"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print ("-------------------------------------")
        print ("CUBE drought -sma -gdmp - 2c can be downloaded using following link:")
        print (URL)

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

# COMMAND ----------

SELECT 
                admin_category
                ,GridNum10km
                ,LULUCF_CODE
                ,env_zones
                ,natura2000_protection
                ,SUM(AreaHa) as Areaha
                --Sum:
                ,SUM(GDMP_2000_pv) as GDMP_2000_pv
                ,SUM(GDMP_2001_pv) as GDMP_2001_pv
                ,SUM(GDMP_2002_pv) as GDMP_2002_pv
                ,SUM(GDMP_2003_pv) as GDMP_2003_pv
                ,SUM(GDMP_2004_pv) as GDMP_2004_pv
                ,SUM(GDMP_2005_pv) as GDMP_2005_pv
                ,SUM(GDMP_2006_pv) as GDMP_2006_pv
                ,SUM(GDMP_2007_pv) as GDMP_2007_pv
                ,SUM(GDMP_2008_pv) as GDMP_2008_pv
                ,SUM(GDMP_2009_pv) as GDMP_2009_pv
                ,SUM(GDMP_2010_pv) as GDMP_2010_pv
                ,SUM(GDMP_2011_pv) as GDMP_2011_pv
                ,SUM(GDMP_2012_pv) as GDMP_2012_pv
                ,SUM(GDMP_2013_pv) as GDMP_2013_pv
                ,SUM(GDMP_2014_pv) as GDMP_2014_pv
                ,SUM(GDMP_2015_pv) as GDMP_2015_pv
                ,SUM(GDMP_2016_pv) as GDMP_2016_pv
                ,SUM(GDMP_2017_pv) as GDMP_2017_pv
                ,SUM(GDMP_2018_pv) as GDMP_2018_pv
                ,SUM(GDMP_2019_pv) as GDMP_2019_pv
                ,SUM(GDMP_2020_pv) as GDMP_2020_pv
                ,SUM(GDMP_2021_pv) as GDMP_2021_pv
                ,SUM(GDMP_2022_pv) as GDMP_2022_pv

                --SUM:
                ,SUM(annual_drought_impact_area_2000) as annual_drought_impact_area_2000
                ,SUM(annual_drought_impact_area_2001) as annual_drought_impact_area_2001
                ,SUM(annual_drought_impact_area_2002) as annual_drought_impact_area_2002
                ,SUM(annual_drought_impact_area_2003) as annual_drought_impact_area_2003
                ,SUM(annual_drought_impact_area_2004) as annual_drought_impact_area_2004
                ,SUM(annual_drought_impact_area_2005) as annual_drought_impact_area_2005
                ,SUM(annual_drought_impact_area_2006) as annual_drought_impact_area_2006
                ,SUM(annual_drought_impact_area_2007) as annual_drought_impact_area_2007
                ,SUM(annual_drought_impact_area_2008) as annual_drought_impact_area_2008
                ,SUM(annual_drought_impact_area_2009) as annual_drought_impact_area_2009
                ,SUM(annual_drought_impact_area_2010) as annual_drought_impact_area_2010
                ,SUM(annual_drought_impact_area_2011) as annual_drought_impact_area_2011
                ,SUM(annual_drought_impact_area_2012) as annual_drought_impact_area_2012
                ,SUM(annual_drought_impact_area_2013) as annual_drought_impact_area_2013
                ,SUM(annual_drought_impact_area_2014) as annual_drought_impact_area_2014
                ,SUM(annual_drought_impact_area_2015) as annual_drought_impact_area_2015
                ,SUM(annual_drought_impact_area_2016) as annual_drought_impact_area_2016
                ,SUM(annual_drought_impact_area_2017) as annual_drought_impact_area_2017
                ,SUM(annual_drought_impact_area_2018) as annual_drought_impact_area_2018
                ,SUM(annual_drought_impact_area_2019) as annual_drought_impact_area_2019
                ,SUM(annual_drought_impact_area_2020) as annual_drought_impact_area_2020
                ,SUM(annual_drought_impact_area_2021) as annual_drought_impact_area_2021
                ,SUM(annual_drought_impact_area_2022) as annual_drought_impact_area_2022
                --Weighted avg:
                ,SUM(annual_drought_impact_intensity_relative_2000* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2000
                ,SUM(annual_drought_impact_intensity_relative_2001* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2001
                ,SUM(annual_drought_impact_intensity_relative_2002* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2002
                ,SUM(annual_drought_impact_intensity_relative_2003* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2003
                ,SUM(annual_drought_impact_intensity_relative_2004* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2004
                ,SUM(annual_drought_impact_intensity_relative_2005* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2005
                ,SUM(annual_drought_impact_intensity_relative_2006* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2006
                ,SUM(annual_drought_impact_intensity_relative_2007* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2007
                ,SUM(annual_drought_impact_intensity_relative_2008* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2008
                ,SUM(annual_drought_impact_intensity_relative_2009* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2009
                ,SUM(annual_drought_impact_intensity_relative_2010* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2010
                ,SUM(annual_drought_impact_intensity_relative_2011* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2011
                ,SUM(annual_drought_impact_intensity_relative_2012* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2012
                ,SUM(annual_drought_impact_intensity_relative_2013* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2013
                ,SUM(annual_drought_impact_intensity_relative_2014* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2014
                ,SUM(annual_drought_impact_intensity_relative_2015* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2015
                ,SUM(annual_drought_impact_intensity_relative_2016* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2016
                ,SUM(annual_drought_impact_intensity_relative_2017* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2017
                ,SUM(annual_drought_impact_intensity_relative_2018* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2018
                ,SUM(annual_drought_impact_intensity_relative_2019* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2019
                ,SUM(annual_drought_impact_intensity_relative_2020* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2020
                ,SUM(annual_drought_impact_intensity_relative_2021* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2021
                ,SUM(annual_drought_impact_intensity_relative_2022* AreaHa )/ SUM(AreaHa)  as annual_drought_impact_intensity_relative_2022
                --SUM: (absolute)
                ,SUM(annual_drought_impact_intensity_absolute_2000) as annual_drought_impact_intensity_absolute_2000
                ,SUM(annual_drought_impact_intensity_absolute_2001) as annual_drought_impact_intensity_absolute_2001
                ,SUM(annual_drought_impact_intensity_absolute_2002) as annual_drought_impact_intensity_absolute_2002
                ,SUM(annual_drought_impact_intensity_absolute_2003) as annual_drought_impact_intensity_absolute_2003
                ,SUM(annual_drought_impact_intensity_absolute_2004) as annual_drought_impact_intensity_absolute_2004
                ,SUM(annual_drought_impact_intensity_absolute_2005) as annual_drought_impact_intensity_absolute_2005
                ,SUM(annual_drought_impact_intensity_absolute_2006) as annual_drought_impact_intensity_absolute_2006
                ,SUM(annual_drought_impact_intensity_absolute_2007) as annual_drought_impact_intensity_absolute_2007
                ,SUM(annual_drought_impact_intensity_absolute_2008) as annual_drought_impact_intensity_absolute_2008
                ,SUM(annual_drought_impact_intensity_absolute_2009) as annual_drought_impact_intensity_absolute_2009
                ,SUM(annual_drought_impact_intensity_absolute_2010) as annual_drought_impact_intensity_absolute_2010
                ,SUM(annual_drought_impact_intensity_absolute_2011) as annual_drought_impact_intensity_absolute_2011
                ,SUM(annual_drought_impact_intensity_absolute_2012) as annual_drought_impact_intensity_absolute_2012
                ,SUM(annual_drought_impact_intensity_absolute_2013) as annual_drought_impact_intensity_absolute_2013
                ,SUM(annual_drought_impact_intensity_absolute_2014) as annual_drought_impact_intensity_absolute_2014
                ,SUM(annual_drought_impact_intensity_absolute_2015) as annual_drought_impact_intensity_absolute_2015
                ,SUM(annual_drought_impact_intensity_absolute_2016) as annual_drought_impact_intensity_absolute_2016
                ,SUM(annual_drought_impact_intensity_absolute_2017) as annual_drought_impact_intensity_absolute_2017
                ,SUM(annual_drought_impact_intensity_absolute_2018) as annual_drought_impact_intensity_absolute_2018
                ,SUM(annual_drought_impact_intensity_absolute_2019) as annual_drought_impact_intensity_absolute_2019
                ,SUM(annual_drought_impact_intensity_absolute_2020) as annual_drought_impact_intensity_absolute_2020
                ,SUM(annual_drought_impact_intensity_absolute_2021) as annual_drought_impact_intensity_absolute_2021
                ,SUM(annual_drought_impact_intensity_absolute_2022) as annual_drought_impact_intensity_absolute_2022

                from cube_2_f

                GROUP BY
                admin_category
                ,GridNum10km
                ,LULUCF_CODE
                ,env_zones
                ,natura2000_protection

# COMMAND ----------


