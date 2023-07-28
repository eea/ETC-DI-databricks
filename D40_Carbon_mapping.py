# Databricks notebook source
# MAGIC %md # Carbon mapping
# MAGIC
# MAGIC ![](https://space4environment.com/fileadmin/Resources/Public/Images/Logos/S4E-Logo.png)
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
# MAGIC //// 10.1 (SOC)  ISRIC SOC 0-30 cm################################################################################                 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC //   Organic Carbon Stock from ISRIC
# MAGIC //   mean rescaled at 100m
# MAGIC //   values expressed as t/ha
# MAGIC //   data provided by VITO
# MAGIC //   S:\Common workspace\ETC_DI\f03_JEDI_PREPARATION\f01_dims\SOC_mapping\ISRIC
# MAGIC //   https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1947&fileId=972
# MAGIC ///  cwsblobstorage01/cwsblob01/Dimensions/D_isricsoc030_972_2023216_100m
# MAGIC val parquetFileDF_isric_30 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_isricsoc030_972_2023216_100m/")
# MAGIC parquetFileDF_isric_30.createOrReplaceTempView("isric_30")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 10.2 (SOC) ISRIC Organic Carbon Stock 100cmm################################################################################  100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC //   Organic Carbon Stock from ISRIC
# MAGIC //   Calculated
# MAGIC //   sum of carbon densities (SoilGrids, Organic carbon density) weighted by layer thickness 
# MAGIC //   ['ocd_0-5cm_mean'*0.05 +'ocd_5-15cm_mean'+0.1+'ocd_15-30cm_mean'*0.15+'ocd_30-60cm_mean'*0.3+'ocd_60-100cm_mean'*0.4] '
# MAGIC //   *0.001 to convert from hg/dm3 (kg/m3) to ton/ha for 1 m of thickness
# MAGIC //   https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1958&fileId=983
# MAGIC ///  cwsblobstorage01/cwsblob01/Dimensions/D_isricocs100_983_2023320_100m
# MAGIC
# MAGIC val parquetFileDF_isric_100 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_isricocs100_983_2023320_100m/")
# MAGIC parquetFileDF_isric_100.createOrReplaceTempView("isric_100")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 10.3 (SOC FLUX)SOC soil organic carbon stock arable + grassland areas (0-30 cm)  1999-2021############################## 1km!! 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC // !! based on unpublished paper from JRC
# MAGIC // Model DayCent 1x1km resolution  
# MAGIC // https://en.wikipedia.org/wiki/DayCent
# MAGIC // Format = GEOTIFF
# MAGIC // unit = [g C m-2 ]
# MAGIC // time = annual values from 1990-2021: 
# MAGIC // layer1= 1990, layer 32= 2021
# MAGIC // NoDATA value =  -999
# MAGIC // https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2007&fileId=1029&successMessage=true
# MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_SOC_crop_grass99_21b_1029_2023718_1km
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_soc_flux_DayCent = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_SOC_crop_grass99_21b_1029_2023718_1km/")
# MAGIC parquetFileDF_soc_flux_DayCent.createOrReplaceTempView("soc_flux_daycent")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 11 (SOC STOCK for wetlands -Extended Wetland layer, C stock pool - 2018
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2004&fileId=1026
# MAGIC // UNIT: https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2004&fileId=1026
# MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_extwet_cstock_1026_2023717_100m
# MAGIC // UNIT: Mg C ha-1          equal to t/ha
# MAGIC val parquetFileDF_soc_stock_ext_wetland = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_extwet_cstock_1026_2023717_100m/")
# MAGIC parquetFileDF_soc_stock_ext_wetland.createOrReplaceTempView("soc_stock_ext_wetland")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 11 (AGB 2018)ESA CCI Above Ground Biomass 2018 v4 ############################## 1 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC ///  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1955&fileId=980
# MAGIC //  Biomass Climate Change Initiative (Biomass_cci): Global datasets of forest above-ground biomass for the year 2018, v4
# MAGIC //  Data as been resampled to 100m into ETRS89 projection
# MAGIC //   This DIM is valid for the 2018 year, include 2 values
# MAGIC //     1) above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) (raster dataset). This is defined as the mass, expressed as oven-dry weight of the woody parts (stem, bark, // 
# MAGIC //     branches and twigs) of all living trees excluding stump and roots
# MAGIC //   [ESA_CCI_AGB_2018]
# MAGIC //   2) per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha (raster dataset)  UNIT: [Mg/ha]
# MAGIC //   [ESA_CCI_AGB_2018_SD]
# MAGIC //
# MAGIC //  no data value: -9999
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_ESACCIAGB2018v3a_980_2023223_100m
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_AGB_2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ESACCIAGB2018v3a_980_2023223_100m/")
# MAGIC parquetFileDF_AGB_2018.createOrReplaceTempView("AGB_2018")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 12 (BGB  2018)  Forest Carbon Monitoring 2020 Below Ground Biomass############################## 100m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1991&fileId=1013
# MAGIC //  absolute value and standard deviation
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_FCM2020BGB_1013_2023630_100m
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_BGB_forest_2020 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_FCM2020BGB_1013_2023630_100m/")
# MAGIC parquetFileDF_BGB_forest_2020.createOrReplaceTempView("BGB_forest_2020")
# MAGIC
# MAGIC
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select LULUCF_CODE,LULUCF_DESCRIPTION
# MAGIC  from lULUCF_2018
# MAGIC
# MAGIC where 
# MAGIC group by 
# MAGIC LULUCF_CODE,LULUCF_DESCRIPTION
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC
# MAGIC
# MAGIC from BGB_forest_2020  
# MAGIC where FCM_Europe_demo_2020_BGB < 0 ---or FCM_Europe_demo_2020_BGB =128
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from GDMP_1km_99_19
# MAGIC  

# COMMAND ----------

# MAGIC %md ## 2) Building CUBES

# COMMAND ----------

# MAGIC %md ### (2.1) SOC STOCK

# COMMAND ----------

# MAGIC %md #### (2.1.1) DASHBOARD SOC-STOCk (A) ISRIC 30cm for Cropland, Grassland, (Settlements, other)
# MAGIC
# MAGIC
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/soc.JPG?raw=true)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ///2 (group by) SET UP SUB-CUBE for the SOC dashboard:
# MAGIC
# MAGIC /// example
# MAGIC // Exporting the final table  ---city indicator: ua-classes vs. clc-plus inside the core city:
# MAGIC val SUB_CUBE_SOC_STOCK_1_30cm = spark.sql("""
# MAGIC
# MAGIC SELECT 
# MAGIC   
# MAGIC   nuts3_2021.Category, ----FOR ADMIN
# MAGIC   
# MAGIC   ----nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(isric_30.ocs030cm100m)  as SOC_STOCK_isric30cm_t,    --values expressed as t/ha
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC   
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN isric_30     on nuts3_2021.GridNum = isric_30.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC
# MAGIC group by 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   ----nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   env_zones.Category ,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC --FL	Forest land  # CL	Cropland # GL	Grassland #SL	Settlements #WL	Wetlands #OL	Other land #null	null
# MAGIC             """)
# MAGIC SUB_CUBE_SOC_STOCK_1_30cm
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_1_30cm_nuts3")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ///2 (group by) SET UP SUB-CUBE for the SOC dashboard:
# MAGIC
# MAGIC /// example
# MAGIC // Exporting the final table  ---city indicator: ua-classes vs. clc-plus inside the core city:
# MAGIC val SUB_CUBE_SOC_STOCK_1_30cm = spark.sql("""
# MAGIC
# MAGIC SELECT 
# MAGIC   
# MAGIC   nuts3_2021.Category, ----FOR ADMIN
# MAGIC   
# MAGIC   nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(isric_30.ocs030cm100m)  as SOC_STOCK_isric30cm_t,    --values expressed as t/ha
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC   
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN isric_30     on nuts3_2021.GridNum = isric_30.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC
# MAGIC group by 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   env_zones.Category ,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC --FL	Forest land  # CL	Cropland # GL	Grassland #SL	Settlements #WL	Wetlands #OL	Other land #null	null
# MAGIC             """)
# MAGIC SUB_CUBE_SOC_STOCK_1_30cm
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_1_30cm_10km_nuts3")

# COMMAND ----------

# MAGIC %md #### (2.1.2) DASHBOARD SOC-STOCk (B) ISRIC 100cm for Forest

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ///2 (group by) SET UP SUB-CUBE for the SOC dashboard:
# MAGIC
# MAGIC /// example
# MAGIC // Exporting the final table  ---city indicator: ua-classes vs. clc-plus inside the core city:
# MAGIC val SUB_CUBE_SOC_STOCK_2_100cm = spark.sql("""
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   
# MAGIC   ---nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(isric_100.carbonStocks_0_100cm_100m)  as SOC_STOCK_isric100cm_t,    --values expressed as t/ha
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC   
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN isric_100    on nuts3_2021.GridNum = isric_100.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE = 'FL'   ---- only for forest
# MAGIC
# MAGIC group by 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   ---nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   env_zones.Category ,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC
# MAGIC --FL	Forest land  # CL	Cropland # GL	Grassland #SL	Settlements #WL	Wetlands #OL	Other land #null	null
# MAGIC             """)
# MAGIC SUB_CUBE_SOC_STOCK_2_100cm
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_2_100cm_nuts3")

# COMMAND ----------

# MAGIC %md #### (2.1.3) DASHBOARD SOC-STOCk (C) Wetland

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC
# MAGIC soc_stock_ext_wetland.wetlands_categories as wetland_c_t_per_ha ,* 
# MAGIC from soc_stock_ext_wetland
# MAGIC
# MAGIC --From the data and literature analysis, a carbon stock value has been assigned per each Extended Wetland layer class depending on thei geographic location. The data supports the estimation of wetland carbon pool for the definition of wetlands from a LULUCF approach and the second, from an ecosystem perspective.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ----- Testing cube for soc-stock forest using isric 100cm dataset:
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   
# MAGIC  --- nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC  nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC CLC_2018.Category as clc18_level3_class,
# MAGIC
# MAGIC
# MAGIC ---soc_stock_ext_wetland.wetlands_categories as wetland_c_t_per_ha 
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(soc_stock_ext_wetland.wetlands_categories)  as SOC_STOCK_t_ext_wetland,
# MAGIC  if( CLC_2018.Category in  (411,412,421,422,423 , 511, 512, 521, 522 , 523) ,sum(soc_stock_ext_wetland.wetlands_categories),0) as SOC_STOCK_t_wetland,
# MAGIC       --values expressed as t/ha
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   OrganicSoils,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC   
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN soc_stock_ext_wetland    on nuts3_2021.GridNum = soc_stock_ext_wetland.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN CLC_2018     on nuts3_2021.GridNum = CLC_2018.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and soc_stock_ext_wetland.wetlands_categories >0 ---- only for wetlands
# MAGIC
# MAGIC
# MAGIC
# MAGIC ----and ISO2 ='LU'
# MAGIC ---[LEVEL3_CODE] in (411,412,421,422,423, 511, 512, 521, 522 , 523) 
# MAGIC --411Inland marshes ,412Peat bogs  ,421Salt marshes, 422Salines ,423Intertidal flats
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC group by 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   ---nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   env_zones.Category , 
# MAGIC   CLC_2018.Category,
# MAGIC   OrganicSoils,
# MAGIC if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ///2 (group by) SET UP SUB-CUBE for the SOC dashboard: WETLAND
# MAGIC
# MAGIC /// example
# MAGIC // Exporting the final table  
# MAGIC val SUB_CUBE_SOC_STOCK_3_wetland = spark.sql("""
# MAGIC SELECT 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   
# MAGIC  --- nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC  nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC CLC_2018.Category as clc18_level3_class,
# MAGIC ---soc_stock_ext_wetland.wetlands_categories as wetland_c_t_per_ha 
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(soc_stock_ext_wetland.wetlands_categories)  as SOC_STOCK_t_ext_wetland,
# MAGIC  if( CLC_2018.Category in  (411,412,421,422,423 , 511, 512, 521, 522 , 523) ,sum(soc_stock_ext_wetland.wetlands_categories),0) as SOC_STOCK_t_wetland,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   OrganicSoils,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC from nuts3_2021
# MAGIC LEFT JOIN soc_stock_ext_wetland    on nuts3_2021.GridNum = soc_stock_ext_wetland.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN CLC_2018     on nuts3_2021.GridNum = CLC_2018.GridNum
# MAGIC where nuts3_2021.LEVEL3_code is not null and soc_stock_ext_wetland.wetlands_categories >0 ---- only for wetlands
# MAGIC group by 
# MAGIC   nuts3_2021.Category,
# MAGIC   ---nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   env_zones.Category , 
# MAGIC   CLC_2018.Category,
# MAGIC   OrganicSoils,
# MAGIC if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC             """)
# MAGIC SUB_CUBE_SOC_STOCK_3_wetland
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_3_wetland_nuts3")

# COMMAND ----------

# MAGIC %md ### (2.2) SOC FLUX

# COMMAND ----------

# MAGIC %md #### (2.2.1) DASHBOARD  SOC-FLUX (A) JRC-DAY-CENT 1km time series for Cropland and Grassland // unit = [g C m-2 ]

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from soc_flux_daycent
# MAGIC
# MAGIC --where gridnum = 9402376024752128

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   -----nuts3_2021.GridNum,
# MAGIC   nuts3_2021.Category,
# MAGIC   
# MAGIC   nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   nuts3_2021.AreaHa as AreaHa,
# MAGIC
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1990,0)  as y1990 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1991,0)  as y1991 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1992,0)  as y1992 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1993,0)  as y1993 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1994,0)  as y1994 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1995,0)  as y1995 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1996,0)  as y1996 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1997,0)  as y1997 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1998,0)  as y1998 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_1999,0)  as y1999 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2000,0)  as y2000 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2001,0)  as y2001 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2002,0)  as y2002 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2003,0)  as y2003 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2004,0)  as y2004 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2005,0)  as y2005 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2006,0)  as y2006 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2007,0)  as y2007 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2008,0)  as y2008 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2009,0)  as y2009 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2010,0)  as y2010 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2011,0)  as y2011 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2012,0)  as y2012 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2013,0)  as y2013 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2014,0)  as y2014 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2015,0)  as y2015 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2016,0)  as y2016 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2017,0)  as y2017 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2018,0)  as y2018 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2019,0)  as y2019 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2020,0)  as y2020 ,
# MAGIC   ifnull(SOC_g_m2_90_21_epsg3035_2021,0)  as y2021 ,
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(organic_soil.OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC
# MAGIC from nuts3_2021
# MAGIC LEFT JOIN soc_flux_daycent on nuts3_2021.GridNum1km = soc_flux_daycent.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN organic_soil     on nuts3_2021.GridNum1km =     organic_soil.gridnum         ------ 1km JOIN !!!!!!
# MAGIC
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC
# MAGIC where nuts3_2021.ISO2	 ='LU' and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC and 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1990,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1991,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1992,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1993,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1994,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1995,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1996,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1997,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1998,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1999,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2000,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2001,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2002,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2003,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2004,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2005,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2006,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2007,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2008,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2009,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2010,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2011,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2012,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2013,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2014,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2015,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2016,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2017,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2018,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2019,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2020,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2021,0)   >0
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   nuts3_2021.Category,
# MAGIC   nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   sum(nuts3_2021.AreaHa) as AreaHa,
# MAGIC  sum(nuts3_2021.AreaHa /10000) as Aream2,-----check units!!!!!!!!!!!!
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1990,0))  as y1990 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1991,0))  as y1991 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1992,0))  as y1992 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1993,0))  as y1993 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1994,0))  as y1994 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1995,0))  as y1995 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1996,0))  as y1996 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1997,0))  as y1997 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1998,0))  as y1998 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1999,0))  as y1999 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2000,0))  as y2000 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2001,0))  as y2001 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2002,0))  as y2002 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2003,0))  as y2003 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2004,0))  as y2004 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2005,0))  as y2005 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2006,0))  as y2006 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2007,0))  as y2007 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2008,0))  as y2008 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2009,0))  as y2009 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2010,0))  as y2010 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2011,0))  as y2011 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2012,0))  as y2012 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2013,0))  as y2013 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2014,0))  as y2014 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2015,0))  as y2015 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2016,0))  as y2016 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2017,0))  as y2017 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2018,0))  as y2018 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2019,0)) as y2019 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2020,0))  as y2020 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2021,0))  as y2021 ,
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(organic_soil.OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC
# MAGIC from nuts3_2021
# MAGIC LEFT JOIN soc_flux_daycent on nuts3_2021.GridNum1km = soc_flux_daycent.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN organic_soil     on nuts3_2021.GridNum1km =     organic_soil.gridnum         ------ 1km JOIN !!!!!!
# MAGIC
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC
# MAGIC where nuts3_2021.ISO2	 ='LU' and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC and 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1990,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1991,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1992,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1993,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1994,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1995,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1996,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1997,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1998,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1999,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2000,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2001,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2002,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2003,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2004,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2005,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2006,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2007,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2008,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2009,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2010,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2011,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2012,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2013,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2014,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2015,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2016,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2017,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2018,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2019,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2020,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2021,0)   >0
# MAGIC
# MAGIC
# MAGIC
# MAGIC group by 
# MAGIC   nuts3_2021.Category,
# MAGIC   nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(organic_soil.OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')),
# MAGIC   env_zones.Category 

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // set up of new time-series table for SOC flux -based on DayCent
# MAGIC
# MAGIC val SOC_FLUX_DAYCENT_spark = spark.sql(""" 
# MAGIC  
# MAGIC SELECT 
# MAGIC   nuts3_2021.Category,
# MAGIC  --- nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   sum(nuts3_2021.AreaHa) as AreaHa,
# MAGIC
# MAGIC  -- sum(nuts3_2021.AreaHa*10000) as Area_m2,
# MAGIC
# MAGIC   ---sum(ifnull(SOC_g_m2_90_21_epsg3035_1990 *10000))  , --- / sum(nuts3_2021.AreaHa)*10000 / 1000000 as y1990 , ---- to get tones
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1990,0))  as y1990 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1991,0))  as y1991 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1992,0))  as y1992 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1993,0))  as y1993 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1994,0))  as y1994 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1995,0))  as y1995 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1996,0))  as y1996 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1997,0))  as y1997 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1998,0))  as y1998 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_1999,0))  as y1999 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2000,0))  as y2000 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2001,0))  as y2001 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2002,0))  as y2002 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2003,0))  as y2003 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2004,0))  as y2004 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2005,0))  as y2005 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2006,0))  as y2006 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2007,0))  as y2007 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2008,0))  as y2008 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2009,0))  as y2009 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2010,0))  as y2010 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2011,0))  as y2011 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2012,0))  as y2012 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2013,0))  as y2013 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2014,0))  as y2014 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2015,0))  as y2015 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2016,0))  as y2016 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2017,0))  as y2017 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2018,0))  as y2018 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2019,0)) as y2019 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2020,0))  as y2020 ,
# MAGIC   sum(ifnull(SOC_g_m2_90_21_epsg3035_2021,0))  as y2021 ,
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(organic_soil.OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC
# MAGIC from nuts3_2021
# MAGIC LEFT JOIN soc_flux_daycent on nuts3_2021.GridNum1km = soc_flux_daycent.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN organic_soil     on nuts3_2021.GridNum1km =     organic_soil.gridnum         ------ 1km JOIN !!!!!!
# MAGIC
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC and 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1990,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1991,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1992,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1993,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1994,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1995,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1996,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1997,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1998,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_1999,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2000,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2001,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2002,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2003,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2004,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2005,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2006,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2007,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2008,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2009,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2010,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2011,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2012,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2013,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2014,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2015,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2016,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2017,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2018,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2019,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2020,0)  + 
# MAGIC ifnull(SOC_g_m2_90_21_epsg3035_2021,0)   >0
# MAGIC
# MAGIC
# MAGIC
# MAGIC group by 
# MAGIC   nuts3_2021.Category,
# MAGIC  --- nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(organic_soil.OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')),
# MAGIC   env_zones.Category 
# MAGIC             """)
# MAGIC
# MAGIC             
# MAGIC SOC_FLUX_DAYCENT_spark.createOrReplaceTempView("SOC_FLUX_DAYCENT_cube")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOC_FLUX_DAYCENT_cube

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# bring sql to pandas>
sql_for_panda = spark.sql('''
Select  * from SOC_FLUX_DAYCENT_cube
''')

df = sql_for_panda.select("*").toPandas()


#df_transformed =df.melt(id_vars=['Category',	'GridNum10km',	'ADM_ID',	'ADM_COUNTRY',	'ISO2',	'LEVEL3_name',	'LEVEL2_name',	'LEVEL1_name',	'LEVEL0_name',	'LEVEL3_code',	'LEVEL2_code',	'LEVEL1_code',	'LEVEL0_code',	'NUTS_EU',	'TAA',	'LULUCF_CODE',	'LULUCF_DESCRIPTION',	'soil_type',	'env_zones', ], var_name="year", value_name="soc")

df_transformed =df.melt(id_vars=['Category','ADM_ID',	'ADM_COUNTRY',	'ISO2',	'LEVEL3_name',	'LEVEL2_name',	'LEVEL1_name',	'LEVEL0_name',	'LEVEL3_code',	'LEVEL2_code',	'LEVEL1_code',	'LEVEL0_code',	'NUTS_EU',	'TAA',	'LULUCF_CODE',	'LULUCF_DESCRIPTION',	'soil_type',	'env_zones', 'AreaHa'], var_name="year", value_name="soc")



# COMMAND ----------

df_transformed

# COMMAND ----------

# MAGIC
# MAGIC %python
# MAGIC # panda df to SPARK:
# MAGIC df_scala = spark.createDataFrame(df_transformed)
# MAGIC df_scala.createOrReplaceTempView("SOC_FLUX_daycent_cube_transfomred")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val tableDF_export_db = spark.sql("""Select * from SOC_FLUX_daycent_cube_transfomred """)
# MAGIC tableDF_export_db
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_FLUX1_daycent")

# COMMAND ----------

# MAGIC %md ### (2.3) AGB STOCK
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/agb.JPG?raw=true?raw=true)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md #### (2.3.1) DASHBOARD  AGB-STOCK (A)  ESA CCI (100m) 2018 for Forest, Agro-forestry (clc_244) and (clc_221, 223,223) permanent crops  Settlements  (check UNITS!!)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from AGB_2018

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- TEST
# MAGIC
# MAGIC SELECT 
# MAGIC   
# MAGIC   nuts3_2021.Category, ----FOR ADMIN
# MAGIC   
# MAGIC   ----nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC
# MAGIC   ----CLC_2018.Category as clc18_level3_class,
# MAGIC
# MAGIC   if(CLC_2018.Category  =224,'Agro-forestry areas',         'other clc') as agro_forestry,
# MAGIC   if(CLC_2018.Category in (221, 223,223),'Permanent crops','other clc') as permanent_crops,
# MAGIC
# MAGIC
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(AGB_2018.esacciagb2018)  as esacciagb2018,        -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) (raster dataset). 
# MAGIC   SUM(AGB_2018.esacciagbsd2018)  as esacciagbsd2018,    -- per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha (raster dataset)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC   
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN AGB_2018     on nuts3_2021.GridNum = AGB_2018.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC LEFT JOIN CLC_2018     on nuts3_2021.GridNum = CLC_2018.GridNum ---
# MAGIC
# MAGIC
# MAGIC where  nuts3_2021.ISO2= 'LU' and lULUCF_2018.LULUCF_CODE in ('FL','CL','SL','OL')
# MAGIC
# MAGIC
# MAGIC
# MAGIC ---where nuts3_2021.LEVEL3_code is not null 
# MAGIC group by 
# MAGIC
# MAGIC   nuts3_2021.Category,
# MAGIC   ----nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.ADM_ID,
# MAGIC   nuts3_2021.ADM_COUNTRY	,
# MAGIC   nuts3_2021.ISO2	,
# MAGIC   nuts3_2021.LEVEL3_name	,
# MAGIC   nuts3_2021.LEVEL2_name	,
# MAGIC   nuts3_2021.LEVEL1_name	,
# MAGIC   nuts3_2021.LEVEL0_name	,
# MAGIC   nuts3_2021.LEVEL3_code	,
# MAGIC   nuts3_2021.LEVEL2_code	,
# MAGIC   nuts3_2021.LEVEL1_code	,
# MAGIC   nuts3_2021.LEVEL0_code	,
# MAGIC   nuts3_2021.NUTS_EU,	
# MAGIC   nuts3_2021.TAA ,
# MAGIC   ---CLC_2018.Category,
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   env_zones.Category ,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')),
# MAGIC
# MAGIC   if(CLC_2018.Category  =224,'Agro-forestry areas',         'other clc') ,
# MAGIC   if(CLC_2018.Category in (221, 223,223),'Permanent crops','other clc') 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC /// exporting AGB stock 
# MAGIC val tableDF_export_db_nuts3_agb1 = spark.sql("""
# MAGIC
# MAGIC       SELECT 
# MAGIC         
# MAGIC         nuts3_2021.Category, ----FOR ADMIN
# MAGIC         
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC
# MAGIC         ----CLC_2018.Category as clc18_level3_class,
# MAGIC
# MAGIC         if(CLC_2018.Category  =224,'Agro-forestry areas',         'other clc') as agro_forestry,
# MAGIC         if(CLC_2018.Category in (221, 223,223),'Permanent crops','other clc') as permanent_crops,
# MAGIC
# MAGIC
# MAGIC         SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC         SUM(AGB_2018.esacciagb2018)  as esacciagb2018,        -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) (raster dataset). 
# MAGIC         SUM(AGB_2018.esacciagbsd2018)  as esacciagbsd2018,    -- per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha (raster dataset)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC         env_zones.Category as env_zones
# MAGIC         
# MAGIC
# MAGIC       from nuts3_2021
# MAGIC
# MAGIC       LEFT JOIN AGB_2018     on nuts3_2021.GridNum = AGB_2018.GridNum
# MAGIC       LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC       LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC       LEFT JOIN CLC_2018     on nuts3_2021.GridNum = CLC_2018.GridNum ---
# MAGIC
# MAGIC
# MAGIC       where  nuts3_2021.LEVEL3_code is not null  and lULUCF_2018.LULUCF_CODE in ('FL','CL','SL','OL')
# MAGIC  
# MAGIC       group by 
# MAGIC
# MAGIC         nuts3_2021.Category,
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ---CLC_2018.Category,
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         env_zones.Category ,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')),
# MAGIC
# MAGIC         if(CLC_2018.Category  =224,'Agro-forestry areas',         'other clc') ,
# MAGIC         if(CLC_2018.Category in (221, 223,223),'Permanent crops','other clc') 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC """)
# MAGIC tableDF_export_db_nuts3_agb1
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB/AGB_STOCK1_ESA_CCI2018")

# COMMAND ----------

# MAGIC %md #### (2.3.2) DASHBOARD  AGB-STOCK (B)  GDMP2018 for Grassland 
# MAGIC
# MAGIC for grassland ghte gdmp2018 1km (100m) will be used

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from GDMP_1km_99_19

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC         nuts3_2021.Category, 
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ----CLC_2018.Category as clc18_level3_class,
# MAGIC         SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC         SUM(GDMP_1km_99_19.GDMP_2018) as GDMP_2018,
# MAGIC         
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC         env_zones.Category as env_zones
# MAGIC     
# MAGIC       from nuts3_2021
# MAGIC
# MAGIC       LEFT JOIN GDMP_1km_99_19     on nuts3_2021.GridNum1km = GDMP_1km_99_19.GridNum ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN lULUCF_2018        on nuts3_2021.GridNum =    lULUCF_2018.GridNum
# MAGIC       LEFT JOIN organic_soil       on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN env_zones          on nuts3_2021.GridNum =    env_zones.GridNum
# MAGIC      ----- LEFT JOIN CLC_2018           on nuts3_2021.GridNum =    CLC_2018.GridNum ---
# MAGIC
# MAGIC
# MAGIC       where  nuts3_2021.LEVEL3_code is not null  and lULUCF_2018.LULUCF_CODE in ('GL')
# MAGIC
# MAGIC     group by 
# MAGIC
# MAGIC         nuts3_2021.Category,
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ---CLC_2018.Category,
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         env_zones.Category ,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC /// exporting AGB stock  2 -gdmp 1km 2018
# MAGIC val tableDF_export_db_nuts3_agb2 = spark.sql("""
# MAGIC
# MAGIC SELECT 
# MAGIC         nuts3_2021.Category, 
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ----CLC_2018.Category as clc18_level3_class,
# MAGIC         SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC         SUM(GDMP_1km_99_19.GDMP_2018) as GDMP_2018,
# MAGIC         
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC         env_zones.Category as env_zones
# MAGIC     
# MAGIC       from nuts3_2021
# MAGIC
# MAGIC       LEFT JOIN GDMP_1km_99_19     on nuts3_2021.GridNum1km = GDMP_1km_99_19.GridNum ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN lULUCF_2018        on nuts3_2021.GridNum =    lULUCF_2018.GridNum
# MAGIC       LEFT JOIN organic_soil       on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN env_zones          on nuts3_2021.GridNum =    env_zones.GridNum
# MAGIC      ----- LEFT JOIN CLC_2018           on nuts3_2021.GridNum =    CLC_2018.GridNum ---
# MAGIC
# MAGIC
# MAGIC       where  nuts3_2021.LEVEL3_code is not null  and lULUCF_2018.LULUCF_CODE in ('GL')
# MAGIC
# MAGIC     group by 
# MAGIC
# MAGIC         nuts3_2021.Category,
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ---CLC_2018.Category,
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         env_zones.Category ,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC
# MAGIC
# MAGIC
# MAGIC """)
# MAGIC tableDF_export_db_nuts3_agb2
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB/AGB_STOCK2_GDMP_1km_2018")

# COMMAND ----------

# MAGIC %md ### (2.4) AGB FLUX 

# COMMAND ----------

# MAGIC %md ### (2.5) BGB STOCK
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/bgb.JPG?raw=true?raw=true?raw=true)

# COMMAND ----------

# MAGIC %md #### (2.5,1) BGB STOCK for forest

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from BGB_forest_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC --- TESTING bgb:
# MAGIC SELECT 
# MAGIC         
# MAGIC         nuts3_2021.Category, ----FOR ADMIN
# MAGIC         
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC
# MAGIC         SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC
# MAGIC         SUM(BGB_forest_2020.FCM_Europe_demo_2020_BGB)  as FCM_Europe_demo_2020_BGB,        -- 
# MAGIC         SUM(BGB_forest_2020.FCM_Europe_demo_2020_BGB_SD)  as FCM_Europe_demo_2020_BGB_SD,    --  standard deviation 
# MAGIC
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC         env_zones.Category as env_zones
# MAGIC         
# MAGIC
# MAGIC       from nuts3_2021
# MAGIC
# MAGIC       LEFT JOIN BGB_forest_2020     on nuts3_2021.GridNum = BGB_forest_2020.GridNum
# MAGIC       LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC       LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC       where  nuts3_2021.LEVEL3_code is not null  and lULUCF_2018.LULUCF_CODE in ('FL')
# MAGIC  
# MAGIC       group by 
# MAGIC
# MAGIC         nuts3_2021.Category,
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ---CLC_2018.Category,
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         env_zones.Category ,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC
# MAGIC       

# COMMAND ----------

# MAGIC %scala
# MAGIC /// exporting BGB stock 1 forest 
# MAGIC val tableDF_export_db_nuts3_bgb1 = spark.sql("""
# MAGIC
# MAGIC SELECT 
# MAGIC         
# MAGIC         nuts3_2021.Category, ----FOR ADMIN
# MAGIC         
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC
# MAGIC         SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC
# MAGIC         SUM(BGB_forest_2020.FCM_Europe_demo_2020_BGB)  as FCM_Europe_demo_2020_BGB,        -- 
# MAGIC         SUM(BGB_forest_2020.FCM_Europe_demo_2020_BGB_SD)  as FCM_Europe_demo_2020_BGB_SD,    --  standard deviation 
# MAGIC
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC         env_zones.Category as env_zones
# MAGIC         
# MAGIC
# MAGIC       from nuts3_2021
# MAGIC
# MAGIC       LEFT JOIN BGB_forest_2020     on nuts3_2021.GridNum = BGB_forest_2020.GridNum
# MAGIC       LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC       LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC       where  nuts3_2021.LEVEL3_code is not null  and lULUCF_2018.LULUCF_CODE in ('FL')
# MAGIC  
# MAGIC       group by 
# MAGIC
# MAGIC         nuts3_2021.Category,
# MAGIC         ----nuts3_2021.GridNum10km,
# MAGIC         nuts3_2021.ADM_ID,
# MAGIC         nuts3_2021.ADM_COUNTRY	,
# MAGIC         nuts3_2021.ISO2	,
# MAGIC         nuts3_2021.LEVEL3_name	,
# MAGIC         nuts3_2021.LEVEL2_name	,
# MAGIC         nuts3_2021.LEVEL1_name	,
# MAGIC         nuts3_2021.LEVEL0_name	,
# MAGIC         nuts3_2021.LEVEL3_code	,
# MAGIC         nuts3_2021.LEVEL2_code	,
# MAGIC         nuts3_2021.LEVEL1_code	,
# MAGIC         nuts3_2021.LEVEL0_code	,
# MAGIC         nuts3_2021.NUTS_EU,	
# MAGIC         nuts3_2021.TAA ,
# MAGIC         ---CLC_2018.Category,
# MAGIC         lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         env_zones.Category ,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC
# MAGIC
# MAGIC """)
# MAGIC tableDF_export_db_nuts3_bgb1
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/BGB/BGB_STOCK1_forest_2020")

# COMMAND ----------

# MAGIC %md # (ANNEX) Testing AREA----------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md ## Testing gridnum in different sizes: 100m 1000m 10 000m

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC gridnum &  -16777216 as GridNUM1km,
# MAGIC gridnum &  -4294967296 as GridNUM1km10_test,
# MAGIC
# MAGIC *
# MAGIC
# MAGIC from nuts3_2021

# COMMAND ----------

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
