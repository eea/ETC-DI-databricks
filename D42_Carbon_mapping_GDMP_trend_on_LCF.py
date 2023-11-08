# Databricks notebook source
# MAGIC %md # D42 Carbon mapping GDMP trend on LCF 
# MAGIC
# MAGIC The notebook will produce the tables, which needed to update the following statistics:
# MAGIC - https://www.eea.europa.eu/ims/impact-of-land-use-on
# MAGIC
# MAGIC The Productivity will be expressed by the  Gross Dry Matter Productivity GDMP:
# MAGIC - https://land.copernicus.eu/global/products/DMP
# MAGIC

# COMMAND ----------

# MAGIC %md ## 1) Reading DIMs  
# MAGIC LETS START WITH READING THE INPUT DATA.
# MAGIC
# MAGIC the following box reads all single DIMS and LUT from Azure:

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
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (3) CLC and LUT-LandCoverFlows carbon sequestration ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //(A) Reading lookup table for LandCover Flows: Carbon and Biodiversity #############################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Lookup/show?lookUpId=139
# MAGIC //cwsblobstorage01/cwsblob01/Lookups/LandCoverFlow_ca_bio/20220727134022.637.csv
# MAGIC
# MAGIC
# MAGIC //cwsblobstorage01/cwsblob01/LookupTablesFiles/LUT_full_july_2022_csv_139x_139.csv 
# MAGIC
# MAGIC val schema_lcf_carbon_bio = new StructType()
# MAGIC .add("Direction",StringType,true)
# MAGIC .add("CLC0018",LongType,true)
# MAGIC .add("CLC name_from",StringType,true)
# MAGIC .add("CLC name_to",StringType,true)
# MAGIC .add("LD_C",StringType,true)
# MAGIC .add("LD_Biodiversity",StringType,true)
# MAGIC .add("LD_code_level1",LongType,true)
# MAGIC .add("LD_name_level1",StringType,true)
# MAGIC .add("LD_code_name_level1",StringType,true)
# MAGIC .add("LD_code_level2",StringType,true)     // changed to text
# MAGIC .add("LD_name_level2",StringType,true)
# MAGIC .add("LD_code_name_level2",StringType,true)
# MAGIC .add("Area_ha_distribution_2000_2018",FloatType,true)
# MAGIC
# MAGIC
# MAGIC val lcf_carbon_bio  = spark.read.format("csv")
# MAGIC     .options(Map("delimiter"->","))
# MAGIC     .schema(schema_lcf_carbon_bio)
# MAGIC      ////.load("dbfs:/mnt/trainingDatabricks/Lookups/LandCoverFlow_ca_bio/20220727134022.637.csv") ////founding NULL values for CLC0018 row 125
# MAGIC     .load("dbfs:/mnt/trainingDatabricks//LookupTablesFiles/LUT_LCF_version_August2022_comma_delimited_139.csv")
# MAGIC lcf_carbon_bio.createOrReplaceTempView("lcf_carbon_bio")
# MAGIC
# MAGIC //(B) Reading CLC accounting dim #############################################################
# MAGIC ////CLC      LCF carbon sequestration  2000-2018:
# MAGIC ///https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1086&fileId=106
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_LCF0018_106_2018328_100m
# MAGIC // the following lines are reading the CLC accounting dataset:
# MAGIC val parquetFileDF_CLC_accounting = spark.read.format("delta").parquet("dbfs:/mnt/trainingDatabricks/Dimensions/D_LCF0018_106_2018328_100m/")
# MAGIC parquetFileDF_CLC_accounting.createOrReplaceTempView("CLC_accounting")
# MAGIC
# MAGIC
# MAGIC
# MAGIC /// producing a LCF0018 CUBE with LCF classification -CLC  classes:
# MAGIC val LCF0018_clc_sq1 = spark.sql(""" 
# MAGIC   select 
# MAGIC       t1.GridNum,
# MAGIC       t1.GridNum10km,
# MAGIC       t1.clc0018 as clc0018_change_code,
# MAGIC       t1.clc00 as clc00,
# MAGIC       t1.clc18 as clc18,
# MAGIC       t1.CLC0018 as lcf_clc0018,
# MAGIC       LD_code_level1,
# MAGIC       LD_name_level1,
# MAGIC       LD_code_level2,
# MAGIC       LD_name_level2,
# MAGIC       LD_code_name_level1,
# MAGIC       LD_code_name_level2,
# MAGIC       LD_C as LD_Carbon_mapping,
# MAGIC       LD_Biodiversity as LD_Biodiversity_mapping,
# MAGIC       t1.areaHa
# MAGIC FROM CLC_accounting t1
# MAGIC    LEFT JOIN   lcf_carbon_bio  t2 ON t1.clc0018 = t2.CLC0018 
# MAGIC
# MAGIC    ---LEFT JOIN   LUT_clc_classes t3_2000  ON t1.clc00 = t3_2000.LEVEL3_CODE  
# MAGIC    ---LEFT JOIN   LUT_clc_classes t3_2018  ON t1.clc18 = t3_2018.LEVEL3_CODE  
# MAGIC
# MAGIC    where clc00 <> clc18 """)
# MAGIC                          
# MAGIC LCF0018_clc_sq1.createOrReplaceTempView("LCF0018_clc_sq1")    
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
# MAGIC parquetFileDF_soc_stock_ext_wetland.createOrReplaceTempView("soc_stock_ext_wetland_draft")
# MAGIC
# MAGIC
# MAGIC // Reading the LUT wetland - SOC values...:
# MAGIC //https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=150
# MAGIC //cwsblobstorage01/cwsblob01/Lookups/extwetCstockLUT/20230717151903.49.csv
# MAGIC
# MAGIC val schema_lut_wetland= new StructType()
# MAGIC .add("FIT",LongType,true)
# MAGIC .add("Extwet_Cla",StringType,true)
# MAGIC .add("C_stok",FloatType,true)
# MAGIC
# MAGIC val lut_wetland  = spark.read.format("csv")
# MAGIC .schema(schema_lut_wetland)
# MAGIC .options(Map("delimiter"->"|"))
# MAGIC  //.option("header", "true")
# MAGIC    .load("dbfs:/mnt/trainingDatabricks/Lookups/extwetCstockLUT/20230717151903.49.csv")     
# MAGIC lut_wetland.createOrReplaceTempView("LUT_wetland")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC val soc_wetland = spark.sql(""" 
# MAGIC          
# MAGIC       Select * from soc_stock_ext_wetland_draft
# MAGIC       left join LUT_wetland on wetlands_categories=FIT                                         
# MAGIC                                                         """)                                  
# MAGIC soc_wetland.createOrReplaceTempView("soc_stock_ext_wetland")  
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
# MAGIC //##########################################################################################################################################
# MAGIC //// 14 (GDMP 100m   2014-2022)  100m -- ############################## 100m DIM
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
# MAGIC
# MAGIC       if(GDMP_2014_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2015_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2016_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2017_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2018_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       if(GDMP_2019_pv_100m_EPSG3035= 0 , 1 , 
# MAGIC       0))))))
# MAGIC          as QC_gap_YES
# MAGIC          from GDMP_100m_14_22_raw
# MAGIC                                                       """)                                  
# MAGIC parquetFileDF_gdmp_100m_2.createOrReplaceTempView("GDMP_100m_14_22")  
# MAGIC
# MAGIC
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
# MAGIC val parquetFileDF_gdmp_1km_full = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_pv_1042_2023918_1km/")
# MAGIC parquetFileDF_gdmp_1km_full.createOrReplaceTempView("gdmp_1km_pv_raw")
# MAGIC
# MAGIC
# MAGIC // we found GAPs in the time-series.. therefore we add. an attribute which shows the gaps [QC_gap_YES]
# MAGIC // if the attribute is 1, then this row should not be used for statistics OR a gab filling should be done:
# MAGIC
# MAGIC val GDMP_1km_99_22 = spark.sql(""" 
# MAGIC
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
# MAGIC //##########################################################################################################################################
# MAGIC //// 13 (GDMP 1km  STATISTICS 1999-2022)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2021&fileId=1043
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km_stat = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_gdmp_1km_statistic_c_1043_2023918_1km/")
# MAGIC parquetFileDF_gdmp_1km_stat.createOrReplaceTempView("GDMP_1km_statistics")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 14(GDMP 1km  TREND 2000-2022)  1km-- ############################## 1000m DIM
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC //  https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2029&fileId=1051&successMessage=true
# MAGIC //  cwsblobstorage01/cwsblob01/Dimensions/D_GDMP_1km_trend_analy_1051_2023928_1km
# MAGIC
# MAGIC val parquetFileDF_gdmp_1km_trend = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_GDMP_1km_trend_analy_1051_2023928_1km/")
# MAGIC parquetFileDF_gdmp_1km_trend.createOrReplaceTempView("GDMP_1km_trend")
# MAGIC
# MAGIC
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
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %md ## 2) Building CUBES

# COMMAND ----------

# MAGIC %md ### 2.1) CUBE (1) Referenc-DATASET

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns from LCF0018_clc_sq1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC   
# MAGIC                     nuts3_2021.Category as admin_category, ----FOR ADMIN
# MAGIC                     nuts3_2021.GridNum10km,
# MAGIC                     nuts3_2021.GridNum1km,
# MAGIC                      clc0018_change_code    , 
# MAGIC                     clc00,
# MAGIC                     clc18,
# MAGIC                     ---lcf_clc0018,
# MAGIC                     ---LD_code_level1,
# MAGIC                     ---LD_name_level1,
# MAGIC                     ---LD_code_level2,
# MAGIC                     ---LD_name_level2,
# MAGIC                     ---LD_code_name_level1    , 
# MAGIC                     ---LD_code_name_level2    , 
# MAGIC                     ---LD_Carbon_mapping,
# MAGIC                     ---LD_Biodiversity_mapping , 
# MAGIC
# MAGIC                     if(lULUCF_2018.LULUCF_CODE is null, 'none',lULUCF_2018.LULUCF_CODE) as LULUCF_CODE,
# MAGIC                     if(env_zones.Category is null, 'none',env_zones.Category) as env_zones,
# MAGIC                     if(natura2000_protection is null, 'none Nature 2000 protection',natura2000_protection) as natura2000_protection,
# MAGIC                     ----,if(PA_2022_protection == 'protected','protected', 'not protected') as Pa2022_100m_NET
# MAGIC                      SUM(nuts3_2021.AreaHa) as AreaHa
# MAGIC
# MAGIC                     from nuts3_2021
# MAGIC                     LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC
# MAGIC                     LEFT JOIN LCF0018_clc_sq1 on nuts3_2021.GridNum  = LCF0018_clc_sq1.GridNum
# MAGIC
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
# MAGIC                         clc0018_change_code    , 
# MAGIC                         clc00,
# MAGIC                         clc18,
# MAGIC                         ---lcf_clc0018,
# MAGIC                         ---LD_code_level1,
# MAGIC                         ---LD_name_level1,
# MAGIC                         ---LD_code_level2,
# MAGIC                         ---LD_name_level2,
# MAGIC                         ---LD_code_name_level1    , 
# MAGIC                         ---LD_code_name_level2    , 
# MAGIC                         ---LD_Carbon_mapping,
# MAGIC                         ---LD_Biodiversity_mapping , 
# MAGIC                         lULUCF_2018.LULUCF_CODE,
# MAGIC                         env_zones.Category ,
# MAGIC                         natura2000_protection 
# MAGIC                         --,Pa2022_100m_NET
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

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
# MAGIC                      clc0018_change_code    , 
# MAGIC                     clc00,
# MAGIC                     clc18,
# MAGIC                     ---lcf_clc0018,
# MAGIC                     ---LD_code_level1,
# MAGIC                     ---LD_name_level1,
# MAGIC                     ---LD_code_level2,
# MAGIC                     ---LD_name_level2,
# MAGIC                     ---LD_code_name_level1    , 
# MAGIC                     ---LD_code_name_level2    , 
# MAGIC                     ---LD_Carbon_mapping,
# MAGIC                     ---LD_Biodiversity_mapping , 
# MAGIC
# MAGIC                     if(lULUCF_2018.LULUCF_CODE is null, 'none',lULUCF_2018.LULUCF_CODE) as LULUCF_CODE,
# MAGIC                     if(env_zones.Category is null, 'none',env_zones.Category) as env_zones,
# MAGIC                     if(natura2000_protection is null, 'none Nature 2000 protection',natura2000_protection) as natura2000_protection,
# MAGIC                     ----,if(PA_2022_protection == 'protected','protected', 'not protected') as Pa2022_100m_NET
# MAGIC                      SUM(nuts3_2021.AreaHa) as AreaHa
# MAGIC
# MAGIC                     from nuts3_2021
# MAGIC                     LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC
# MAGIC                     LEFT JOIN LCF0018_clc_sq1 on nuts3_2021.GridNum  = LCF0018_clc_sq1.GridNum
# MAGIC
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
# MAGIC                         clc0018_change_code    , 
# MAGIC                         clc00,
# MAGIC                         clc18,
# MAGIC                         ---lcf_clc0018,
# MAGIC                         ---LD_code_level1,
# MAGIC                         ---LD_name_level1,
# MAGIC                         ---LD_code_level2,
# MAGIC                         ---LD_name_level2,
# MAGIC                         ---LD_code_name_level1    , 
# MAGIC                         ---LD_code_name_level2    , 
# MAGIC                         ---LD_Carbon_mapping,
# MAGIC                         ---LD_Biodiversity_mapping , 
# MAGIC                         lULUCF_2018.LULUCF_CODE,
# MAGIC                         env_zones.Category ,
# MAGIC                         natura2000_protection 
# MAGIC                         --,Pa2022_100m_NET
# MAGIC
# MAGIC             """)
# MAGIC //ref_cube
# MAGIC //    .coalesce(1) //be careful with this
# MAGIC //    .write.format("com.databricks.spark.csv")
# MAGIC //    .mode(SaveMode.Overwrite)
# MAGIC //    .option("sep","|")
# MAGIC //    .option("overwriteSchema", "true")
# MAGIC //    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC //    .option("emptyValue", "")
# MAGIC //    .option("header","true")
# MAGIC //    .option("treatEmptyValuesAsNulls", "true")  
# MAGIC //  
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube")
# MAGIC
# MAGIC  ref_cube.createOrReplaceTempView("ref_cube")
# MAGIC
# MAGIC  
# MAGIC /// REFCUBE . without 1km >
# MAGIC //
# MAGIC //val ref_cube2 = spark.sql("""
# MAGIC //                SELECT 
# MAGIC //  
# MAGIC //                    nuts3_2021.Category as admin_category, ----FOR ADMIN
# MAGIC //                    nuts3_2021.GridNum10km,
# MAGIC //    
# MAGIC //                    if(lULUCF_2018.LULUCF_CODE is null, 'none',lULUCF_2018.LULUCF_CODE) as LULUCF_CODE,
# MAGIC //                    if(env_zones.Category is null, 'none',env_zones.Category) as env_zones,
# MAGIC //                    if(natura2000_protection is null, 'none Nature 2000 protection',natura2000_protection) as natura2000_protection,
# MAGIC //                    ----,if(PA_2022_protection == 'protected','protected', 'not protected') as Pa2022_100m_NET
# MAGIC //                     SUM(nuts3_2021.AreaHa) as AreaHa
# MAGIC //
# MAGIC //                    from nuts3_2021
# MAGIC //                    LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC //                    LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC //                    LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC //                  ----  LEFT JOIN Pa2022_100m_NET     on nuts3_2021.GridNum = Pa2022_100m_NET.GridNum
# MAGIC //
# MAGIC //                    where nuts3_2021.ISO2 is not null
# MAGIC //
# MAGIC //                        group by 
# MAGIC //                        nuts3_2021.Category,
# MAGIC //                  
# MAGIC //                        nuts3_2021.GridNum10km,
# MAGIC //                        lULUCF_2018.LULUCF_CODE,
# MAGIC //                        env_zones.Category ,
# MAGIC //                        natura2000_protection 
# MAGIC //                        --,Pa2022_100m_NET
# MAGIC //
# MAGIC //            """)
# MAGIC //ref_cube2
# MAGIC //    .coalesce(1) //be careful with this
# MAGIC //    .write.format("com.databricks.spark.csv")
# MAGIC //    .mode(SaveMode.Overwrite)
# MAGIC //    .option("sep","|")
# MAGIC //    .option("overwriteSchema", "true")
# MAGIC //    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC //    .option("emptyValue", "")
# MAGIC //    .option("header","true")
# MAGIC //    .option("treatEmptyValuesAsNulls", "true")  
# MAGIC //  
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube2")
# MAGIC
# MAGIC  //ref_cube2.createOrReplaceTempView("ref_cube_2")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ### 2.2) CUBE (2) lcf-with gmdp and reference cube

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT  
# MAGIC  ref_cube.GridNum1km
# MAGIC ,ref_cube.GridNum10km
# MAGIC ,ref_cube.admin_category
# MAGIC ,ref_cube.env_zones
# MAGIC ,ref_cube.LULUCF_CODE
# MAGIC ,ref_cube.clc18
# MAGIC ,ref_cube.clc00
# MAGIC ,ref_cube.clc0018_change_code
# MAGIC ,ref_cube.lcf_clc0018
# MAGIC ,ref_cube.LD_name_level2
# MAGIC ,ref_cube.LD_name_level1
# MAGIC ,ref_cube.LD_code_name_level2
# MAGIC ,ref_cube.LD_code_name_level1
# MAGIC ,ref_cube.LD_code_level2
# MAGIC ,ref_cube.LD_code_level1
# MAGIC ,ref_cube.LD_Carbon_mapping
# MAGIC ,ref_cube.LD_Biodiversity_mapping
# MAGIC ,ref_cube.natura2000_protection
# MAGIC ,ref_cube.AreaHa
# MAGIC
# MAGIC
# MAGIC ,ifnull(GDMP_1999, 0) as GDMP_1999
# MAGIC ,ifnull(GDMP_2000, 0) as GDMP_2000
# MAGIC ,ifnull(GDMP_2001, 0) as GDMP_2001
# MAGIC ,ifnull(GDMP_2002, 0) as GDMP_2002
# MAGIC ,ifnull(GDMP_2003, 0) as GDMP_2003
# MAGIC ,ifnull(GDMP_2004, 0) as GDMP_2004
# MAGIC ,ifnull(GDMP_2005, 0) as GDMP_2005
# MAGIC ,ifnull(GDMP_2006, 0) as GDMP_2006
# MAGIC ,ifnull(GDMP_2007, 0) as GDMP_2007
# MAGIC ,ifnull(GDMP_2008, 0) as GDMP_2008
# MAGIC ,ifnull(GDMP_2009, 0) as GDMP_2009
# MAGIC ,ifnull(GDMP_2010, 0) as GDMP_2010
# MAGIC ,ifnull(GDMP_2011, 0) as GDMP_2011
# MAGIC ,ifnull(GDMP_2012, 0) as GDMP_2012
# MAGIC ,ifnull(GDMP_2013, 0) as GDMP_2013
# MAGIC ,ifnull(GDMP_2014, 0) as GDMP_2014
# MAGIC ,ifnull(GDMP_2015, 0) as GDMP_2015
# MAGIC ,ifnull(GDMP_2016, 0) as GDMP_2016
# MAGIC ,ifnull(GDMP_2017, 0) as GDMP_2017
# MAGIC ,ifnull(GDMP_2018, 0) as GDMP_2018
# MAGIC ,ifnull(GDMP_2019, 0) as GDMP_2019
# MAGIC ,ifnull(GDMP_2020, 0) as GDMP_2020
# MAGIC ,ifnull(GDMP_2021, 0) as GDMP_2021
# MAGIC ,ifnull(GDMP_2022, 0) as GDMP_2022
# MAGIC
# MAGIC
# MAGIC from
# MAGIC   ref_cube
# MAGIC   LEFT JOIN GDMP_1km_99_22  on GDMP_1km_99_22.GridNum1km   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC
# MAGIC limit 10
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC /// CUBE F (1)
# MAGIC //
# MAGIC val cube_f = spark.sql("""
# MAGIC       SELECT  
# MAGIC         ref_cube.GridNum1km
# MAGIC         ,ref_cube.GridNum10km
# MAGIC         ,ref_cube.admin_category
# MAGIC         ,ref_cube.env_zones
# MAGIC         ,ref_cube.LULUCF_CODE
# MAGIC         ,ref_cube.clc18
# MAGIC         ,ref_cube.clc00
# MAGIC         ,ref_cube.clc0018_change_code
# MAGIC        ---,ref_cube.lcf_clc0018
# MAGIC        ---,ref_cube.LD_name_level2
# MAGIC        ---,ref_cube.LD_name_level1
# MAGIC        ---,ref_cube.LD_code_name_level2
# MAGIC        ---,ref_cube.LD_code_name_level1
# MAGIC        ---,ref_cube.LD_code_level2
# MAGIC        ---,ref_cube.LD_code_level1
# MAGIC        ---,ref_cube.LD_Carbon_mapping
# MAGIC        ---,ref_cube.LD_Biodiversity_mapping
# MAGIC         ,ref_cube.natura2000_protection
# MAGIC         ,ref_cube.AreaHa
# MAGIC         ,ifnull(GDMP_1999, 0) as GDMP_1999
# MAGIC         ,ifnull(GDMP_2000, 0) as GDMP_2000
# MAGIC         ,ifnull(GDMP_2001, 0) as GDMP_2001
# MAGIC         ,ifnull(GDMP_2002, 0) as GDMP_2002
# MAGIC         ,ifnull(GDMP_2003, 0) as GDMP_2003
# MAGIC         ,ifnull(GDMP_2004, 0) as GDMP_2004
# MAGIC         ,ifnull(GDMP_2005, 0) as GDMP_2005
# MAGIC         ,ifnull(GDMP_2006, 0) as GDMP_2006
# MAGIC         ,ifnull(GDMP_2007, 0) as GDMP_2007
# MAGIC         ,ifnull(GDMP_2008, 0) as GDMP_2008
# MAGIC         ,ifnull(GDMP_2009, 0) as GDMP_2009
# MAGIC         ,ifnull(GDMP_2010, 0) as GDMP_2010
# MAGIC         ,ifnull(GDMP_2011, 0) as GDMP_2011
# MAGIC         ,ifnull(GDMP_2012, 0) as GDMP_2012
# MAGIC         ,ifnull(GDMP_2013, 0) as GDMP_2013
# MAGIC         ,ifnull(GDMP_2014, 0) as GDMP_2014
# MAGIC         ,ifnull(GDMP_2015, 0) as GDMP_2015
# MAGIC         ,ifnull(GDMP_2016, 0) as GDMP_2016
# MAGIC         ,ifnull(GDMP_2017, 0) as GDMP_2017
# MAGIC         ,ifnull(GDMP_2018, 0) as GDMP_2018
# MAGIC         ,ifnull(GDMP_2019, 0) as GDMP_2019
# MAGIC         ,ifnull(GDMP_2020, 0) as GDMP_2020
# MAGIC         ,ifnull(GDMP_2021, 0) as GDMP_2021
# MAGIC         ,ifnull(GDMP_2022, 0) as GDMP_2022
# MAGIC
# MAGIC   from
# MAGIC     ref_cube
# MAGIC     LEFT JOIN GDMP_1km_99_22  on GDMP_1km_99_22.GridNum1km   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC
# MAGIC             """)
# MAGIC //cube_f
# MAGIC //    .coalesce(1) //be careful with this
# MAGIC //    .write.format("com.databricks.spark.csv")
# MAGIC //    .mode(SaveMode.Overwrite)
# MAGIC //    .option("sep","|")
# MAGIC //    .option("overwriteSchema", "true")
# MAGIC //    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC //    .option("emptyValue", "")
# MAGIC //    .option("header","true")
# MAGIC //    .option("treatEmptyValuesAsNulls", "true")  
# MAGIC //  
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF")
# MAGIC
# MAGIC  cube_f.createOrReplaceTempView("cube_f")
# MAGIC
# MAGIC
# MAGIC  
# MAGIC /// CUBE C (2)
# MAGIC //
# MAGIC val cube_c = spark.sql("""
# MAGIC select 
# MAGIC           admin_category
# MAGIC           ,GridNum10km
# MAGIC           ,natura2000_protection
# MAGIC           ,env_zones
# MAGIC           ,clc18
# MAGIC           ,clc00
# MAGIC           ,LULUCF_CODE
# MAGIC       ---    ,lcf_clc0018
# MAGIC           ,clc0018_change_code
# MAGIC         ---,LD_name_level2
# MAGIC         ---,LD_name_level1
# MAGIC         ---,LD_code_name_level2
# MAGIC         ---,LD_code_name_level1
# MAGIC         ---,LD_code_level2
# MAGIC         ---,LD_code_level1
# MAGIC         ---,LD_Carbon_mapping
# MAGIC         ---,LD_Biodiversity_mapping
# MAGIC
# MAGIC           ,SUM(GDMP_2022) as GDMP_2022
# MAGIC           ,SUM(GDMP_2021) as GDMP_2021
# MAGIC           ,SUM(GDMP_2020) as GDMP_2020
# MAGIC           ,SUM(GDMP_2019) as GDMP_2019
# MAGIC           ,SUM(GDMP_2018) as GDMP_2018
# MAGIC           ,SUM(GDMP_2017) as GDMP_2017
# MAGIC           ,SUM(GDMP_2016) as GDMP_2016
# MAGIC           ,SUM(GDMP_2015) as GDMP_2015
# MAGIC           ,SUM(GDMP_2014) as GDMP_2014
# MAGIC           ,SUM(GDMP_2013) as GDMP_2013
# MAGIC           ,SUM(GDMP_2012) as GDMP_2012
# MAGIC           ,SUM(GDMP_2011) as GDMP_2011
# MAGIC           ,SUM(GDMP_2010) as GDMP_2010
# MAGIC           ,SUM(GDMP_2009) as GDMP_2009
# MAGIC           ,SUM(GDMP_2008) as GDMP_2008
# MAGIC           ,SUM(GDMP_2007) as GDMP_2007
# MAGIC           ,SUM(GDMP_2006) as GDMP_2006
# MAGIC           ,SUM(GDMP_2005) as GDMP_2005
# MAGIC           ,SUM(GDMP_2004) as GDMP_2004
# MAGIC           ,SUM(GDMP_2003) as GDMP_2003
# MAGIC           ,SUM(GDMP_2002) as GDMP_2002
# MAGIC           ,SUM(GDMP_2001) as GDMP_2001
# MAGIC           ,SUM(GDMP_2000) as GDMP_2000
# MAGIC           ,SUM(GDMP_1999) as GDMP_1999
# MAGIC           ,SUM(AreaHa) as AreaHa
# MAGIC
# MAGIC           from cube_f
# MAGIC           group by 
# MAGIC           admin_category
# MAGIC             ,GridNum10km
# MAGIC             ,natura2000_protection
# MAGIC             ,env_zones
# MAGIC             ,clc18
# MAGIC             ,clc00
# MAGIC             ,LULUCF_CODE
# MAGIC           ---  ,lcf_clc0018
# MAGIC             ,clc0018_change_code
# MAGIC            --- ,LD_name_level2
# MAGIC            --- ,LD_name_level1
# MAGIC            --- ,LD_code_name_level2
# MAGIC            --- ,LD_code_name_level1
# MAGIC            --- ,LD_code_level2
# MAGIC            --- ,LD_code_level1
# MAGIC            --- ,LD_Carbon_mapping
# MAGIC            --- ,LD_Biodiversity_mapping
# MAGIC             """)
# MAGIC //cube_c
# MAGIC //   .coalesce(1) //be careful with this
# MAGIC //   .write.format("com.databricks.spark.csv")
# MAGIC //   .mode(SaveMode.Overwrite)
# MAGIC //   .option("sep","|")
# MAGIC //   .option("overwriteSchema", "true")
# MAGIC //   .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC //   .option("emptyValue", "")
# MAGIC //   .option("header","true")
# MAGIC //   .option("treatEmptyValuesAsNulls", "true")  
# MAGIC // 
# MAGIC //   .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF")
# MAGIC
# MAGIC  cube_f.createOrReplaceTempView("cube_c")
# MAGIC
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

### FOR transforming and updating ESA_CCI_CUBE_STATUS_biomass table: ########################


# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# bring sql to pandas> ----------------------------table 1 ESA_CCI_CUBE_STATUS_biomass
sql_for_panda = spark.sql('''
SELECT *   from cube_c where admin_category is not null
''')

df = sql_for_panda.select("*").toPandas()

#for EVA https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html

df_transformed =df.melt(id_vars=[

 'admin_category'
,'GridNum10km'
,'natura2000_protection'
,'env_zones'
,'clc18'
,'clc00'
,'LULUCF_CODE'
,'clc0018_change_code'
,'AreaHa'
 ], var_name="year", value_name="GDMP")

## updapte year:  GDMP_2012
#df_transformed['year_link'] = df_transformed['year'].str[-4:]
##
### dataframe to table:
#df_transformed_c1 = spark.createDataFrame(df_transformed)
#df_transformed_c1.createOrReplaceTempView("df_transformed_gdmp")


# COMMAND ----------

## updapte year:  GDMP_2012
df_transformed['year_link'] = df_transformed['year'].str[-4:]
##
### dataframe to table:
df_transformed_c1 = spark.createDataFrame(df_transformed)
df_transformed_c1.createOrReplaceTempView("df_transformed_gdmp")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC  
# MAGIC /// CUBE C_pivot (2) export
# MAGIC //
# MAGIC val C_pivot = spark.sql("""
# MAGIC select  * from df_transformed_gdmp      
# MAGIC             """)
# MAGIC C_pivot
# MAGIC    .coalesce(1) //be careful with this
# MAGIC    .write.format("com.databricks.spark.csv")
# MAGIC    .mode(SaveMode.Overwrite)
# MAGIC    .option("sep","|")
# MAGIC    .option("overwriteSchema", "true")
# MAGIC    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC    .option("emptyValue", "")
# MAGIC    .option("header","true")
# MAGIC    .option("treatEmptyValuesAsNulls", "true")  
# MAGIC  
# MAGIC    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF_unpivot")
# MAGIC
# MAGIC  C_pivot.createOrReplaceTempView("C_pivot")

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF_unpivot"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print ("----------------------------------------------------------------")
        print (URL)
    


# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC  admin_category
# MAGIC ,GridNum10km
# MAGIC ,natura2000_protection
# MAGIC ,env_zones
# MAGIC ,clc18
# MAGIC ,clc00
# MAGIC ,LULUCF_CODE
# MAGIC ,lcf_clc0018
# MAGIC ,clc0018_change_code
# MAGIC ,LD_name_level2
# MAGIC ,LD_name_level1
# MAGIC ,LD_code_name_level2
# MAGIC ,LD_code_name_level1
# MAGIC ,LD_code_level2
# MAGIC ,LD_code_level1
# MAGIC ,LD_Carbon_mapping
# MAGIC ,LD_Biodiversity_mapping
# MAGIC
# MAGIC ,SUM(GDMP_2022) as GDMP_2022
# MAGIC ,SUM(GDMP_2021) as GDMP_2021
# MAGIC ,SUM(GDMP_2020) as GDMP_2020
# MAGIC ,SUM(GDMP_2019) as GDMP_2019
# MAGIC ,SUM(GDMP_2018) as GDMP_2018
# MAGIC ,SUM(GDMP_2017) as GDMP_2017
# MAGIC ,SUM(GDMP_2016) as GDMP_2016
# MAGIC ,SUM(GDMP_2015) as GDMP_2015
# MAGIC ,SUM(GDMP_2014) as GDMP_2014
# MAGIC ,SUM(GDMP_2013) as GDMP_2013
# MAGIC ,SUM(GDMP_2012) as GDMP_2012
# MAGIC ,SUM(GDMP_2011) as GDMP_2011
# MAGIC ,SUM(GDMP_2010) as GDMP_2010
# MAGIC ,SUM(GDMP_2009) as GDMP_2009
# MAGIC ,SUM(GDMP_2008) as GDMP_2008
# MAGIC ,SUM(GDMP_2007) as GDMP_2007
# MAGIC ,SUM(GDMP_2006) as GDMP_2006
# MAGIC ,SUM(GDMP_2005) as GDMP_2005
# MAGIC ,SUM(GDMP_2004) as GDMP_2004
# MAGIC ,SUM(GDMP_2003) as GDMP_2003
# MAGIC ,SUM(GDMP_2002) as GDMP_2002
# MAGIC ,SUM(GDMP_2001) as GDMP_2001
# MAGIC ,SUM(GDMP_2000) as GDMP_2000
# MAGIC ,SUM(GDMP_1999) as GDMP_1999
# MAGIC ,SUM(AreaHa) as AreaHa
# MAGIC
# MAGIC from cube_f
# MAGIC group by 
# MAGIC  admin_category
# MAGIC   ,GridNum10km
# MAGIC   ,natura2000_protection
# MAGIC   ,env_zones
# MAGIC   ,clc18
# MAGIC   ,clc00
# MAGIC   ,LULUCF_CODE
# MAGIC   ,lcf_clc0018
# MAGIC   ,clc0018_change_code
# MAGIC   ,LD_name_level2
# MAGIC   ,LD_name_level1
# MAGIC   ,LD_code_name_level2
# MAGIC   ,LD_code_name_level1
# MAGIC   ,LD_code_level2
# MAGIC   ,LD_code_level1
# MAGIC   ,LD_Carbon_mapping
# MAGIC   ,LD_Biodiversity_mapping

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### 2.3) CUBE (3) clc-change gmdp-trend and reference cube

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns from GDMP_1km_trend

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT  
# MAGIC         ref_cube.GridNum1km
# MAGIC         ,ref_cube.GridNum10km
# MAGIC         ,ref_cube.admin_category
# MAGIC         ,ref_cube.env_zones
# MAGIC         ,ref_cube.LULUCF_CODE
# MAGIC         ,ref_cube.clc18
# MAGIC         ,ref_cube.clc00
# MAGIC         ,ref_cube.clc0018_change_code
# MAGIC         ,ref_cube.natura2000_protection
# MAGIC         ,ref_cube.AreaHa
# MAGIC         ,GDMP_1km_00_22_pvalue
# MAGIC         ,GDMP_1km_00_22_relative_change
# MAGIC         ,GDMP_1km_00_22_slope
# MAGIC         ,GDMP_1km_00_22_trend
# MAGIC
# MAGIC   from
# MAGIC     ref_cube
# MAGIC     LEFT JOIN GDMP_1km_trend  on GDMP_1km_trend.gridnum   =ref_cube.GridNum1km  ---1km JOIN!!!

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC /// CUBE F trend (1)
# MAGIC //
# MAGIC val cube_trend_f = spark.sql("""
# MAGIC
# MAGIC   SELECT  
# MAGIC         ref_cube.GridNum1km
# MAGIC         ,ref_cube.GridNum10km
# MAGIC         ,ref_cube.admin_category
# MAGIC         ,ref_cube.env_zones
# MAGIC         ,ref_cube.LULUCF_CODE
# MAGIC         ,ref_cube.clc18
# MAGIC         ,ref_cube.clc00
# MAGIC         ,ref_cube.clc0018_change_code
# MAGIC         ,ref_cube.natura2000_protection
# MAGIC         ,ref_cube.AreaHa
# MAGIC         ,GDMP_1km_00_22_pvalue
# MAGIC         ,GDMP_1km_00_22_relative_change
# MAGIC         ,GDMP_1km_00_22_slope
# MAGIC         ,GDMP_1km_00_22_trend
# MAGIC
# MAGIC   from
# MAGIC     ref_cube
# MAGIC     LEFT JOIN GDMP_1km_trend  on GDMP_1km_trend.gridnum   =ref_cube.GridNum1km  ---1km JOIN!!!
# MAGIC
# MAGIC
# MAGIC             """)
# MAGIC //cube_trend_f
# MAGIC //    .coalesce(1) //be careful with this
# MAGIC //    .write.format("com.databricks.spark.csv")
# MAGIC //    .mode(SaveMode.Overwrite)
# MAGIC //    .option("sep","|")
# MAGIC //    .option("overwriteSchema", "true")
# MAGIC //    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC //    .option("emptyValue", "")
# MAGIC //    .option("header","true")
# MAGIC //    .option("treatEmptyValuesAsNulls", "true")  
# MAGIC //  
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF/cube_trend_f")
# MAGIC
# MAGIC  cube_trend_f.createOrReplaceTempView("cube_trend_f")
# MAGIC
# MAGIC
# MAGIC  
# MAGIC /// CUBE C (2)
# MAGIC //
# MAGIC val cube_trend_c = spark.sql("""
# MAGIC
# MAGIC   SELECT  
# MAGIC     
# MAGIC          admin_category
# MAGIC         ,env_zones
# MAGIC         ,LULUCF_CODE
# MAGIC         ,clc18
# MAGIC         ,clc00
# MAGIC         ,clc0018_change_code
# MAGIC         ,natura2000_protection
# MAGIC         ,sum(AreaHa) as AreaHa
# MAGIC         ,SUM(GDMP_1km_00_22_relative_change* AreaHa )/ SUM(AreaHa) as GDMP_1km_00_22_relative_change
# MAGIC         ,SUM(GDMP_1km_00_22_slope* AreaHa )/ SUM(AreaHa) as GDMP_1km_00_22_slope
# MAGIC   from cube_trend_f
# MAGIC
# MAGIC   group by          admin_category
# MAGIC         ,env_zones
# MAGIC         ,LULUCF_CODE
# MAGIC         ,clc18
# MAGIC         ,clc00
# MAGIC         ,clc0018_change_code
# MAGIC         ,natura2000_protection
# MAGIC             """)
# MAGIC cube_trend_c
# MAGIC    .coalesce(1) //be careful with this
# MAGIC    .write.format("com.databricks.spark.csv")
# MAGIC    .mode(SaveMode.Overwrite)
# MAGIC    .option("sep","|")
# MAGIC    .option("overwriteSchema", "true")
# MAGIC    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC    .option("emptyValue", "")
# MAGIC    .option("header","true")
# MAGIC    .option("treatEmptyValuesAsNulls", "true")  
# MAGIC  
# MAGIC    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF/cube_trend_c")
# MAGIC
# MAGIC  cube_trend_c.createOrReplaceTempView("cube_trend_c")
# MAGIC
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/GDMP_LCF/cube_trend_c"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print ("----------------------------------------------------------------")
        print (URL)


# COMMAND ----------



# COMMAND ----------


