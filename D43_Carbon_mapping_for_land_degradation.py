# Databricks notebook source
# MAGIC %md # Carbon mapping (land degradation)
# MAGIC
# MAGIC ![](https://space4environment.com/fileadmin/Resources/Public/Images/Logos/S4E-Logo.png)
# MAGIC
# MAGIC https://app.heygen.com/share/982039d83d714866b6816e50fe2989b1
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
# MAGIC /// producing a LCF0018 CUBE with LCF classification -LULUCF classes:
# MAGIC val LCF0018_Lulucf_sq1 = spark.sql(""" 
# MAGIC   select 
# MAGIC       t1.GridNum,
# MAGIC       t1.GridNum10km,
# MAGIC       t1.clc0018 as clc0018_change_code,
# MAGIC       t1.clc00 as clc00,
# MAGIC       t1.clc18 as clc18,
# MAGIC       t1.CLC0018 as lcf_clc0018,
# MAGIC       t3_2000.LULUCF_CODE as LULUCF_CODE_00,   
# MAGIC       t3_2000.LULUCF_DESCRIPTION as LULUCF_DESCRIPTION_00,  
# MAGIC       
# MAGIC       t3_2018.LULUCF_CODE as LULUCF_CODE_18,   
# MAGIC       t3_2018.LULUCF_DESCRIPTION as LULUCF_DESCRIPTION_18 ,
# MAGIC       if(t3_2000.LULUCF_CODE<> t3_2018.LULUCF_CODE , CONCAT ( t3_2000.LULUCF_CODE, t3_2018.LULUCF_CODE), '' ) aS  lulucrf_change0018,
# MAGIC       --t2.ection,
# MAGIC       --t2.C,
# MAGIC       ---t2.LD_Biodiversity,
# MAGIC     --  t2.code_level1,
# MAGIC      -- t2.name_level1,
# MAGIC       --t2.code_name_level1,
# MAGIC       --t2.code_level2,
# MAGIC       --t2.name_level2,
# MAGIC       --t2.code_name_level2,
# MAGIC         t1.areaHa
# MAGIC FROM CLC_accounting t1
# MAGIC    LEFT JOIN   lcf_carbon_bio  t2 ON t1.clc0018 = t2.CLC0018 
# MAGIC
# MAGIC    LEFT JOIN   LUT_clc_classes t3_2000  ON t1.clc00 = t3_2000.LEVEL3_CODE  
# MAGIC    LEFT JOIN   LUT_clc_classes t3_2018  ON t1.clc18 = t3_2018.LEVEL3_CODE  
# MAGIC
# MAGIC ----where   t2.CLC0018 is not null """)
# MAGIC                          
# MAGIC LCF0018_Lulucf_sq1.createOrReplaceTempView("LCF0018_lulucf")    
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
# MAGIC
# MAGIC
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

# COMMAND ----------

# MAGIC %scala
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
# MAGIC /// producing a LCF0018 CUBE with LCF classification -LULUCF classes:
# MAGIC val LCF0018_Lulucf_sq1 = spark.sql(""" 
# MAGIC   select 
# MAGIC       t1.GridNum,
# MAGIC       t1.GridNum10km,
# MAGIC       t1.clc0018 as clc0018_change_code,
# MAGIC     
# MAGIC       t1.CLC0018 as lcf_clc0018,
# MAGIC       t3_2000.LULUCF_CODE as LULUCF_CODE_00,   
# MAGIC       t3_2000.LULUCF_DESCRIPTION as LULUCF_DESCRIPTION_00,  
# MAGIC       t3_2018.LULUCF_CODE as LULUCF_CODE_18,   
# MAGIC       t3_2018.LULUCF_DESCRIPTION as LULUCF_DESCRIPTION_18 ,
# MAGIC       if(t3_2000.LULUCF_CODE<> t3_2018.LULUCF_CODE , CONCAT ( t3_2000.LULUCF_CODE, t3_2018.LULUCF_CODE), '' ) aS  lulucrf_change0018,
# MAGIC       --t2.ection,
# MAGIC       --t2.C,
# MAGIC       t2.LD_Biodiversity,
# MAGIC     --  t2.code_level1,
# MAGIC      -- t2.name_level1,
# MAGIC       --t2.code_name_level1,
# MAGIC       --t2.code_level2,
# MAGIC       --t2.name_level2,
# MAGIC       --t2.code_name_level2,
# MAGIC         t1.areaHa
# MAGIC FROM CLC_accounting t1
# MAGIC    LEFT JOIN   lcf_carbon_bio  t2 ON t1.clc0018 = t2.CLC0018 
# MAGIC
# MAGIC    LEFT JOIN   LUT_clc_classes t3_2000  ON t1.clc00 = t3_2000.LEVEL3_CODE  
# MAGIC    LEFT JOIN   LUT_clc_classes t3_2018  ON t1.clc18 = t3_2018.LEVEL3_CODE  
# MAGIC
# MAGIC where   t2.CLC0018 is not null """)
# MAGIC                          
# MAGIC LCF0018_Lulucf_sq1.createOrReplaceTempView("LCF0018_lulucf_bio")    
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from LCF0018_lulucf
# MAGIC
# MAGIC ----where lcf_clc0018 is not null
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2) Building CUBES

# COMMAND ----------

# MAGIC %md ### (2.1) NEW SOC2018 dataset - if neede .. combination of 0-30 & 0-100

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC /// producing CUBe
# MAGIC /// 311 Broad-leaved forest
# MAGIC /// 312 Coniferous forest
# MAGIC /// 313 Mixed forest
# MAGIC val SOC_2018 = spark.sql(""" 
# MAGIC SELECT 
# MAGIC  LCF0018_lulucf.GridNum 
# MAGIC ,LCF0018_lulucf.clc18	
# MAGIC ,isric_30.ocs030cm100m as SOC_0to30cm
# MAGIC ,isric_100.carbonStocks_0_100cm_100m as SOC_0to100cm
# MAGIC ,IF(LCF0018_lulucf.clc18 in (311,312,313),isric_100.carbonStocks_0_100cm_100m ,IF(isric_30.ocs030cm100m is NULL,0,isric_30.ocs030cm100m ) ) as SOC_2018
# MAGIC
# MAGIC from LCF0018_lulucf
# MAGIC LEFT JOIN isric_30           on LCF0018_lulucf.GridNum = isric_30.GridNum
# MAGIC LEFT JOIN isric_100          on LCF0018_lulucf.GridNum = isric_100.GridNum
# MAGIC where LCF0018_lulucf.clc18	 is not null
# MAGIC """)
# MAGIC                          
# MAGIC SOC_2018.createOrReplaceTempView("SOC_2018")   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SOC_2018
# MAGIC

# COMMAND ----------

# MAGIC %md ### (2.2) CUBE LULUCF 2000 - 2018 with NUTS3 - ENV-ZONES - ISRIC 30 -- ISRIC 100

# COMMAND ----------

# MAGIC %sql
# MAGIC --- TESTING OLD OLD OLD 
# MAGIC SELECT 
# MAGIC  nuts3_2021.GridNum 
# MAGIC ,nuts3_2021.GridNum10km	
# MAGIC ,nuts3_2021.Category	 as admin_category
# MAGIC ,nuts3_2021.GridNum10km	
# MAGIC ,nuts3_2021.GridNum1km	
# MAGIC ,nuts3_2021.ADM_ID	
# MAGIC ,nuts3_2021.ADM_COUNTRY	
# MAGIC ,nuts3_2021.ISO2	
# MAGIC ,nuts3_2021.LEVEL3_name	
# MAGIC ,nuts3_2021.LEVEL2_name	
# MAGIC ,nuts3_2021.LEVEL1_name	
# MAGIC ,nuts3_2021.LEVEL0_name	
# MAGIC ,nuts3_2021.LEVEL3_code	
# MAGIC ,nuts3_2021.LEVEL2_code	
# MAGIC ,nuts3_2021.LEVEL1_code	
# MAGIC ,nuts3_2021.LEVEL0_code	
# MAGIC ,nuts3_2021.EEA38_2020	
# MAGIC ,nuts3_2021.EEA39	
# MAGIC ,nuts3_2021.EU27_2020	
# MAGIC ,nuts3_2021.EU28	
# MAGIC ,nuts3_2021.NUTS_EU	
# MAGIC ,nuts3_2021.TAA
# MAGIC
# MAGIC ,nuts3_2021.AreaHa	
# MAGIC
# MAGIC ,LCF0018_lulucf.clc0018_change_code	
# MAGIC ,LCF0018_lulucf.clc00
# MAGIC ,LCF0018_lulucf.clc18	
# MAGIC
# MAGIC ,LCF0018_lulucf.LULUCF_CODE_00	
# MAGIC ,LCF0018_lulucf.LULUCF_DESCRIPTION_00
# MAGIC ,LCF0018_lulucf.LULUCF_CODE_18	
# MAGIC ,LCF0018_lulucf.LULUCF_DESCRIPTION_18	
# MAGIC ,LCF0018_lulucf.lulucrf_change0018	
# MAGIC ,env_zones.Category	asenv_zone_code
# MAGIC
# MAGIC ,isric_30.ocs030cm100m as SOC_0to30cm
# MAGIC ,isric_100.carbonStocks_0_100cm_100m as SOC_0to100cm
# MAGIC
# MAGIC ,IF(clc18 in (111,112,121,122,123,124,132,133,131),0 ,isric_30.ocs030cm100m  ) as SOC_0to30cm_urban_masked2018
# MAGIC ,IF(clc18 in (111,112,121,122,123,124,132,133,131),0 ,isric_100.carbonStocks_0_100cm_100m  ) as SOC_0to100cm_urban_masked2018
# MAGIC ---141,142
# MAGIC
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN LCF0018_lulucf     on nuts3_2021.GridNum = LCF0018_lulucf.GridNum
# MAGIC LEFT JOIN env_zones          on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN isric_30           on nuts3_2021.GridNum = isric_30.GridNum
# MAGIC LEFT JOIN isric_100          on nuts3_2021.GridNum = isric_100.GridNum
# MAGIC
# MAGIC where  nuts3_2021.GridNum =18587349602598912  ---nuts3_2021.ISO2	 ='LU'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC CALC 01:
# MAGIC The following box calc. the CUBE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from isric_30

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC  nuts3_2021.GridNum 
# MAGIC ,nuts3_2021.GridNum10km	
# MAGIC ,nuts3_2021.Category	 as admin_category
# MAGIC ,nuts3_2021.GridNum10km	
# MAGIC ,nuts3_2021.GridNum1km	
# MAGIC ,nuts3_2021.ADM_ID	
# MAGIC ,nuts3_2021.ADM_COUNTRY	
# MAGIC ,nuts3_2021.ISO2	
# MAGIC ,nuts3_2021.LEVEL3_name	
# MAGIC ,nuts3_2021.LEVEL2_name	
# MAGIC ,nuts3_2021.LEVEL1_name	
# MAGIC ,nuts3_2021.LEVEL0_name	
# MAGIC ,nuts3_2021.LEVEL3_code	
# MAGIC ,nuts3_2021.LEVEL2_code	
# MAGIC ,nuts3_2021.LEVEL1_code	
# MAGIC ,nuts3_2021.LEVEL0_code	
# MAGIC ,nuts3_2021.EEA38_2020	
# MAGIC ,nuts3_2021.EEA39	
# MAGIC ,nuts3_2021.EU27_2020	
# MAGIC ,nuts3_2021.EU28	
# MAGIC ,nuts3_2021.NUTS_EU	
# MAGIC ,nuts3_2021.TAA
# MAGIC
# MAGIC ,nuts3_2021.AreaHa	
# MAGIC
# MAGIC ,LCF0018_lulucf.clc0018_change_code	
# MAGIC ,LCF0018_lulucf.clc00
# MAGIC ,LCF0018_lulucf.clc18	
# MAGIC
# MAGIC ,LCF0018_lulucf.LULUCF_CODE_00	
# MAGIC ,LCF0018_lulucf.LULUCF_DESCRIPTION_00
# MAGIC ,LCF0018_lulucf.LULUCF_CODE_18	
# MAGIC ,LCF0018_lulucf.LULUCF_DESCRIPTION_18	
# MAGIC ,LCF0018_lulucf.lulucrf_change0018	
# MAGIC ,env_zones.Category	as env_zone_code
# MAGIC ,isric_30.ocs030cm100m as SOC2018
# MAGIC ---,SOC_2018.SOC_2018 as SOC_2018
# MAGIC ,IF(LCF0018_lulucf.clc18 in (111,112,121,122,123,124,132,133,131),0 ,isric_30.ocs030cm100m  ) as SOC2018_urban_masked2018
# MAGIC ---141,142 ---without urban green
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN LCF0018_lulucf     on nuts3_2021.GridNum = LCF0018_lulucf.GridNum
# MAGIC LEFT JOIN env_zones          on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN isric_30            on nuts3_2021.GridNum = isric_30.gridnum
# MAGIC where nuts3_2021.ISO2	 is not null
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC /// producing CUBe with SOC 0-30 for ALL LULUCF classes!!!
# MAGIC
# MAGIC val SOC_Landdegradation_0018 = spark.sql(""" 
# MAGIC SELECT 
# MAGIC  nuts3_2021.GridNum 
# MAGIC ,nuts3_2021.GridNum10km	
# MAGIC ,nuts3_2021.Category	 as admin_category
# MAGIC ,nuts3_2021.GridNum10km	
# MAGIC ,nuts3_2021.GridNum1km	
# MAGIC ,nuts3_2021.ADM_ID	
# MAGIC ,nuts3_2021.ADM_COUNTRY	
# MAGIC ,nuts3_2021.ISO2	
# MAGIC ,nuts3_2021.LEVEL3_name	
# MAGIC ,nuts3_2021.LEVEL2_name	
# MAGIC ,nuts3_2021.LEVEL1_name	
# MAGIC ,nuts3_2021.LEVEL0_name	
# MAGIC ,nuts3_2021.LEVEL3_code	
# MAGIC ,nuts3_2021.LEVEL2_code	
# MAGIC ,nuts3_2021.LEVEL1_code	
# MAGIC ,nuts3_2021.LEVEL0_code	
# MAGIC ,nuts3_2021.EEA38_2020	
# MAGIC ,nuts3_2021.EEA39	
# MAGIC ,nuts3_2021.EU27_2020	
# MAGIC ,nuts3_2021.EU28	
# MAGIC ,nuts3_2021.NUTS_EU	
# MAGIC ,nuts3_2021.TAA
# MAGIC
# MAGIC ,nuts3_2021.AreaHa	
# MAGIC
# MAGIC ,LCF0018_lulucf.clc0018_change_code	
# MAGIC ,LCF0018_lulucf.clc00
# MAGIC ,LCF0018_lulucf.clc18	
# MAGIC
# MAGIC ,LCF0018_lulucf.LULUCF_CODE_00	
# MAGIC ,LCF0018_lulucf.LULUCF_DESCRIPTION_00
# MAGIC ,LCF0018_lulucf.LULUCF_CODE_18	
# MAGIC ,LCF0018_lulucf.LULUCF_DESCRIPTION_18	
# MAGIC ,LCF0018_lulucf.lulucrf_change0018	
# MAGIC ,env_zones.Category	as env_zone_code
# MAGIC ,isric_30.ocs030cm100m as SOC2018
# MAGIC ---,SOC_2018.SOC_2018 as SOC_2018
# MAGIC ,IF(LCF0018_lulucf.clc18 in (111,112,121,122,123,124,132,133,131),0 ,isric_30.ocs030cm100m  ) as SOC2018_urban_masked2018
# MAGIC ---141,142 ---without urban green
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN LCF0018_lulucf     on nuts3_2021.GridNum = LCF0018_lulucf.GridNum
# MAGIC LEFT JOIN env_zones          on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN isric_30            on nuts3_2021.GridNum = isric_30.gridnum
# MAGIC where nuts3_2021.ISO2	 is not null
# MAGIC """)
# MAGIC                          
# MAGIC SOC_Landdegradation_0018.createOrReplaceTempView("SOC_Landdegradation_0018")    
# MAGIC
# MAGIC
# MAGIC
# MAGIC val SOC_Landdegradation_0018_nuts3 = spark.sql(""" 
# MAGIC SELECT
# MAGIC     LEVEL3_code,
# MAGIC     TAA,
# MAGIC     LULUCF_CODE_00,
# MAGIC     LULUCF_CODE_18,
# MAGIC     lulucrf_change0018,
# MAGIC     env_zone_code,
# MAGIC     SUM(SOC2018_urban_masked2018) as SOC2018_urban_masked2018,
# MAGIC     SUM(SOC2018) as SOC2018,
# MAGIC     
# MAGIC     AVG(SOC2018_urban_masked2018) as SOC2018_urban_masked2018_AVG,
# MAGIC     AVG(SOC2018) as SOC2018_AVG,
# MAGIC
# MAGIC     SUM(AreaHa)as AreaHa
# MAGIC
# MAGIC     FROM
# MAGIC         SOC_Landdegradation_0018
# MAGIC     
# MAGIC     
# MAGIC     group by
# MAGIC     LEVEL3_code,
# MAGIC     TAA,
# MAGIC     LULUCF_CODE_00,
# MAGIC     LULUCF_CODE_18,
# MAGIC     lulucrf_change0018,
# MAGIC       env_zone_code
# MAGIC
# MAGIC """)
# MAGIC SOC_Landdegradation_0018_nuts3.createOrReplaceTempView("SOC_Landdegradation_0018_nuts3")    
# MAGIC
# MAGIC ///// JOIN AVG SOC data to lulucf classes 2000 and 20178:
# MAGIC
# MAGIC val SOC_Landdegradation_0018_nuts3_v1 = spark.sql(""" 
# MAGIC
# MAGIC SELECT 
# MAGIC t1.LEVEL3_code,
# MAGIC t1.TAA,
# MAGIC t1.LULUCF_CODE_00,
# MAGIC t1.LULUCF_CODE_18,
# MAGIC t1.lulucrf_change0018,
# MAGIC t1.env_zone_code,
# MAGIC
# MAGIC t1.SOC2018_urban_masked2018,
# MAGIC t1.SOC2018,
# MAGIC t1.SOC2018_urban_masked2018_AVG,
# MAGIC t1.SOC2018_AVG,
# MAGIC
# MAGIC t1.AreaHa,
# MAGIC
# MAGIC t2.LULUCF_CODE_0018 as LULUCF_CODE_0018_18,
# MAGIC t2.SOC_2018_AVG_not_disturbed_pixel as SOC_2018_AVG_not_disturbed_pixel_on_2018 ,
# MAGIC t3.LULUCF_CODE_0018  as LULUCF_CODE_0018_00,
# MAGIC t3.SOC_2018_AVG_not_disturbed_pixel as SOC_2018_AVG_not_disturbed_pixel_on_2000
# MAGIC
# MAGIC
# MAGIC  from SOC_Landdegradation_0018_nuts3 t1
# MAGIC
# MAGIC  LEFT JOIN SOC_AVG_on_non_changes  t2 on 
# MAGIC       t2.LEVEL3_code =    t1.LEVEL3_code AND
# MAGIC       t2.TAA =            t1.TAA AND
# MAGIC       t2.env_zone_code =  t1.env_zone_code AND
# MAGIC       t2.LULUCF_CODE_0018 = t1.LULUCF_CODE_18 
# MAGIC
# MAGIC LEFT JOIN SOC_AVG_on_non_changes  t3 on 
# MAGIC       t3.LEVEL3_code =    t1.LEVEL3_code AND
# MAGIC       t3.TAA =            t1.TAA AND
# MAGIC       t3.env_zone_code =  t1.env_zone_code AND
# MAGIC       t3.LULUCF_CODE_0018 = t1.LULUCF_CODE_00
# MAGIC
# MAGIC """)
# MAGIC
# MAGIC
# MAGIC SOC_Landdegradation_0018_nuts3_v1
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC_landdegradation0018_nuts3")
# MAGIC
# MAGIC SOC_Landdegradation_0018_nuts3_v1.createOrReplaceTempView("SOC_Landdegradation_0018_nuts3_v2")   

# COMMAND ----------

# MAGIC %scala
# MAGIC ///OLD OLD OLD
# MAGIC
# MAGIC /// producing CUBe
# MAGIC ///al SOC_Landdegradation_0018 = spark.sql(""" 
# MAGIC ///ELECT 
# MAGIC ///nuts3_2021.GridNum 
# MAGIC ///nuts3_2021.GridNum10km	
# MAGIC ///nuts3_2021.Category	 as admin_category
# MAGIC ///nuts3_2021.GridNum10km	
# MAGIC ///nuts3_2021.GridNum1km	
# MAGIC ///nuts3_2021.ADM_ID	
# MAGIC ///nuts3_2021.ADM_COUNTRY	
# MAGIC ///nuts3_2021.ISO2	
# MAGIC ///nuts3_2021.LEVEL3_name	
# MAGIC ///nuts3_2021.LEVEL2_name	
# MAGIC ///nuts3_2021.LEVEL1_name	
# MAGIC ///nuts3_2021.LEVEL0_name	
# MAGIC ///nuts3_2021.LEVEL3_code	
# MAGIC ///nuts3_2021.LEVEL2_code	
# MAGIC ///nuts3_2021.LEVEL1_code	
# MAGIC ///nuts3_2021.LEVEL0_code	
# MAGIC ///nuts3_2021.EEA38_2020	
# MAGIC ///nuts3_2021.EEA39	
# MAGIC ///nuts3_2021.EU27_2020	
# MAGIC ///nuts3_2021.EU28	
# MAGIC ///nuts3_2021.NUTS_EU	
# MAGIC ///nuts3_2021.TAA
# MAGIC ///
# MAGIC ///nuts3_2021.AreaHa	
# MAGIC ///
# MAGIC ///LCF0018_lulucf.clc0018_change_code	
# MAGIC ///LCF0018_lulucf.clc00
# MAGIC ///LCF0018_lulucf.clc18	
# MAGIC ///
# MAGIC ///LCF0018_lulucf.LULUCF_CODE_00	
# MAGIC ///LCF0018_lulucf.LULUCF_DESCRIPTION_00
# MAGIC ///LCF0018_lulucf.LULUCF_CODE_18	
# MAGIC ///LCF0018_lulucf.LULUCF_DESCRIPTION_18	
# MAGIC ///LCF0018_lulucf.lulucrf_change0018	
# MAGIC ///env_zones.Category	as env_zone_code
# MAGIC ///
# MAGIC ///isric_30.ocs030cm100m as SOC_0to30cm
# MAGIC ///isric_100.carbonStocks_0_100cm_100m as SOC_0to100cm
# MAGIC ///
# MAGIC ///IF(clc18 in (111,112,121,122,123,124,132,133,131),0 ,isric_30.ocs030cm100m  ) as SOC_0to30cm_urban_masked2018
# MAGIC ///IF(clc18 in (111,112,121,122,123,124,132,133,131),0 ,isric_100.carbonStocks_0_100cm_100m  ) as ///OC_0to100cm_urban_masked2018
# MAGIC ///--141,142 ---without urban green
# MAGIC ///
# MAGIC ///
# MAGIC ///rom nuts3_2021
# MAGIC ///
# MAGIC ///EFT JOIN LCF0018_lulucf     on nuts3_2021.GridNum = LCF0018_lulucf.GridNum
# MAGIC ///EFT JOIN env_zones          on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC ///EFT JOIN isric_30           on nuts3_2021.GridNum = isric_30.GridNum
# MAGIC ///EFT JOIN isric_100          on nuts3_2021.GridNum = isric_100.GridNum
# MAGIC ///here nuts3_2021.ISO2	 is not null
# MAGIC ///"")
# MAGIC ///                        
# MAGIC ///OC_Landdegradation_0018.createOrReplaceTempView("SOC_Landdegradation_0018")    
# MAGIC ///
# MAGIC ///
# MAGIC ///al SOC_Landdegradation_0018_nuts3 = spark.sql(""" 
# MAGIC ///ELECT
# MAGIC ///   LEVEL3_code,
# MAGIC ///   TAA,
# MAGIC ///   LULUCF_CODE_00,
# MAGIC ///   LULUCF_CODE_18,
# MAGIC ///   lulucrf_change0018,
# MAGIC ///   env_zone_code,
# MAGIC ///   SUM(SOC_0to30cm_urban_masked2018) as SOC_0to30cm_urban_masked2018,
# MAGIC ///   SUM(SOC_0to100cm_urban_masked2018) as SOC_0to100cm_urban_masked2018,
# MAGIC ///
# MAGIC ///   SUM(SOC_0to30cm) as SOC_0to30cm,
# MAGIC ///   SUM(SOC_0to100cm) as SOC_0to100cm,
# MAGIC ///
# MAGIC ///   avg(SOC_0to30cm_urban_masked2018) as SOC_0to30cm_urban_masked2018_AVG_ha_value,
# MAGIC ///   avg(SOC_0to100cm_urban_masked2018) as SOC_0to100cm_urban_masked2018_AVG_ha_value,
# MAGIC ///
# MAGIC ///   avg(SOC_0to30cm) as SOC_0to30cm_AVG_ha_value,
# MAGIC ///   avg(SOC_0to100cm) as SOC_0to100cm_AVG_ha_value,
# MAGIC ///
# MAGIC ///   SUM(AreaHa)as AreaHa
# MAGIC ///
# MAGIC ///   FROM
# MAGIC ///       SOC_Landdegradation_0018
# MAGIC ///   
# MAGIC ///   
# MAGIC ///   group by
# MAGIC ///   LEVEL3_code,
# MAGIC ///   TAA,
# MAGIC ///   LULUCF_CODE_00,
# MAGIC ///   LULUCF_CODE_18,
# MAGIC ///   lulucrf_change0018,
# MAGIC ///     env_zone_code
# MAGIC ///
# MAGIC ///"")
# MAGIC ///                       
# MAGIC ///OC_Landdegradation_0018_nuts3
# MAGIC ///   .coalesce(1) //be careful with this
# MAGIC ///   .write.format("com.databricks.spark.csv")
# MAGIC ///   .mode(SaveMode.Overwrite)
# MAGIC ///   .option("sep","|")
# MAGIC ///   .option("overwriteSchema", "true")
# MAGIC ///   .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC ///   .option("emptyValue", "")
# MAGIC ///   .option("header","true")
# MAGIC ///   .option("treatEmptyValuesAsNulls", "true")  
# MAGIC ///   
# MAGIC ///   .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC_landdegradation0018_nuts3")
# MAGIC ///
# MAGIC ///
# MAGIC ///  SOC_Landdegradation_0018_nuts3.createOrReplaceTempView("SOC_Landdegradation_0018_nuts3")    
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC_landdegradation0018_nuts3"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The following box calc. the AVG & SUM of SOC values per reference unit under non disturbed soils (no change 2000-2018)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC /// producing reference SOC values (avg) under non disturbed pixels: (-this new lut should be JOINED to the changes!)
# MAGIC val SOC_AVG_on_non_changes = spark.sql(""" 
# MAGIC   SELECT
# MAGIC   LEVEL3_code,
# MAGIC   TAA,
# MAGIC   --LULUCF_CODE_00,
# MAGIC   LULUCF_CODE_18 as LULUCF_CODE_0018 ,
# MAGIC  --- lulucrf_change0018,
# MAGIC   env_zone_code,
# MAGIC   AVG(SOC2018) as SOC_2018_AVG_not_disturbed_pixel
# MAGIC  
# MAGIC   FROM
# MAGIC   SOC_Landdegradation_0018
# MAGIC   where  LULUCF_CODE_00 = LULUCF_CODE_18
# MAGIC   group by
# MAGIC   LEVEL3_code,
# MAGIC   TAA,
# MAGIC   LULUCF_CODE_00,
# MAGIC   LULUCF_CODE_18,
# MAGIC   ---lulucrf_change0018,
# MAGIC   env_zone_code
# MAGIC   """)
# MAGIC SOC_AVG_on_non_changes.createOrReplaceTempView("SOC_AVG_on_non_changes")   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC t1.LEVEL3_code,
# MAGIC t1.TAA,
# MAGIC t1.LULUCF_CODE_00,
# MAGIC t1.LULUCF_CODE_18,
# MAGIC t1.lulucrf_change0018,
# MAGIC t1.env_zone_code,
# MAGIC
# MAGIC t1.SOC2018_urban_masked2018,
# MAGIC t1.SOC2018,
# MAGIC t1.SOC2018_urban_masked2018_AVG,
# MAGIC t1.SOC2018_AVG,
# MAGIC
# MAGIC t1.AreaHa,
# MAGIC
# MAGIC t2.LULUCF_CODE_0018 as LULUCF_CODE_0018_18,
# MAGIC t2.SOC_2018_AVG_not_disturbed_pixel as SOC_2018_AVG_not_disturbed_pixel_on_2018 ,
# MAGIC t3.LULUCF_CODE_0018  as LULUCF_CODE_0018_00,
# MAGIC t3.SOC_2018_AVG_not_disturbed_pixel as SOC_2018_AVG_not_disturbed_pixel_on_2000
# MAGIC
# MAGIC
# MAGIC  from SOC_Landdegradation_0018_nuts3 t1
# MAGIC
# MAGIC  LEFT JOIN SOC_AVG_on_non_changes  t2 on 
# MAGIC       t2.LEVEL3_code =    t1.LEVEL3_code AND
# MAGIC       t2.TAA =            t1.TAA AND
# MAGIC       t2.env_zone_code =  t1.env_zone_code AND
# MAGIC       t2.LULUCF_CODE_0018 = t1.LULUCF_CODE_18 
# MAGIC
# MAGIC LEFT JOIN SOC_AVG_on_non_changes  t3 on 
# MAGIC       t3.LEVEL3_code =    t1.LEVEL3_code AND
# MAGIC       t3.TAA =            t1.TAA AND
# MAGIC       t3.env_zone_code =  t1.env_zone_code AND
# MAGIC       t3.LULUCF_CODE_0018 = t1.LULUCF_CODE_00
# MAGIC
# MAGIC

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

# COMMAND ----------

# MAGIC %md ###  Get resulting URL for download:

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB/AGB_STOCK1_ESA_CCI2018"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)
    

# COMMAND ----------

# MAGIC %md ###  union three tables

# COMMAND ----------

# MAGIC %sql
# MAGIC /****** Script for SelectTopNRows command from SSMS  ******/
# MAGIC
# MAGIC drop table if exists  [Carbon_Mapping].[carbon_mapping].[soc_stock_A_30cm]
# MAGIC go
# MAGIC SELECT [Category]
# MAGIC       ,[GridNum10km]
# MAGIC       ,[ADM_ID]
# MAGIC       ,[ADM_COUNTRY]
# MAGIC       ,[ISO2]
# MAGIC       ,[LEVEL3_name]
# MAGIC       ,[LEVEL2_name]
# MAGIC       ,[LEVEL1_name]
# MAGIC       ,[LEVEL0_name]
# MAGIC       ,[LEVEL3_code]
# MAGIC       ,[LEVEL2_code]
# MAGIC       ,[LEVEL1_code]
# MAGIC       ,[LEVEL0_code]
# MAGIC       ,[NUTS_EU]
# MAGIC       ,[TAA]
# MAGIC       ,[LULUCF_CODE]
# MAGIC       ,[LULUCF_DESCRIPTION]
# MAGIC       ,[soil_type]
# MAGIC       ,[env_zones]
# MAGIC
# MAGIC 	  ,[AreaHa]
# MAGIC       ,[SOC_STOCK_isric30cm_t]
# MAGIC 	  ,NULL as [SOC_STOCK_isric100cm_t]
# MAGIC 		,NULL as	[clc18_level3_class]
# MAGIC 		,NULL as 	[SOC_STOCK_t_ext_wetland]
# MAGIC 		,NULL as	[SOC_STOCK_t_wetland]
# MAGIC
# MAGIC into [Carbon_Mapping].[carbon_mapping].[soc_stock_A_30cm]
# MAGIC   FROM [Carbon_Mapping].[carbon_mapping].[soc_stock_A_isric30cm]
# MAGIC   go
# MAGIC
# MAGIC   
# MAGIC drop table if exists  [Carbon_Mapping].[carbon_mapping].[soc_stock_B_100cm]
# MAGIC go
# MAGIC
# MAGIC   Select 
# MAGIC
# MAGIC    [Category]
# MAGIC       ,[GridNum10km]
# MAGIC       ,[ADM_ID]
# MAGIC       ,[ADM_COUNTRY]
# MAGIC       ,[ISO2]
# MAGIC       ,[LEVEL3_name]
# MAGIC       ,[LEVEL2_name]
# MAGIC       ,[LEVEL1_name]
# MAGIC       ,[LEVEL0_name]
# MAGIC       ,[LEVEL3_code]
# MAGIC       ,[LEVEL2_code]
# MAGIC       ,[LEVEL1_code]
# MAGIC       ,[LEVEL0_code]
# MAGIC       ,[NUTS_EU]
# MAGIC       ,[TAA]
# MAGIC       ,[LULUCF_CODE]
# MAGIC       ,[LULUCF_DESCRIPTION]
# MAGIC       ,[soil_type]
# MAGIC       ,[env_zones]
# MAGIC
# MAGIC 	  ,[AreaHa]
# MAGIC       ,NULL as  [SOC_STOCK_isric30cm_t]
# MAGIC 	  ,[SOC_STOCK_isric100cm_t]
# MAGIC 		,NULL as	[clc18_level3_class]
# MAGIC 		,NULL as 	[SOC_STOCK_t_ext_wetland]
# MAGIC 		,NULL as	[SOC_STOCK_t_wetland]
# MAGIC 		into [Carbon_Mapping].[carbon_mapping].[soc_stock_B_100cm]
# MAGIC 		from  [carbon_mapping].[soc_stock_B_isric100cm_forest]
# MAGIC   go
# MAGIC
# MAGIC   
# MAGIC drop table if exists  [Carbon_Mapping].[carbon_mapping].[soc_stock_C_wetland]
# MAGIC go
# MAGIC   Select 
# MAGIC
# MAGIC    [Category]
# MAGIC       ,[GridNum10km]
# MAGIC       ,[ADM_ID]
# MAGIC       ,[ADM_COUNTRY]
# MAGIC       ,[ISO2]
# MAGIC       ,[LEVEL3_name]
# MAGIC       ,[LEVEL2_name]
# MAGIC       ,[LEVEL1_name]
# MAGIC       ,[LEVEL0_name]
# MAGIC       ,[LEVEL3_code]
# MAGIC       ,[LEVEL2_code]
# MAGIC       ,[LEVEL1_code]
# MAGIC       ,[LEVEL0_code]
# MAGIC       ,[NUTS_EU]
# MAGIC       ,[TAA]
# MAGIC       ,[LULUCF_CODE]
# MAGIC       ,[LULUCF_DESCRIPTION]
# MAGIC       ,[soil_type]
# MAGIC       ,[env_zones]
# MAGIC
# MAGIC 	  ,[AreaHa]
# MAGIC       ,NULL as  [SOC_STOCK_isric30cm_t]
# MAGIC 	  ,NULL as[SOC_STOCK_isric100cm_t]
# MAGIC 		,[clc18_level3_class]
# MAGIC 		,	[SOC_STOCK_t_ext_wetland]
# MAGIC 		,[SOC_STOCK_t_wetland]
# MAGIC
# MAGIC 		into [Carbon_Mapping].[carbon_mapping].[soc_stock_C_wetland]
# MAGIC 		from  [carbon_mapping].[soc_stock_C_wetland_tab]
# MAGIC
# MAGIC go
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM [Carbon_Mapping].[carbon_mapping].[soc_stock_A_30cm]
# MAGIC UNION ALL
# MAGIC SELECT * FROM [Carbon_Mapping].[carbon_mapping].[soc_stock_B_100cm]
# MAGIC UNION ALL
# MAGIC SELECT * FROM [Carbon_Mapping].[carbon_mapping].[soc_stock_C_wetland]

# COMMAND ----------

# MAGIC %md ###  show columns in table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS IN AGB_STOCK2_CL_GDMP_1km_2018;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ###  Using over (partition by ....)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   nuts3_2021.GridNum,
# MAGIC   nuts3_2021.Category,
# MAGIC   nuts3_2021.GridNum10km,
# MAGIC   nuts3_2021.GridNum1km,
# MAGIC   GDMP_2018,
# MAGIC   nuts3_2021.AreaHa,
# MAGIC
# MAGIC   count(nuts3_2021.GridNum) over (partition by nuts3_2021.GridNum1km) as Total_1km_grid_cells,
# MAGIC   count(nuts3_2021.GridNum) over (partition by nuts3_2021.GridNum10km) as Total_sum_10km,
# MAGIC
# MAGIC   GDMP_2018/100 as   GDMP_2018_updated_for_100m
# MAGIC
# MAGIC
# MAGIC  from nuts3_2021
# MAGIC
# MAGIC  LEFT JOIN GDMP_1km_99_19     on nuts3_2021.GridNum1km = GDMP_1km_99_19.GridNum ------ 1km JOIN !!!!!!
# MAGIC where  nuts3_2021.GridNum10km=9419829647769600  
