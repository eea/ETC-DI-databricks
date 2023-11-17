-- Databricks notebook source
-- MAGIC %md
-- MAGIC # IPCC Tier 2 method LB (Living Biomass) increase simulation for afferestation on cropland and grassland
-- MAGIC
-- MAGIC
-- MAGIC In this example notebook the impact on the LB for a certain prototype configuration will be calculated and visualized. This notebook is developed in such a way, that that user can play around with different configurations and analyze the outcome.
-- MAGIC
-- MAGIC The IPCC layers and and needed LUT tables will be used in the first place to enable this simulation
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The corresponding paramters are derived from the JRC yield table. A LUT has been generated that allows direct import of the LB increase for the first 20 years and for the age between 21-30 years.
-- MAGIC The LB increment can be different for the same tree species, dependent on the forest zone in which the NUTS3-region is located.
-- MAGIC
-- MAGIC PART1: CONFIGURE AFFORESTATION SCENARIO
-- MAGIC Options for configuration of afforestation scenario
-- MAGIC Define the configuration for the flexible parameters that will determine the afforestation scenario
-- MAGIC
-- MAGIC Slope
-- MAGIC Threshold on slope for afforestation. Slopes above the defined threshold will be used for afforesation. Options between 0-87.5 (%)
-- MAGIC
-- MAGIC RCP
-- MAGIC The emission scenario of the EUTrees4F that will be used for the future occurrence probability of a certain tree species. Default take the 'rcp45' scenario
-- MAGIC
-- MAGIC Tree_prob
-- MAGIC The probability of occurrence that a certain tree species should have in a certain target year before it can be used for afforestation. Options between 0-100 (%)
-- MAGIC
-- MAGIC Tree_species
-- MAGIC The tree species you want to use for the afforestation. Full list please consult JRC LUT
-- MAGIC
-- MAGIC Fagus_sylvatica
-- MAGIC
-- MAGIC Larix_decidua
-- MAGIC
-- MAGIC Picea_abies
-- MAGIC
-- MAGIC Pinus_pinaster
-- MAGIC
-- MAGIC Pinus_sylvestris
-- MAGIC
-- MAGIC Quercus_robur
-- MAGIC
-- MAGIC Perc_reforest
-- MAGIC The percentage of suitable (determined by the afforestation mask) grassland/cropland that should be afforested. Options between 0-100 (%)
-- MAGIC
-- MAGIC Year_potential
-- MAGIC The target year for which the afforestation carbon sequestration potential calculation should be done (2024 -...)
-- MAGIC
-- MAGIC Year_baseline
-- MAGIC The baseline year for which the afforestation carbon sequestration potential calculation should start and thus the year at which the trees will be planted
-- MAGIC
-- MAGIC lst_CLC_afforestation
-- MAGIC Define which CLC classes can be used for afforestation Based on the column 'Afforestation_mask' from CLC_LULUCF table: https://eea1.sharepoint.com/:x:/r/teams/-EXT-CrossETCexchange/_layouts/15/Doc.aspx?sourcedoc=%7B447d1502-9f54-43e2-900a-14a239770769%7D&action=edit&wdLOR=cAE9313DD-6E50-40F6-A328-F1101ABF05EE&activeCell=%27CLC_LULUCF_LUT%27!G30&wdinitialsession=73a03156-95ab-4412-8a03-e3ccd81233d7&wdrldsc=2&wdrldc=1&wdrldr=AccessTokenExpiredWarning%2CRefreshingExpiredAccessT&cid=16243ca6-b7d4-411f-9dd8-924b725e256a
-- MAGIC
-- MAGIC
-- MAGIC The use of NUTS specific factors can be enabled by setting the parameter 'run_NUTS_specific_scenario' on True.
-- MAGIC Please be aware that in that case a LUT should be established which assign a specific configuration to each NUTS region.
-- MAGIC Currently, a randomly defined configuration in the data folder 'NUTS_LUT_afforestation_scenario' will be loaded.
-- MAGIC
-- MAGIC First prototype configuration:
-- MAGIC This is the initial configuration that was used to generate the afforestation maps
-- MAGIC
-- MAGIC 'Slope': 0,
-- MAGIC 'RCP': 'rcp45',
-- MAGIC 'Tree_prob': 70,
-- MAGIC 'Tree_species': 'Betula_pendula',
-- MAGIC 'Perc_reforest': 10,
-- MAGIC 'Year_potential': 2035,
-- MAGIC 'input_source': 'EEA39'}
-- MAGIC
-- MAGIC the input_source parameter just indicates that this is the default configuration at EEA39 scale. If a NUTS specific scenario is enabled this will be set to the NUTS level for which this parameter is defined.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.Building spatial CUBE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.1 Loading DIMS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC //##########################################################################################################################################
-- MAGIC //   THIS BOX reads all Dimensions (DIM) and Lookuptables (LUT) that are needed for the NUTS3 and 10x10km GRID statistics
-- MAGIC //   info: loehnertz@space4environment.com
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC
-- MAGIC //// FIRST start the cluster: ETC-ULS !!!!!!!!!!!!!!!!!!!!!!!!
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC //// (0) ADMIN layer  Nuts2021 ################################################################################
-- MAGIC // Reading the admin DIM:---------------------------------------------
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1517&fileId=542
-- MAGIC val parquetFileDF_D_ADMbndEEA39v2021 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ADMbndEEA39v2021_542_2022613_100m/")             /// use load
-- MAGIC parquetFileDF_D_ADMbndEEA39v2021.createOrReplaceTempView("D_admbndEEA39v2021")
-- MAGIC
-- MAGIC ///// Reading the LUT :---------------------------------------------
-- MAGIC ///https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=65
-- MAGIC
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val schema_nuts2021 = new StructType()
-- MAGIC .add("ADM_ID",LongType,true)
-- MAGIC .add("ISO2",StringType,true)
-- MAGIC .add("ESTAT",StringType,true)
-- MAGIC .add("ADM_COUNTRY",StringType,true)
-- MAGIC
-- MAGIC .add("LEVEL3_name",StringType,true)
-- MAGIC .add("LEVEL2_name",StringType,true)
-- MAGIC .add("LEVEL1_name",StringType,true)
-- MAGIC .add("LEVEL0_name",StringType,true)
-- MAGIC .add("LEVEL3_code",StringType,true)
-- MAGIC .add("LEVEL2_code",StringType,true)
-- MAGIC .add("LEVEL1_code",StringType,true)
-- MAGIC .add("LEVEL0_code",StringType,true)
-- MAGIC
-- MAGIC .add("EEA32_2020",IntegerType,true)
-- MAGIC .add("EEA38_2020",IntegerType,true)
-- MAGIC .add("EEA39",IntegerType,true)
-- MAGIC .add("EEA33",IntegerType,true)
-- MAGIC .add("EEA32_2006",IntegerType,true)
-- MAGIC .add("EU27_2020",IntegerType,true)
-- MAGIC .add("EU28",IntegerType,true)
-- MAGIC .add("EU27_2007",IntegerType,true)
-- MAGIC .add("EU25",IntegerType,true)
-- MAGIC .add("EU15",IntegerType,true)
-- MAGIC .add("EU12",IntegerType,true)
-- MAGIC .add("EU10",IntegerType,true)
-- MAGIC .add("EFTA4",IntegerType,true)
-- MAGIC .add("NUTS_EU",StringType,true)
-- MAGIC .add("TAA",StringType,true)
-- MAGIC
-- MAGIC
-- MAGIC val LUT_nuts2021  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_nuts2021)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/adm_eea39_2021LUT/20200527111402.69.csv")
-- MAGIC LUT_nuts2021.createOrReplaceTempView("LUT_nuts2021")
-- MAGIC
-- MAGIC
-- MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and NUTS information:---------------------------------------------
-- MAGIC
-- MAGIC val nuts3_2021 = spark.sql(""" 
-- MAGIC                SELECT 
-- MAGIC
-- MAGIC D_admbndEEA39v2021.GridNum,
-- MAGIC D_admbndEEA39v2021.Category,
-- MAGIC D_admbndEEA39v2021.AreaHa,
-- MAGIC D_admbndEEA39v2021.GridNum10km,
-- MAGIC D_admbndEEA39v2021.gridnum &  -16777216 as GridNum1km,
-- MAGIC LUT_nuts2021.ADM_ID,
-- MAGIC LUT_nuts2021.ADM_COUNTRY	,
-- MAGIC LUT_nuts2021.ISO2	,
-- MAGIC LUT_nuts2021.LEVEL3_name	,
-- MAGIC LUT_nuts2021.LEVEL2_name	,
-- MAGIC LUT_nuts2021.LEVEL1_name	,
-- MAGIC LUT_nuts2021.LEVEL0_name	,
-- MAGIC LUT_nuts2021.LEVEL3_code	,
-- MAGIC LUT_nuts2021.LEVEL2_code	,
-- MAGIC LUT_nuts2021.LEVEL1_code	,
-- MAGIC LUT_nuts2021.LEVEL0_code	,
-- MAGIC LUT_nuts2021.EEA32_2020	,
-- MAGIC LUT_nuts2021.EEA38_2020,	
-- MAGIC LUT_nuts2021.EEA39	,
-- MAGIC LUT_nuts2021.EEA33	,
-- MAGIC LUT_nuts2021.EEA32_2006,	
-- MAGIC LUT_nuts2021.EU27_2020	,
-- MAGIC LUT_nuts2021.EU28	,
-- MAGIC LUT_nuts2021.EU27_2007,	
-- MAGIC LUT_nuts2021.EU25	,
-- MAGIC LUT_nuts2021.EU15	,
-- MAGIC LUT_nuts2021.EU12	,
-- MAGIC LUT_nuts2021.EU10	,
-- MAGIC LUT_nuts2021.EFTA4	,
-- MAGIC LUT_nuts2021.NUTS_EU,	
-- MAGIC LUT_nuts2021.TAA	
-- MAGIC
-- MAGIC FROM D_admbndEEA39v2021 
-- MAGIC   LEFT JOIN LUT_nuts2021  ON D_admbndEEA39v2021.Category = LUT_nuts2021.ADM_ID 
-- MAGIC   
-- MAGIC        
-- MAGIC  
-- MAGIC                                   """)
-- MAGIC
-- MAGIC nuts3_2021.createOrReplaceTempView("nuts3_2021")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (1) CLC 2018 ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the lULUCF classes:
-- MAGIC // Reading CLC2018 100m DIM:.....
-- MAGIC
-- MAGIC val parquetFileDF_clc18 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_A_CLC_18_210_20181129_100m/")
-- MAGIC parquetFileDF_clc18.createOrReplaceTempView("CLC_2018")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (2) ClULUCF classes ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the lULUCF classes:
-- MAGIC // Reading CLC2018 100m DIM:.....
-- MAGIC
-- MAGIC // Reading the LUT for CLC...:
-- MAGIC val lut_clc  = spark.read.format("csv")
-- MAGIC .options(Map("delimiter"->","))
-- MAGIC  .option("header", "true")
-- MAGIC    .load("dbfs:/mnt/trainingDatabricks/LookupTablesFiles/Corine_Land_Cover_LUT_JEDI_4.csv")     ////------Lookup_CLC_07112022_4.csv   Lookup_CLC_24032021_4.csv
-- MAGIC lut_clc.createOrReplaceTempView("LUT_clc_classes")
-- MAGIC // Construction of a new table: with lULUCF level 1 classes bases on CLC2018 100m:...................
-- MAGIC val lULUCF_sq1 = spark.sql(""" 
-- MAGIC                    SELECT                
-- MAGIC                   CLC_2018.GridNum,
-- MAGIC                   CLC_2018.GridNum10km,                     
-- MAGIC                   ---CONCAT('MAES_',LUT_clc_classes.MAES_CODE) as MAES_CODE ,   
-- MAGIC                   LULUCF_CODE,   
-- MAGIC                   LULUCF_DESCRIPTION,     
-- MAGIC                   CLC_2018.AreaHa
-- MAGIC                   from CLC_2018   
-- MAGIC                   LEFT JOIN   LUT_clc_classes  
-- MAGIC                      ON  CLC_2018.Category  = LUT_clc_classes.LEVEL3_CODE where AreaHa = 1                                 
-- MAGIC                                                         """)                                  
-- MAGIC lULUCF_sq1.createOrReplaceTempView("lULUCF_2018")  
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val clc_sq1 = spark.sql(""" 
-- MAGIC                    SELECT                
-- MAGIC                   CLC_2018.GridNum
-- MAGIC                   ,CLC_2018.GridNum10km                        
-- MAGIC                   ,CLC_2018.AreaHa
-- MAGIC
-- MAGIC                   ,LUT_clc_classes.LEVEL3_CODE
-- MAGIC                   ,LUT_clc_classes.LEVEL2_CODE
-- MAGIC                   ,LUT_clc_classes.LEVEL1_CODE
-- MAGIC                   ,LUT_clc_classes.LEVEL1B_CODE
-- MAGIC                   ,LUT_clc_classes.LEVEL2B_CODE
-- MAGIC                   ,LUT_clc_classes.LEVEL3_NAME
-- MAGIC                   ,LUT_clc_classes.LEVEL2_NAME
-- MAGIC                   ,LUT_clc_classes.LEVEL1_NAME
-- MAGIC                   ,LUT_clc_classes.LEVEL1B_NAME
-- MAGIC                   ,LUT_clc_classes.LEVEL2B_NAME
-- MAGIC                   ,LUT_clc_classes.L3_ALL
-- MAGIC                   ,LUT_clc_classes.L2_ALL
-- MAGIC                   ,LUT_clc_classes.L1_ALL
-- MAGIC                   ,LUT_clc_classes.L1B_ALL
-- MAGIC                   ,LUT_clc_classes.L2B_ALL
-- MAGIC                   ,LUT_clc_classes.MAES_CODE
-- MAGIC                   ,LUT_clc_classes.MAES_NAME
-- MAGIC                   ,LUT_clc_classes.LULUCF_CODE
-- MAGIC                   ,LUT_clc_classes.LULUCF_DESCRIPTION
-- MAGIC                   ,LUT_clc_classes.LULUCF_CODE_L2
-- MAGIC                   ,LUT_clc_classes.LULUCF_DESCRIPTION_L2
-- MAGIC                   ,LUT_clc_classes.LCET_CODE
-- MAGIC                   ,LUT_clc_classes.LCET_NAME
-- MAGIC                   ,LUT_clc_classes.ECOSYSTEM
-- MAGIC                   ,LUT_clc_classes.EU_Ecosystem_code
-- MAGIC                   ,LUT_clc_classes.EU_Ecosystem_name
-- MAGIC                   ,LUT_clc_classes.Tier_I_code
-- MAGIC                   ,LUT_clc_classes.Tier_II_code
-- MAGIC                   ,LUT_clc_classes.Tier_III_code
-- MAGIC                   ,LUT_clc_classes.Tier_I_desc
-- MAGIC                   ,LUT_clc_classes.Tier_II_desc
-- MAGIC                   ,LUT_clc_classes.Tier_III_desc
-- MAGIC                   ,LUT_clc_classes.Intensity_code
-- MAGIC                
-- MAGIC
-- MAGIC
-- MAGIC                   from CLC_2018   
-- MAGIC                   LEFT JOIN   LUT_clc_classes  
-- MAGIC                      ON  CLC_2018.Category  = LUT_clc_classes.LEVEL3_CODE where AreaHa = 1                                 
-- MAGIC                                                         """)                                  
-- MAGIC clc_sq1.createOrReplaceTempView("clc_2018_100m")  
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (3) ENV zones (Metzger) ################################################################################                 100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC // https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1519&fileId=544&successMessage=true
-- MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_EnvZones_544_2020528_100m
-- MAGIC
-- MAGIC ///   -  Category: 
-- MAGIC ///   -  MDN
-- MAGIC ///   -  NEM
-- MAGIC ///   -  ATC
-- MAGIC ///   -  ALS
-- MAGIC ///   -  ATN
-- MAGIC ///   -  PAN
-- MAGIC ///   -  ALN
-- MAGIC ///   -  BOR
-- MAGIC ///   -  CON
-- MAGIC ///   -  MDS
-- MAGIC ///   -  MDM
-- MAGIC ///   -  ARC
-- MAGIC ///   -  MAC
-- MAGIC ///   -  LUS
-- MAGIC ///   -  ANA
-- MAGIC
-- MAGIC val parquetFileDF_env_zones = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_EnvZones_544_2020528_100m/")
-- MAGIC parquetFileDF_env_zones.createOrReplaceTempView("env_zones")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (4) Organic-mineral soils ---Tanneberger 2017 ###############################################################################   100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (5) LCF ##############################################################################                 100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (6) Protected AREA (PA)  ##############################################################################                 100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC //D_PA2022_100m_935_20221111_100m
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1910&fileId=935
-- MAGIC
-- MAGIC //D_PA2022_100m_935_20221111_100m
-- MAGIC /// PA2022:
-- MAGIC
-- MAGIC val PA2022_100m_v2 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_PA2022_100m_935_20221111_100m/")             /// use load
-- MAGIC PA2022_100m_v2.createOrReplaceTempView("PA2022_100m_v2")
-- MAGIC
-- MAGIC // ------------------------------------------------
-- MAGIC // code	protection type
-- MAGIC // 1	Natura 2000 only  -n2k
-- MAGIC // 10	CDDA only
-- MAGIC // 11	Natura2000 and CDDA -nk2
-- MAGIC // 100 Emerald only
-- MAGIC // 101 Emerald and Natura 2000 * -n2k
-- MAGIC // 110	CDDA and Emerald
-- MAGIC // 111	CDDA, Natura2000 and Emerald * -n2k
-- MAGIC // -----------------------------------------
-- MAGIC
-- MAGIC val parquetFileDF_natura2000 = spark.sql(""" 
-- MAGIC Select 
-- MAGIC   gridnum,
-- MAGIC   GridNum10km,
-- MAGIC   IF(ProtectedArea2022_10m in (1,11,101,111), 'Natura2000' ,'not proteced') as natura2000_protection,
-- MAGIC   AreaHa
-- MAGIC from PA2022_100m_v2
-- MAGIC where ProtectedArea2022_10m in (1,11,101,111)
-- MAGIC """)
-- MAGIC parquetFileDF_natura2000.createOrReplaceTempView("Natura2000_100m_NET")  
-- MAGIC
-- MAGIC //// also set up a full PA dim net: protected and not protected:
-- MAGIC
-- MAGIC val parquetFileDF_pa_all = spark.sql(""" 
-- MAGIC Select 
-- MAGIC   gridnum,
-- MAGIC   GridNum10km,
-- MAGIC   IF(ProtectedArea2022_10m >0, 'protected' ,'not proteced') as PA_2022_protection,
-- MAGIC   AreaHa
-- MAGIC from PA2022_100m_v2
-- MAGIC
-- MAGIC """)
-- MAGIC parquetFileDF_pa_all.createOrReplaceTempView("Pa2022_100m_NET")  
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) Forest cliumate zones from GITHUB ##############################################################################                 100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC // Reading the LUT for CLC...:
-- MAGIC //val lut_forest_climate_zones  = spark.read.format("csv")
-- MAGIC //.options(Map("delimiter"->","))
-- MAGIC // .option("header", "true")
-- MAGIC //   .load("https://github.com/eea/ETC-LULUCF/blob/master/notebooks/output/NUTS_LUT_afforestation_scenario/JRC_yield_table/LUT_FOREST_ZONE.csv")     ////
-- MAGIC //lut_forest_climate_zones.createOrReplaceTempView("lut_forest_climate_zones")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (11) EU-DEM slope ##############################################################################                 1000m DIM  (will be updated by 100m)
-- MAGIC //##########################################################################################################################################
-- MAGIC //https://ec.europa.eu/eurostat/de/web/gisco/geodata/reference-data/elevation/eu-dem/slope
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1516&fileId=541
-- MAGIC
-- MAGIC
-- MAGIC // new 100mversion: M:\f01_dims\DEM\EU_DEM_Slope (jedi drive )
-- MAGIC //Source of this dimension: European Digital Elevation Model (EU-DEM), version 1.1
-- MAGIC //The EU-DEM v1.1 is a resulting dataset of the EU-DEM v1.0 upgrade which enhances the correction of geo-positioning issues, reducing the number of artefacts, improving //the vertical accuracy of EU-DEM using ICESat as reference and ensuring consistency with EU-Hydro public beta.
-- MAGIC //
-- MAGIC //The following rasters are derived from EU-DEM v1.1:
-- MAGIC //1. EU_DEM_v11_AI_Biodiversity_Nearest_1Km
-- MAGIC //2. Slope_Percent_EU_DEM_1Km
-- MAGIC //3. Slope_PercentReclass_EU_DEM_1Km
-- MAGIC ///Classes for Percentage of slope
-- MAGIC ///0 ,5.00,1,ValueToValue
-- MAGIC ///5.00,15.00,2,ValueToValue
-- MAGIC ///15.00,25.00,3,ValueToValue
-- MAGIC ///25.00,35.00,4,ValueToValue
-- MAGIC ///35.00,45.00,5,ValueToValue
-- MAGIC ///45.00,77.241341,6,ValueToValue
-- MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_EU_DEM_Slope_1Km_541_2020526_1km
-- MAGIC
-- MAGIC val parquetFileDF_dem_1km_slope = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_EU_DEM_Slope_1Km_541_2020526_1km/")
-- MAGIC parquetFileDF_dem_1km_slope.createOrReplaceTempView("dem_1km_slope")
-- MAGIC
-- MAGIC // next re-classify percent-slope into the three classes flat, moderate and steep:
-- MAGIC val dem_1km_slope_classes = spark.sql(""" 
-- MAGIC select 
-- MAGIC   GridNum10km
-- MAGIC   , GridNum as GridNum_1km 
-- MAGIC   ---,Category_Slope_Percent_EU_DEM_1Km as slope_percent
-- MAGIC   , if(Category_Slope_Percent_EU_DEM_1Km <= 15, 'FLAT',if(Category_Slope_Percent_EU_DEM_1Km <= 30, 'MODERATE',if(Category_Slope_Percent_EU_DEM_1Km < 10000, 'STEEP',NULL))) as slope_class
-- MAGIC   , AreaHa
-- MAGIC   from dem_1km_slope
-- MAGIC
-- MAGIC """)
-- MAGIC dem_1km_slope_classes.createOrReplaceTempView("dem_1km_slope_classes")  
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------



-- COMMAND ----------

show columns from  LUT_clc_classes

-- COMMAND ----------


SELECT	Category from env_zones

group by Category

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC //https://stackoverflow.com/questions/57014043/reading-data-from-url-using-spark-databricks-platform
-- MAGIC
-- MAGIC import org.apache.commons.io.IOUtils // jar will be already there in spark cluster no need to worry
-- MAGIC import java.net.URL 
-- MAGIC
-- MAGIC
-- MAGIC //https://github.com/eea/ETC-LULUCF/blob/c6c6d79725a489052d6578800d09adfd4e56664a/notebooks/output/NUTS_LUT_afforestation_scenario/JRC_yield_table/LUT_FOREST_ZONE.csv
-- MAGIC val urlfile=new URL("https://github.com/eea/ETC-LULUCF/blob/c6c6d79725a489052d6578800d09adfd4e56664a/notebooks/output/NUTS_LUT_afforestation_scenario/JRC_yield_table/LUT_FOREST_ZONE.csv")
-- MAGIC   val testcsvgit = IOUtils.toString(urlfile,"UTF-8").lines.toList.toDS()
-- MAGIC   val testcsv = spark
-- MAGIC                 .read.option("header", true)
-- MAGIC                 .option("inferSchema", true)
-- MAGIC                 .csv(testcsvgit)
-- MAGIC   testcsv.show
-- MAGIC
-- MAGIC   testcsv.createOrReplaceTempView("testcsv")  

-- COMMAND ----------

select * from testcsv

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import java.net.URL
-- MAGIC import org.apache.spark.SparkFiles
-- MAGIC val urlfile="https://github.com/eea/ETC-LULUCF/blob/c6c6d79725a489052d6578800d09adfd4e56664a/notebooks/output/NUTS_LUT_afforestation_scenario/JRC_yield_table/LUT_FOREST_ZONE.csv"
-- MAGIC spark.sparkContext.addFile(urlfile)
-- MAGIC
-- MAGIC val df = spark.read
-- MAGIC .option("inferSchema", true)
-- MAGIC .option("header", true)
-- MAGIC .csv("file://"+SparkFiles.get("//Workspace/c159s.csv"))   
-- MAGIC df.show
-- MAGIC
-- MAGIC
-- MAGIC //val LUT_nuts2021  = spark.read.format("csv")
-- MAGIC // .options(Map("delimiter"->"|"))
-- MAGIC // .schema(schema_nuts2021)
-- MAGIC // .load("dbfs:/mnt/trainingDatabricks/Lookups/adm_eea39_2021LUT/20200527111402.69.csv")
-- MAGIC //LUT_nuts2021.createOrReplaceTempView("LUT_nuts2021")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # IMPORT tables 
-- MAGIC import pandas as pd
-- MAGIC LUT_FOREST_ZONE = pd.read_csv("./tables/LUT_FOREST_ZONE.csv")
-- MAGIC LUT_FOREST_ZONE

-- COMMAND ----------

show columns from Pa2022_100m_NET

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.2 Loading LookUpTables for the model

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## This notebook imported the pyhton-data frame into a SQL table:
-- MAGIC
-- MAGIC ########### LUT (1) Forest Zone: ############################################
-- MAGIC # IMPORT tables 
-- MAGIC import pandas as pd
-- MAGIC LUT_FOREST_ZONE_table = pd.read_csv("./tables/LUT_FOREST_ZONE.csv")
-- MAGIC # dataframe to table:
-- MAGIC df_LUT_FOREST_ZONE = spark.createDataFrame(LUT_FOREST_ZONE_table)
-- MAGIC df_LUT_FOREST_ZONE.createOrReplaceTempView("LUT_FOREST_ZONE")
-- MAGIC
-- MAGIC ########### LUT (2) Conversion_LU_LEVEL_SOC ####################################
-- MAGIC # IMPORT tables 
-- MAGIC
-- MAGIC LUT_LEVEL_SOC_classif_table = pd.read_csv("./tables/LUT_CONVERSION_LU_LEVEL_SOC_classif.csv")
-- MAGIC # dataframe to table:
-- MAGIC df_LUT_CONVERSION_LU_LEVEL_SOC_classif = spark.createDataFrame(LUT_LEVEL_SOC_classif_table)
-- MAGIC df_LUT_CONVERSION_LU_LEVEL_SOC_classif.createOrReplaceTempView("LUT_CONVERSION_LU_LEVEL_SOC_classif")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from LUT_CONVERSION_LU_LEVEL_SOC_classif

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val LUT_FOREST_ZONE = spark.sql("select * from testDF")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.3 Building afforestation mask (cube-strata)

-- COMMAND ----------

SELECT 

 nuts3_2021.ADM_ID
,nuts3_2021.ADM_COUNTRY
,nuts3_2021.TAA
,nuts3_2021.NUTS_EU
,nuts3_2021.LEVEL3_name
,nuts3_2021.LEVEL3_code
,nuts3_2021.GridNum1km
,nuts3_2021.GridNum10km
,nuts3_2021.GridNum
,nuts3_2021.EU28
,nuts3_2021.EU27_2020
,nuts3_2021.EU27_2007
,nuts3_2021.EEA39
,nuts3_2021.EEA38_2020
,nuts3_2021.Category
,nuts3_2021.AreaHa


,clc_2018_100m.LEVEL3_CODE
,clc_2018_100m.LEVEL2_CODE
,clc_2018_100m.LEVEL1_CODE
,clc_2018_100m.LEVEL3_NAME
,clc_2018_100m.LEVEL2_NAME
,clc_2018_100m.LEVEL1_NAME

,clc_2018_100m.MAES_CODE
,clc_2018_100m.MAES_NAME
,clc_2018_100m.LULUCF_CODE
,clc_2018_100m.LULUCF_DESCRIPTION
,clc_2018_100m.LULUCF_CODE_L2
,clc_2018_100m.LULUCF_DESCRIPTION_L2
,env_zones.Category as env_zone
,PA_2022_protection
,natura2000_protection
,slope_class

from nuts3_2021

left JOIN clc_2018_100m       on     clc_2018_100m.gridnum = nuts3_2021.gridnum 
left JOIN env_zones           on     env_zones.gridnum = nuts3_2021.gridnum 
left JOIN Natura2000_100m_NET on     Natura2000_100m_NET.gridnum = nuts3_2021.gridnum 
left JOIN Pa2022_100m_NET     on     Pa2022_100m_NET.gridnum = nuts3_2021.gridnum 


left JOIN dem_1km_slope_classes     on     dem_1km_slope_classes.gridnum_1km = nuts3_2021.GridNum1km   --- jon on 1km!!



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2.Exporting cube
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC /// export cube
-- MAGIC val afforestation_cube_1 = spark.sql("""              
-- MAGIC       SELECT 
-- MAGIC
-- MAGIC       nuts3_2021.ADM_ID
-- MAGIC       ,nuts3_2021.ADM_COUNTRY
-- MAGIC       ,nuts3_2021.TAA
-- MAGIC       ,nuts3_2021.NUTS_EU
-- MAGIC       ,nuts3_2021.LEVEL3_name
-- MAGIC       ,nuts3_2021.LEVEL3_code
-- MAGIC       ,nuts3_2021.GridNum1km
-- MAGIC       ,nuts3_2021.GridNum10km
-- MAGIC       ,nuts3_2021.GridNum
-- MAGIC       ,nuts3_2021.EU28
-- MAGIC       ,nuts3_2021.EU27_2020
-- MAGIC       ,nuts3_2021.EU27_2007
-- MAGIC       ,nuts3_2021.EEA39
-- MAGIC       ,nuts3_2021.EEA38_2020
-- MAGIC       ----,nuts3_2021.Category
-- MAGIC       ,nuts3_2021.AreaHa
-- MAGIC
-- MAGIC
-- MAGIC       ,clc_2018_100m.LEVEL3_CODE
-- MAGIC       ,clc_2018_100m.LEVEL2_CODE
-- MAGIC       ,clc_2018_100m.LEVEL1_CODE
-- MAGIC       ,clc_2018_100m.LEVEL3_NAME
-- MAGIC       ,clc_2018_100m.LEVEL2_NAME
-- MAGIC       ,clc_2018_100m.LEVEL1_NAME
-- MAGIC
-- MAGIC       ,clc_2018_100m.MAES_CODE
-- MAGIC       ,clc_2018_100m.MAES_NAME
-- MAGIC       ,clc_2018_100m.LULUCF_CODE
-- MAGIC       ,clc_2018_100m.LULUCF_DESCRIPTION
-- MAGIC       ,clc_2018_100m.LULUCF_CODE_L2
-- MAGIC       ,clc_2018_100m.LULUCF_DESCRIPTION_L2
-- MAGIC       ,env_zones.Category as env_zone
-- MAGIC       ,PA_2022_protection
-- MAGIC       ,natura2000_protection
-- MAGIC       ,slope_class
-- MAGIC
-- MAGIC       from nuts3_2021
-- MAGIC
-- MAGIC       left JOIN clc_2018_100m       on     clc_2018_100m.gridnum = nuts3_2021.gridnum 
-- MAGIC       left JOIN env_zones           on     env_zones.gridnum = nuts3_2021.gridnum 
-- MAGIC       left JOIN Natura2000_100m_NET on     Natura2000_100m_NET.gridnum = nuts3_2021.gridnum 
-- MAGIC       left JOIN Pa2022_100m_NET     on     Pa2022_100m_NET.gridnum = nuts3_2021.gridnum 
-- MAGIC
-- MAGIC
-- MAGIC       left JOIN dem_1km_slope_classes     on     dem_1km_slope_classes.gridnum_1km = nuts3_2021.GridNum1km   --- jon on 1km!!
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC                  """)    
-- MAGIC
-- MAGIC afforestation_cube_1
-- MAGIC
-- MAGIC     .coalesce(1) //be careful with this
-- MAGIC     .write.format("com.databricks.spark.csv")
-- MAGIC     .mode(SaveMode.Overwrite)
-- MAGIC     .option("sep","|")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
-- MAGIC     .option("emptyValue", "")
-- MAGIC     .option("header","true")
-- MAGIC
-- MAGIC     ///.option("encoding", "UTF-16")  /// check ENCODING
-- MAGIC
-- MAGIC     .option("treatEmptyValuesAsNulls", "true")  
-- MAGIC     
-- MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/carbon_mapping/afforestation_simulation/afforestation_cube_1")
-- MAGIC
-- MAGIC
-- MAGIC     afforestation_cube_1.createOrReplaceTempView("afforestation_cube_1")  
-- MAGIC
-- MAGIC   
-- MAGIC

-- COMMAND ----------


