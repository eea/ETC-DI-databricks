# Databricks notebook source
# MAGIC %md # Carbon mapping  - Above ground biomass (AGB) -based on ESA CCI
# MAGIC
# MAGIC https://climate.esa.int/en/odp/#/project/biomass
# MAGIC
# MAGIC Carbon mapping is a critical endeavor in our efforts to better understand and combat climate change. One fundamental aspect of this mapping process is the assessment of Above Ground Biomass (AGB), which refers to the total mass of living vegetation and trees found in a specific area. Accurate AGB measurements are pivotal in estimating carbon stocks and fluxes, as trees and plants play a vital role in sequestering carbon dioxide from the atmosphere. The European Space Agency (ESA) Climate Change Initiative (CCI) has emerged as a key player in advancing our capabilities in AGB assessment and carbon mapping, harnessing the power of satellite technology and sophisticated data analysis techniques to provide valuable insights into our planet's carbon dynamics. In this exploration, we delve into the world of AGB-based carbon mapping and how the ESA CCI contributes to this crucial scientific endeavor.
# MAGIC
# MAGIC This notebook extracts all existing AGB datasets and builds a CUBE from them. 
# MAGIC Datasource information: https://climate.esa.int/media/documents/D4.3_CCI_PUG_V4.0_20230605.pdf
# MAGIC info: loehnertz@space4environment.com

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/AGB_time_series.png?raw=true?raw=true)
# MAGIC
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
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 10 (AGB )ESA CCI Above Ground Biomass 2018 v4 ############################## 100m DIMENSIONS
# MAGIC //##########################################################################################################################################
# MAGIC //  Biomass Climate Change Initiative (Biomass_cci): Global datasets of forest above-ground biomass for the year 2018, v4
# MAGIC //  Data as been resampled to 100m into ETRS89 projection
# MAGIC //   This DIM is valid for the 2018 year, include 2 values
# MAGIC //     1) above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) (raster dataset). This is defined as the mass, expressed as oven-dry weight of the woody parts (stem, bark, // 
# MAGIC //     branches and twigs) of all living trees excluding stump and roots
# MAGIC //   [ESA_CCI_AGB_2018]
# MAGIC //   2) per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha (raster dataset)  UNIT: [Mg/ha]
# MAGIC //   [ESA_CCI_AGB_2018_SD]
# MAGIC //  no data value: -9999
# MAGIC //----------------------------------------------------------------------------------
# MAGIC // STATUS layer:
# MAGIC // 2010
# MAGIC // 2017
# MAGIC // 2018
# MAGIC // 2020
# MAGIC //----------------------------------------------------------------------------------
# MAGIC // CHANGE layer:
# MAGIC // 2019-2018
# MAGIC // 2018-2017
# MAGIC // 2020-2019
# MAGIC // 2020-2010
# MAGIC //----------------------------------------------------------------------------------
# MAGIC
# MAGIC //// 11 (AGB 2010)ESA CCI Above Ground Biomass 2010 v4 ############################## 1 100m DIM
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1948&fileId=973
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_ESACCI2010v3_973_2023216_100m
# MAGIC val parquetFileDF_AGB_2010 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ESACCI2010v3_973_2023216_100m/")
# MAGIC parquetFileDF_AGB_2010.createOrReplaceTempView("AGB_2010")
# MAGIC
# MAGIC //// 12 (AGB 2018)ESA CCI Above Ground Biomass 2017 v4 ############################## 1 100m DIM
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1738&fileId=763
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_Biomass17_763_2021127_100m
# MAGIC val parquetFileDF_AGB_2017 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ESACCI2017v3_967_2023214_100m/")
# MAGIC parquetFileDF_AGB_2017.createOrReplaceTempView("AGB_2017")
# MAGIC
# MAGIC //// 13 (AGB 2018)ESA CCI Above Ground Biomass 2018 v4 ############################## 1 100m DIM
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1955&fileId=980
# MAGIC val parquetFileDF_AGB_2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ESACCIAGB2018v3a_980_2023223_100m/")
# MAGIC parquetFileDF_AGB_2018.createOrReplaceTempView("AGB_2018")  
# MAGIC
# MAGIC //// 14 (AGB 2020)ESA CCI Above Ground Biomass 2020 v4 ############################## 1 100m DIM
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1993&fileId=1015
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_ESACCIAGB2020v4_1015_202374_100m
# MAGIC val parquetFileDF_AGB_2020 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ESACCIAGB2020v4_1015_202374_100m/")
# MAGIC parquetFileDF_AGB_2020.createOrReplaceTempView("AGB_2020")
# MAGIC
# MAGIC
# MAGIC
# MAGIC // AGB CHANGE: Quality flag integer values:
# MAGIC // 0: AGB=0 in both maps
# MAGIC // 1: AGB loss
# MAGIC // 2: Potential AGB loss
# MAGIC // 3: Improbable change
# MAGIC // 4: Potential AGB gain
# MAGIC // 5: AGB gain
# MAGIC
# MAGIC //// 15 (AGB 2020)ESA CCI Above Ground Biomass 2020 v4 ############################## 1 100m DIM CHANGES:...........
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1998&fileId=1020
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_esa20202010agb_1020_202375_100m
# MAGIC val parquetFileDF_AGB_20202010 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_esa20202010agb_1020_202375_100m/")
# MAGIC parquetFileDF_AGB_20202010.createOrReplaceTempView("AGB_2020_2010")
# MAGIC
# MAGIC //// 16 (AGB 2020)ESA CCI Above Ground Biomass 2020 v4 ############################## 1 100m DIM
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2000&fileId=1022
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_esaagb20182017_1022_202375_100m
# MAGIC val parquetFileDF_AGB_20182017 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_esaagb20182017_1022_202375_100m/")
# MAGIC parquetFileDF_AGB_20182017.createOrReplaceTempView("AGB_2018_2017")
# MAGIC
# MAGIC //// 167 (AGB 2020)ESA CCI Above Ground Biomass 2020 v4 ############################## 1 100m DIM
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2001&fileId=1023
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_esaagb20192018_1023_202375_100m
# MAGIC val parquetFileDF_AGB_20192018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_esaagb20192018_1023_202375_100m/")
# MAGIC parquetFileDF_AGB_20192018.createOrReplaceTempView("AGB_2019_2018")
# MAGIC
# MAGIC //// 18 (AGB 2020)ESA CCI Above Ground Biomass 2020 v4 ############################## 1 100m DIM
# MAGIC //
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_esaagb20202019_1021_202375_100m
# MAGIC val parquetFileDF_AGB_20202019 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_esaagb20202019_1021_202375_100m/")
# MAGIC parquetFileDF_AGB_20202019.createOrReplaceTempView("AGB_2020_2019")
# MAGIC
# MAGIC
# MAGIC //-----------------------Protected area PA2022:...........................................................
# MAGIC
# MAGIC ///PA 100m
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
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2) Building & testing CUBES
# MAGIC The following lines constructed a JOIN between all single tables and transforme it to an data-cube:
# MAGIC
# MAGIC https://www.markdownguide.org/basic-syntax/

# COMMAND ----------

# MAGIC %md ### 2.1 ) Building F-CUBE

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > #### The following box creates a large "CUBE"  
# MAGIC > 
# MAGIC > ###### There, ALL the required single DIMS are connected to the ADMIN-DIM. The resolution of the CUBE is 1ha. 
# MAGIC >
# MAGIC > Analysis DIMS:
# MAGIC - NUTS3
# MAGIC - Env.Zones
# MAGIC - Protected Areas
# MAGIC - LULUCF Categories
# MAGIC > AGB STATUS DIMS:
# MAGIC - AGB_2010 biomass t/ha
# MAGIC - AGB_2010 standard deviation (SD)
# MAGIC - AGB_2017 biomass t/ha
# MAGIC - AGB_2017 standard deviation (SD)
# MAGIC - AGB_2018 biomass t/ha
# MAGIC - AGB_2018 standard deviation (SD)
# MAGIC - AGB_2020 biomass t/ha
# MAGIC - AGB_2020 standard deviation (SD)
# MAGIC > AGB CAHNGE DIMS:
# MAGIC - AGB_2020_2010 Quality flag (QF)
# MAGIC - AGB_2020_2010 standard deviation (SD)
# MAGIC - AGB_2018_2017 Quality flag (QF)
# MAGIC - AGB_2018_2017 standard deviation (SD)
# MAGIC - AGB_2019_2018 Quality flag (QF)
# MAGIC - AGB_2019_2018 standard deviation (SD) 
# MAGIC - AGB_2020_2019	Quality flag (QF)
# MAGIC - AGB_2020_2019	standard deviation (SD)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql  -- testing and checking SQL for the final query (next box)
# MAGIC
# MAGIC --SELECT 
# MAGIC --  nuts3_2021.Category, ----FOR ADMIN
# MAGIC --  nuts3_2021.GridNum10km,
# MAGIC --  nuts3_2021.ADM_ID,
# MAGIC --  nuts3_2021.ADM_COUNTRY	,
# MAGIC --  nuts3_2021.ISO2	,
# MAGIC --  nuts3_2021.LEVEL3_name	,
# MAGIC --  nuts3_2021.LEVEL2_name	,
# MAGIC --  nuts3_2021.LEVEL1_name	,
# MAGIC --  nuts3_2021.LEVEL0_name	,
# MAGIC --  nuts3_2021.LEVEL3_code	,
# MAGIC --  nuts3_2021.LEVEL2_code	,
# MAGIC --  nuts3_2021.LEVEL1_code	,
# MAGIC --  nuts3_2021.LEVEL0_code	,
# MAGIC --  nuts3_2021.NUTS_EU,	
# MAGIC --  nuts3_2021.TAA ,
# MAGIC --  lULUCF_2018.LULUCF_CODE,
# MAGIC --  lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC --  nuts3_2021.AreaHa,
# MAGIC --
# MAGIC --  natura2000_protection,
# MAGIC --  env_zones.Category as env_zones,
# MAGIC --
# MAGIC ----STATUS:
# MAGIC --  AGB_2010.esacci2010_etrs89    as agb_2010,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) 
# MAGIC --  AGB_2017.esacci_2017_NN_100m  as agb_2017,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha)  
# MAGIC --  AGB_2018.esacciagb2018        as agb_2018,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) 
# MAGIC --  AGB_2020.ESA_CCI_AGB_2020     as agb_2020,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) 
# MAGIC --
# MAGIC --  AGB_2010.esaccisd2010_etrs89    as SD_2010,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC --  AGB_2017.esacci_SD2017_NN_100m  as SD_2017,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC --  AGB_2018.esacciagbsd2018        as SD_2018,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC --  AGB_2020.ESA_CCI_AGB_2020_SD    as SD_2020,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC --
# MAGIC -----CHANGE:
# MAGIC --    AGB_2020_2010.ESA_CCI_AGB_2020_2010_QF_v2   as AGB_2020_2010_QF,
# MAGIC --    AGB_2018_2017.ESA_CCI_AGB_2018_2017_QF_v2	as AGB_2018_2017_QF,
# MAGIC --    AGB_2019_2018.ESA_CCI_AGB_2019_2018_QF_v2   as AGB_2019_2018_QF,
# MAGIC --    AGB_2020_2019.ESA_CCI_AGB_2020_2019_QF	    as AGB_2020_2019_QF,
# MAGIC --
# MAGIC --    AGB_2020_2010.ESA_CCI_AGB_2020_2010_SD_v2   as AGB_2020_2010_SD,
# MAGIC --    AGB_2018_2017.ESA_CCI_AGB_2018_2017_SD_v2   as AGB_2018_2017_SD,
# MAGIC --    AGB_2019_2018.ESA_CCI_AGB_2019_2018_SD_v2   as AGB_2019_2018_SD,
# MAGIC --    AGB_2020_2019.ESA_CCI_AGB_2020_2019_SD      as AGB_2020_2019_SD,
# MAGIC --
# MAGIC --    'ESA CCI AGB' as datasource --- info
# MAGIC --
# MAGIC --
# MAGIC --from nuts3_2021
# MAGIC --
# MAGIC --LEFT JOIN AGB_2010     on nuts3_2021.GridNum = AGB_2010.gridnum
# MAGIC --LEFT JOIN AGB_2017     on nuts3_2021.GridNum = AGB_2017.gridnum
# MAGIC --LEFT JOIN AGB_2018     on nuts3_2021.GridNum = AGB_2018.gridnum
# MAGIC --LEFT JOIN AGB_2020     on nuts3_2021.GridNum = AGB_2020.gridnum
# MAGIC --
# MAGIC --LEFT JOIN AGB_2020_2010 on   nuts3_2021.GridNum =AGB_2020_2010.gridnum   
# MAGIC --LEFT JOIN AGB_2018_2017 on	 nuts3_2021.GridNum =AGB_2018_2017.gridnum	
# MAGIC --LEFT JOIN AGB_2019_2018 on   nuts3_2021.GridNum =AGB_2019_2018.gridnum   
# MAGIC --LEFT JOIN AGB_2020_2019 on   nuts3_2021.GridNum =AGB_2020_2019.gridnum
# MAGIC --
# MAGIC --
# MAGIC --LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC --LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC --LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC --where  nuts3_2021.LEVEL3_code is not null  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // the following script produce a temp-table with the name: [ESA_CCI_CUBE]
# MAGIC
# MAGIC val ESA_CCI_CUBE = spark.sql("""
# MAGIC   
# MAGIC SELECT 
# MAGIC   nuts3_2021.Category, ----FOR ADMIN
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
# MAGIC   nuts3_2021.AreaHa,
# MAGIC
# MAGIC   natura2000_protection,
# MAGIC   env_zones.Category as env_zones,
# MAGIC
# MAGIC --STATUS:
# MAGIC   AGB_2010.esacci2010_etrs89    as agb_2010,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) 
# MAGIC   AGB_2017.esacci_2017_NN_100m  as agb_2017,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha)  
# MAGIC   AGB_2018.esacciagb2018        as agb_2018,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) 
# MAGIC   AGB_2020.ESA_CCI_AGB_2020     as agb_2020,   -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) 
# MAGIC
# MAGIC   AGB_2010.esaccisd2010_etrs89    as SD_2010,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC   AGB_2017.esacci_SD2017_NN_100m  as SD_2017,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC   AGB_2018.esacciagbsd2018        as SD_2018,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC   AGB_2020.ESA_CCI_AGB_2020_SD    as SD_2020,--per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha
# MAGIC
# MAGIC ---CHANGE:
# MAGIC     AGB_2020_2010.ESA_CCI_AGB_2020_2010_QF_v2   as AGB_2020_2010_QF,
# MAGIC     AGB_2018_2017.ESA_CCI_AGB_2018_2017_QF_v2	as AGB_2018_2017_QF,
# MAGIC     AGB_2019_2018.ESA_CCI_AGB_2019_2018_QF_v2   as AGB_2019_2018_QF,
# MAGIC     AGB_2020_2019.ESA_CCI_AGB_2020_2019_QF	    as AGB_2020_2019_QF,
# MAGIC
# MAGIC     AGB_2020_2010.ESA_CCI_AGB_2020_2010_SD_v2   as AGB_2020_2010_SD,
# MAGIC     AGB_2018_2017.ESA_CCI_AGB_2018_2017_SD_v2   as AGB_2018_2017_SD,
# MAGIC     AGB_2019_2018.ESA_CCI_AGB_2019_2018_SD_v2   as AGB_2019_2018_SD,
# MAGIC     AGB_2020_2019.ESA_CCI_AGB_2020_2019_SD      as AGB_2020_2019_SD,
# MAGIC
# MAGIC     'ESA CCI AGB' as datasource --- info
# MAGIC
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN AGB_2010     on nuts3_2021.GridNum = AGB_2010.gridnum
# MAGIC LEFT JOIN AGB_2017     on nuts3_2021.GridNum = AGB_2017.gridnum
# MAGIC LEFT JOIN AGB_2018     on nuts3_2021.GridNum = AGB_2018.gridnum
# MAGIC LEFT JOIN AGB_2020     on nuts3_2021.GridNum = AGB_2020.gridnum
# MAGIC
# MAGIC LEFT JOIN AGB_2020_2010 on   nuts3_2021.GridNum =AGB_2020_2010.gridnum   
# MAGIC LEFT JOIN AGB_2018_2017 on	 nuts3_2021.GridNum =AGB_2018_2017.gridnum	
# MAGIC LEFT JOIN AGB_2019_2018 on   nuts3_2021.GridNum =AGB_2019_2018.gridnum   
# MAGIC LEFT JOIN AGB_2020_2019 on   nuts3_2021.GridNum =AGB_2020_2019.gridnum
# MAGIC
# MAGIC
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC where  nuts3_2021.LEVEL3_code is not null  
# MAGIC
# MAGIC
# MAGIC
# MAGIC """)
# MAGIC
# MAGIC //ESA_CCI_CUBE
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
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB_timeseries")
# MAGIC
# MAGIC  ESA_CCI_CUBE.createOrReplaceTempView("ESA_CCI_CUBE")
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC No, you can use the F-cube: **ESA_CCI_CUBE** in the next queries: for example ''' select * from ESA_CCI_CUBE '''

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from ESA_CCI_CUBE

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN ESA_CCI_CUBE

# COMMAND ----------

# MAGIC %md ### 2.2) Building "C-CUBE" 
# MAGIC - Selected DIMS (AGB) will be aggregated to analysis DIMS (nuts, pa, env,)

# COMMAND ----------

# MAGIC %sql --- C-CUBE 1: 
# MAGIC SELECT 
# MAGIC ISO2
# MAGIC , LEVEL3_code
# MAGIC , TAA
# MAGIC ,ifnull(LULUCF_CODE,'no_LULUCF_CODE')  as LULUCF_CODE  --- set NULL to a better "value" to be able to join 
# MAGIC ,ifnull(LULUCF_DESCRIPTION,'no_LULUCF_CODE')  as LULUCF_DESCRIPTION --- set NULL to a better "value" to be able to join 
# MAGIC ,ifnull(natura2000_protection,'no_protection')  as natura2000_protection --- set NULL to a better "value" to be able to join 
# MAGIC ,ifnull(env_zones,'no_env_zones')  as env_zones --- set NULL to a better "value" to be able to join 
# MAGIC
# MAGIC , sum(AreaHa) as AreaHa
# MAGIC --- SUM into Tonnes: 
# MAGIC , sum(agb_2010) as agb_2010
# MAGIC , sum(agb_2017) as agb_2017
# MAGIC , sum(agb_2018) as agb_2018
# MAGIC , sum(agb_2020) as agb_2020
# MAGIC --- weighted avg SD : 
# MAGIC ,SUM(SD_2010)/ sum(AreaHa)   as SD_2010
# MAGIC ,SUM(SD_2017)/ sum(AreaHa)   as SD_2017
# MAGIC ,SUM(SD_2018)/ sum(AreaHa)   as SD_2018
# MAGIC ,SUM(SD_2020)/ sum(AreaHa)   as SD_2020
# MAGIC ---only avg for testing:
# MAGIC ,AVG(SD_2010)  as SD_2010_avg_for_qc
# MAGIC --- , AGB_2020_2010_QF
# MAGIC --- , AGB_2018_2017_QF
# MAGIC --- , AGB_2019_2018_QF
# MAGIC --- , AGB_2020_2019_QF
# MAGIC --- , AGB_2020_2010_SD
# MAGIC --- , AGB_2018_2017_SD
# MAGIC --- , AGB_2019_2018_SD
# MAGIC --- , AGB_2020_2019_SD
# MAGIC --- , datasource 
# MAGIC from  ESA_CCI_CUBE
# MAGIC GROUP BY 
# MAGIC ISO2
# MAGIC , LEVEL3_code
# MAGIC , TAA
# MAGIC , LULUCF_CODE
# MAGIC , LULUCF_DESCRIPTION
# MAGIC , natura2000_protection
# MAGIC , env_zones
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC /// build up F_cube: ESA_CCI_CUBE_STATUS (aggregated CUBE for AGB STATUS )
# MAGIC
# MAGIC // the following script produce a tabel with the name: [ESA_CCI_CUBE_STATUS_biomass]
# MAGIC val ESA_CCI_CUBE_STATUS_biomass = spark.sql("""
# MAGIC  SELECT 
# MAGIC ISO2
# MAGIC , LEVEL3_code
# MAGIC , TAA
# MAGIC ,ifnull(LULUCF_CODE,'no_LULUCF_CODE')  as LULUCF_CODE
# MAGIC ,ifnull(LULUCF_DESCRIPTION,'no_LULUCF_CODE')  as LULUCF_DESCRIPTION
# MAGIC ,ifnull(natura2000_protection,'no_protection')  as natura2000_protection
# MAGIC ,ifnull(env_zones,'no_env_zones')  as env_zones
# MAGIC
# MAGIC , sum(AreaHa) as AreaHa
# MAGIC --- SUM into Tonnes: 
# MAGIC , sum(agb_2010) as agb_2010
# MAGIC , sum(agb_2017) as agb_2017
# MAGIC , sum(agb_2018) as agb_2018
# MAGIC , sum(agb_2020) as agb_2020
# MAGIC --- weighted avg SD : 
# MAGIC ---,SUM(SD_2010)/ sum(AreaHa)   as SD_2010
# MAGIC ---,SUM(SD_2017)/ sum(AreaHa)   as SD_2017
# MAGIC ---,SUM(SD_2018)/ sum(AreaHa)   as SD_2018
# MAGIC ---,SUM(SD_2020)/ sum(AreaHa)   as SD_2020
# MAGIC
# MAGIC
# MAGIC from  ESA_CCI_CUBE
# MAGIC GROUP BY 
# MAGIC ISO2
# MAGIC , LEVEL3_code
# MAGIC , TAA
# MAGIC , LULUCF_CODE
# MAGIC , LULUCF_DESCRIPTION
# MAGIC , natura2000_protection
# MAGIC , env_zones
# MAGIC
# MAGIC """)
# MAGIC
# MAGIC //ESA_CCI_CUBE_STATUS
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
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB_timeseries/ESA_CCI_CUBE_STATUS")
# MAGIC
# MAGIC  ESA_CCI_CUBE_STATUS_biomass.createOrReplaceTempView("ESA_CCI_CUBE_STATUS_biomass")
# MAGIC
# MAGIC //--------------------------------
# MAGIC
# MAGIC // the following script produce a tabel with the name: [ESA_CCI_CUBE_STATUS_SD]
# MAGIC val ESA_CCI_CUBE_STATUS_SD = spark.sql("""
# MAGIC  SELECT 
# MAGIC ISO2
# MAGIC , LEVEL3_code
# MAGIC , TAA
# MAGIC ,ifnull(LULUCF_CODE,'no_LULUCF_CODE')  as LULUCF_CODE
# MAGIC ,ifnull(LULUCF_DESCRIPTION,'no_LULUCF_CODE')  as LULUCF_DESCRIPTION
# MAGIC ,ifnull(natura2000_protection,'no_protection')  as natura2000_protection
# MAGIC ,ifnull(env_zones,'no_env_zones')  as env_zones
# MAGIC
# MAGIC , sum(AreaHa) as AreaHa
# MAGIC --- SUM into Tonnes: 
# MAGIC  
# MAGIC ,SUM(SD_2010)/ sum(AreaHa)   as SD_2010
# MAGIC ,SUM(SD_2017)/ sum(AreaHa)   as SD_2017
# MAGIC ,SUM(SD_2018)/ sum(AreaHa)   as SD_2018
# MAGIC ,SUM(SD_2020)/ sum(AreaHa)   as SD_2020
# MAGIC
# MAGIC
# MAGIC from  ESA_CCI_CUBE
# MAGIC GROUP BY 
# MAGIC ISO2
# MAGIC , LEVEL3_code
# MAGIC , TAA
# MAGIC , LULUCF_CODE
# MAGIC , LULUCF_DESCRIPTION
# MAGIC , natura2000_protection
# MAGIC , env_zones
# MAGIC
# MAGIC """)
# MAGIC
# MAGIC //ESA_CCI_CUBE_STATUS_SD
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
# MAGIC //    .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB_timeseries/ESA_CCI_CUBE_STATUS_SD")
# MAGIC
# MAGIC  ESA_CCI_CUBE_STATUS_SD.createOrReplaceTempView("ESA_CCI_CUBE_STATUS_SD")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ESA_CCI_CUBE_STATUS_SD

# COMMAND ----------

# MAGIC %md ## 3) Set up data frame from CUBE for py-work

# COMMAND ----------

# MAGIC %md
# MAGIC use pandas  for  working wiht the cube
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
SELECT *   from ESA_CCI_CUBE_STATUS_biomass
''')

df = sql_for_panda.select("*").toPandas()

#for EVA https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html

df_transformed =df.melt(id_vars=[
'AreaHa',
'LEVEL3_code',
'ISO2',
'TAA',
'LULUCF_CODE',
'LULUCF_DESCRIPTION',
'natura2000_protection',
'env_zones'
 ], var_name="year", value_name="agb_tonnes")

## updapte year:
df_transformed['year_link'] = df_transformed['year'].str[-4:]
#
## dataframe to table:
df_transformed_c1 = spark.createDataFrame(df_transformed)
df_transformed_c1.createOrReplaceTempView("df_transformed_agb_biomass_tonnes")



# bring sql to pandas> ----------------------------table 2 ESA_CCI_CUBE_STATUS_SD
sql_for_panda_c2 = spark.sql('''
SELECT *   from ESA_CCI_CUBE_STATUS_SD
''')

df_c2 = sql_for_panda_c2.select("*").toPandas()

#for EVA https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html

df_transformed_c2 =df_c2.melt(id_vars=[
'AreaHa',
'LEVEL3_code',
'ISO2',
'TAA',
'LULUCF_CODE',
'LULUCF_DESCRIPTION',
'natura2000_protection',
'env_zones'
 ], var_name="year", value_name="agb_SD")

## updapte year:
df_transformed_c2['year_link'] = df_transformed_c2['year'].str[-4:]
#
## dataframe to table:
df_transformed_c2 = spark.createDataFrame(df_transformed_c2)
df_transformed_c2.createOrReplaceTempView("df_transformed_agb_SD")




# COMMAND ----------

# MAGIC %md
# MAGIC Start playing with the data:
# MAGIC

# COMMAND ----------

print (df_c2)

# COMMAND ----------



# COMMAND ----------

# correlation between esacciagb2010	 and esacciagb2020
print(df['esacciagb2010'].corr(df['esacciagb2020']))

# COMMAND ----------

import seaborn as sns
# use the function regplot to make a scatterplot
sns.regplot(x=df["esacciagb2010"], y=df["esacciagb2020"])

plt.savefig("Plotting_Correlation_Scatterplot_With_Regression_Fit.jpg")



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## 4) Final CUBES and export
# MAGIC In the following lines we exported the tables into greenmonkey

# COMMAND ----------

# MAGIC %md ### 4.1) Combine single timeseries bevor exporting the data to CWS:
# MAGIC
# MAGIC The following box used the anaylitcal & year to set up a single time series wiht different values (AGB-biomass, AGB-SD,...)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN df_transformed_agb_SD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC
# MAGIC df_transformed_agb_biomass_tonnes.LEVEL3_code
# MAGIC ,df_transformed_agb_biomass_tonnes.ISO2
# MAGIC ,df_transformed_agb_biomass_tonnes.TAA
# MAGIC ,df_transformed_agb_biomass_tonnes.LULUCF_CODE
# MAGIC ,df_transformed_agb_biomass_tonnes.LULUCF_DESCRIPTION
# MAGIC ,df_transformed_agb_biomass_tonnes.natura2000_protection
# MAGIC ,df_transformed_agb_biomass_tonnes.env_zones
# MAGIC --df_transformed_agb_biomass_tonnes.year
# MAGIC ,df_transformed_agb_biomass_tonnes.agb_tonnes
# MAGIC ,df_transformed_agb_SD.agb_SD
# MAGIC ,df_transformed_agb_biomass_tonnes.year_link
# MAGIC ,df_transformed_agb_biomass_tonnes.AreaHa
# MAGIC ,'calc based on EAS CCI data version 4 - conatact: Manuel'
# MAGIC
# MAGIC  from df_transformed_agb_biomass_tonnes  ---df_transformed_agb_SD
# MAGIC
# MAGIC LEFT JOIN df_transformed_agb_SD on 
# MAGIC
# MAGIC   df_transformed_agb_SD.LEVEL3_code = df_transformed_agb_biomass_tonnes.LEVEL3_code AND
# MAGIC   df_transformed_agb_SD.ISO2 = df_transformed_agb_biomass_tonnes.ISO2 AND
# MAGIC   df_transformed_agb_SD.TAA = df_transformed_agb_biomass_tonnes.TAA AND
# MAGIC   df_transformed_agb_SD.LULUCF_CODE = df_transformed_agb_biomass_tonnes.LULUCF_CODE AND
# MAGIC   df_transformed_agb_SD.LULUCF_DESCRIPTION = df_transformed_agb_biomass_tonnes.LULUCF_DESCRIPTION AND
# MAGIC   df_transformed_agb_SD.natura2000_protection = df_transformed_agb_biomass_tonnes.natura2000_protection AND
# MAGIC   df_transformed_agb_SD.env_zones = df_transformed_agb_biomass_tonnes.env_zones AND
# MAGIC   df_transformed_agb_SD.year_link = df_transformed_agb_biomass_tonnes.year_link 

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC
# MAGIC /// exporting AGB status tonnes and SD to CWS:
# MAGIC val tableDF_export_db_agb_status = spark.sql("""
# MAGIC
# MAGIC ---- COPY THE OUTPUT TABLE sql query here!!!!!!!!!!!!!
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC df_transformed_agb_biomass_tonnes.LEVEL3_code
# MAGIC ,df_transformed_agb_biomass_tonnes.ISO2
# MAGIC ,df_transformed_agb_biomass_tonnes.TAA
# MAGIC ,df_transformed_agb_biomass_tonnes.LULUCF_CODE
# MAGIC ,df_transformed_agb_biomass_tonnes.LULUCF_DESCRIPTION
# MAGIC ,df_transformed_agb_biomass_tonnes.natura2000_protection
# MAGIC ,df_transformed_agb_biomass_tonnes.env_zones
# MAGIC --df_transformed_agb_biomass_tonnes.year
# MAGIC ,df_transformed_agb_biomass_tonnes.agb_tonnes
# MAGIC ,df_transformed_agb_SD.agb_SD
# MAGIC ,df_transformed_agb_biomass_tonnes.year_link
# MAGIC ,df_transformed_agb_biomass_tonnes.AreaHa
# MAGIC ,'calc based on EAS CCI data version 4 - conatact: Manuel'
# MAGIC
# MAGIC  from df_transformed_agb_biomass_tonnes  ---df_transformed_agb_SD
# MAGIC
# MAGIC LEFT JOIN df_transformed_agb_SD on 
# MAGIC
# MAGIC   df_transformed_agb_SD.LEVEL3_code = df_transformed_agb_biomass_tonnes.LEVEL3_code AND
# MAGIC   df_transformed_agb_SD.ISO2 = df_transformed_agb_biomass_tonnes.ISO2 AND
# MAGIC   df_transformed_agb_SD.TAA = df_transformed_agb_biomass_tonnes.TAA AND
# MAGIC   df_transformed_agb_SD.LULUCF_CODE = df_transformed_agb_biomass_tonnes.LULUCF_CODE AND
# MAGIC   df_transformed_agb_SD.LULUCF_DESCRIPTION = df_transformed_agb_biomass_tonnes.LULUCF_DESCRIPTION AND
# MAGIC   df_transformed_agb_SD.natura2000_protection = df_transformed_agb_biomass_tonnes.natura2000_protection AND
# MAGIC   df_transformed_agb_SD.env_zones = df_transformed_agb_biomass_tonnes.env_zones AND
# MAGIC   df_transformed_agb_SD.year_link = df_transformed_agb_biomass_tonnes.year_link 
# MAGIC
# MAGIC """)
# MAGIC tableDF_export_db_agb_status
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB_timeseries/agb_status")
# MAGIC
# MAGIC     tableDF_export_db_agb_status.createOrReplaceTempView("tableDF_export_db_agb_status")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //SUB_CUBE_SOC_STOCK_3_wetland.createOrReplaceTempView("SOC_STOCK_3_wetland_nuts3")
# MAGIC 	

# COMMAND ----------


	### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB_timeseries/agb_status"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)
