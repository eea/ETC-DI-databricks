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

# COMMAND ----------

# MAGIC %sql
# MAGIC select LULUCF_CODE,LULUCF_DESCRIPTION
# MAGIC  from lULUCF_2018
# MAGIC
# MAGIC group by 
# MAGIC LULUCF_CODE,LULUCF_DESCRIPTION
# MAGIC
# MAGIC
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

# MAGIC %sql
# MAGIC
# MAGIC --- 1 SET UP SUB-CUBE for the SOC dashboard:
# MAGIC
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
# MAGIC   isric_30.ocs030cm100m as SOC_STOCK_isric30cm_t_per_ha,    --values expressed as t/ha
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones
# MAGIC   
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC
# MAGIC carbonStocks_0_100cm_100m
# MAGIC LEFT JOIN isric_30     on nuts3_2021.GridNum = isric_30.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum = organic_soil.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC --INFO:......FL	Forest land  # CL	Cropland # GL	Grassland #SL	Settlements #WL	Wetlands #OL	Other land #null	null
# MAGIC

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
# MAGIC   SUM(isric_30.ocs030cm100m)  as SOC_STOCK_isric30cm_t_per_ha,    --values expressed as t/ha
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
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum = organic_soil.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE in ('CL','GL','SL','OL')
# MAGIC
# MAGIC group by 
# MAGIC   nuts3_2021.GridNum,
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_1_30cm")

# COMMAND ----------

# MAGIC %md #### (2.1.2) DASHBOARD SOC-STOCk (B) ISRIC 100cm for Forest

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ----- Testing cube for soc-stock forest using isric 100cm dataset:
# MAGIC
# MAGIC SELECT 
# MAGIC   nuts3_2021.GridNum,
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
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(isric_100.ocs030cm100m)  as SOC_STOCK_isric100cm_t_per_ha,    --values expressed as t/ha
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
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum = organic_soil.GridNum
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null and lULUCF_2018.LULUCF_CODE = 'FL'   ---- only for forest
# MAGIC
# MAGIC group by 
# MAGIC   nuts3_2021.GridNum,
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
# MAGIC
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_2_100cm")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### (2.2) SOC FLUX

# COMMAND ----------

# MAGIC %md #### (2.2.1) DASHBOARD  SOC-FLUX (A) JRC-DAY-CENT 1km time series for Cropland and Grassland

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### (2.3) AGB STOCK

# COMMAND ----------

# MAGIC %md #### (2.3.1) DASHBOARD  AGB-STOCK (A)  ESA CCI (100m) 2018 for Forest, Agro-forestry and permanent crops  Settlements  (check UNITS!!)

# COMMAND ----------

# MAGIC %md #### (2.3.2) DASHBOARD  AGB-STOCK (B)  GDMP2018 for Grassland 

# COMMAND ----------

# MAGIC %md ### (2.4) AGB FLUX
