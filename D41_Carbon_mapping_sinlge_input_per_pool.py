# Databricks notebook source
# MAGIC %md # Carbon mapping
# MAGIC
# MAGIC ![](https://space4environment.com/fileadmin/Resources/Public/Images/Logos/S4E-Logo.png)
# MAGIC
# MAGIC
# MAGIC This dashboard was created to have a simpler version of the D40 with the aim of using only one data source for each data pool. 

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
# MAGIC
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
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1955&fileId=980
# MAGIC val parquetFileDF_AGB_2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ESACCIAGB2018v3a_980_2023223_100m/")
# MAGIC parquetFileDF_AGB_2018.createOrReplaceTempView("AGB_2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
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

# COMMAND ----------

# MAGIC %md ### (1.1) Build the MAIN Referencedataset
# MAGIC -combination of NUTS3, 10km, LULUCF2018, PA(N2k) adn evn.Zones

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC ///2 (group by) SET UP SUB-CUBE for the SOC dashboard:
# MAGIC
# MAGIC /// example
# MAGIC // Exporting the final table  ---city indicator: ua-classes vs. clc-plus inside the core city:
# MAGIC val ref_cube = spark.sql("""
# MAGIC                 SELECT 
# MAGIC   
# MAGIC                     nuts3_2021.Category as admin_category, ----FOR ADMIN
# MAGIC                     nuts3_2021.GridNum10km,
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

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/ref_cube"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

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
# MAGIC val SUB_CUBE_SOC_STOCK_1_30cm_all = spark.sql("""
# MAGIC
# MAGIC SELECT 
# MAGIC   
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
# MAGIC
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   SUM(isric_30.ocs030cm100m)  as SOC_STOCK_isric30cm_t,    --values expressed as t/ha
# MAGIC
# MAGIC   lULUCF_2018.LULUCF_CODE,
# MAGIC   lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones,
# MAGIC   natura2000_protection,
# MAGIC   'ISRIC 30cm for all LULUCF classes )' as datasource
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN isric_30     on nuts3_2021.GridNum = isric_30.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC
# MAGIC where nuts3_2021.LEVEL3_code is not null
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
# MAGIC   natura2000_protection,
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil'))
# MAGIC --FL	Forest land  # CL	Cropland # GL	Grassland #SL	Settlements #WL	Wetlands #OL	Other land #null	null
# MAGIC             """)
# MAGIC SUB_CUBE_SOC_STOCK_1_30cm_all
# MAGIC
# MAGIC
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_1_30cm_10km_nuts3_all")
# MAGIC
# MAGIC  SUB_CUBE_SOC_STOCK_1_30cm_all.createOrReplaceTempView("SOC_STOCK_1_30cm_10km_nuts3_all")

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/SOC/SOC_STOCK_1_30cm_10km_nuts3_all"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ### (2.3) AGB STOCK
# MAGIC
# MAGIC ONLY useing ESA CCI FOREST 100m -- 
# MAGIC (Layer only valid for Forest! Nevertheless, the data is released here for all other lulucf classes... the customer is king.
# MAGIC )
# MAGIC
# MAGIC images/esa_cci_agb.PNG
# MAGIC
# MAGIC
# MAGIC ![](https://adb-664128750067591.11.azuredatabricks.net/?o=664128750067591#files/2057301627581187/Workspace/Repos/ETC DI/ETC-DI-databricks/images/esa_cci_agb.PNG?raw=true)
# MAGIC

# COMMAND ----------

# MAGIC %md #### (2.3.1) DASHBOARD  AGB-STOCK  ESA CCI (100m) 2018 for ALL LULUCF classes  (check UNITS!!)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC /// exporting AGB stock 
# MAGIC val tableDF_export_db_nuts3_agb1_all = spark.sql("""
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
# MAGIC   if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type,
# MAGIC   env_zones.Category as env_zones,
# MAGIC   SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC   natura2000_protection,
# MAGIC
# MAGIC  float(NULL) as GDMP_2018  ,
# MAGIC   SUM(AGB_2018.esacciagb2018)  as esacciagb2018,        -- above ground biomass (AGB, unit: tons/ha i.e., Mg/ha) (raster dataset). 
# MAGIC   SUM(AGB_2018.esacciagb2018 *0.45)  as gpp_esacciagb2018,   ---- GPP = 45% of GDMP 
# MAGIC   SUM(AGB_2018.esacciagbsd2018)  as esacciagbsd2018,    -- per-pixel estimates of above-ground biomass uncertainty expressed as the standard deviation in Mg/ha (raster dataset)
# MAGIC   SUM(AGB_2018.esacciagb2018)  as AGB_biomass_t, -- check  
# MAGIC 'ESA CCI AGB 2018 for all LULUCF classes' as datasource
# MAGIC
# MAGIC from nuts3_2021
# MAGIC
# MAGIC LEFT JOIN AGB_2018     on nuts3_2021.GridNum = AGB_2018.GridNum
# MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC
# MAGIC where  nuts3_2021.LEVEL3_code is not null  -
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
# MAGIC   ,natura2000_protection
# MAGIC
# MAGIC """)
# MAGIC
# MAGIC tableDF_export_db_nuts3_agb1_FL_SL
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB/AGB_STOCK1_ESA_CCI2018_ALL")
# MAGIC
# MAGIC
# MAGIC     tableDF_export_db_nuts3_agb1_all.createOrReplaceTempView("AGB_STOCK1_ESA_CCI2018_for_ALL")

# COMMAND ----------


### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/AGB/AGB_STOCK1_ESA_CCI2018_ALL"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)
    



# COMMAND ----------

# MAGIC %md ### (2.5) BGB STOCK
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/bgb.JPG?raw=true?raw=true?raw=true)

# COMMAND ----------

# MAGIC %md #### (2.5,1) BGB STOCK for all LULUCF classes

# COMMAND ----------

# MAGIC %scala
# MAGIC /// exporting BGB stock 
# MAGIC val tableDF_export_db_nuts3_bgb1_all = spark.sql("""
# MAGIC
# MAGIC SELECT 
# MAGIC         
# MAGIC         nuts3_2021.Category, ----FOR ADMIN
# MAGIC         
# MAGIC         nuts3_2021.GridNum10km,
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
# MAGIC
# MAGIC
# MAGIC         SUM(nuts3_2021.AreaHa) as AreaHa,
# MAGIC
# MAGIC         SUM(BGB_forest_2020.FCM_Europe_demo_2020_BGB)  as FCM_Europe_demo_2020_BGB,        -- 
# MAGIC         SUM(BGB_forest_2020.FCM_Europe_demo_2020_BGB_SD)  as FCM_Europe_demo_2020_BGB_SD,    --  standard deviation 
# MAGIC
# MAGIC      ----   lULUCF_2018.LULUCF_CODE,
# MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
# MAGIC         if(OrganicSoils =2,'organic soils', if(OrganicSoils=1,'mineral soils','unknown soil')) as soil_type
# MAGIC       ---  natura2000_protection,
# MAGIC     ---    env_zones.Category as env_zones
# MAGIC
# MAGIC         
# MAGIC   ,if(LULUCF_CODE is null, 'none',LULUCF_CODE) as LULUCF_CODE
# MAGIC   ,if(env_zones.Category is null, 'none',env_zones.Category) as env_zones
# MAGIC   ,if(natura2000_protection is null, 'none Nature 2000 protection',natura2000_protection) as natura2000_protection
# MAGIC
# MAGIC
# MAGIC       from nuts3_2021
# MAGIC
# MAGIC       LEFT JOIN BGB_forest_2020     on nuts3_2021.GridNum = BGB_forest_2020.GridNum
# MAGIC       LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
# MAGIC       LEFT JOIN organic_soil on nuts3_2021.GridNum1km = organic_soil.GridNum  ------ 1km JOIN !!!!!!
# MAGIC       LEFT JOIN env_zones    on nuts3_2021.GridNum = env_zones.GridNum
# MAGIC LEFT JOIN Natura2000_100m_NET on nuts3_2021.GridNum = Natura2000_100m_NET.GridNum
# MAGIC       where  nuts3_2021.LEVEL3_code is not null  
# MAGIC  
# MAGIC       group by 
# MAGIC
# MAGIC         nuts3_2021.Category,
# MAGIC         nuts3_2021.GridNum10km,
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
# MAGIC         natura2000_protection,
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/BGB/BGB_STOCK1_ALL_2020")
# MAGIC
# MAGIC         tableDF_export_db_nuts3_bgb1_all.createOrReplaceTempView("BGB_STOCK_ALL_2018")

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/BGB/BGB_STOCK1_ALL_2020"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

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

      city_base_2021.gridnum, --10m
      city_base_2021.gridnum & cast(-65536 as bigint) as GridNum100m, ----  100m
      city_base_2021.gridnum & cast(-65536 as bigint) &  -16777216 as GridNUM1km, --- 1km --to be checked
      city_base_2021.GridNum10km,

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
