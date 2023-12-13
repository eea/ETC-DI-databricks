-- Databricks notebook source
-- MAGIC %md # Fragmentation - (2012) -2015 -2018
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ## 1) Reading DIMs

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
-- MAGIC //// (1) CLC and LUT-clc for lULUCF classes ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC // The following lines are reading the CLC 2018 DIMS and extracted the lULUCF classes:
-- MAGIC // Reading CLC2018 100m DIM:.....
-- MAGIC val parquetFileDF_clc18 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_A_CLC_18_210_20181129_100m/")
-- MAGIC parquetFileDF_clc18.createOrReplaceTempView("CLC_2018")
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
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (3) ENV zones (Metzger) ################################################################################                 100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC // https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1519&fileId=544&successMessage=true
-- MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_EnvZones_544_2020528_100m
-- MAGIC
-- MAGIC val parquetFileDF_env_zones = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_EnvZones_544_2020528_100m/")
-- MAGIC parquetFileDF_env_zones.createOrReplaceTempView("env_zones")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (4) Organic-mineral soils ---Tanneberger 2017 ###############################################################################   100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC //    https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1957&fileId=982
-- MAGIC //    cwsblobstorage01/cwsblob01/Dimensions/D_organicsoil_982_2023313_1km
-- MAGIC //      1 Mineral soils
-- MAGIC //      2 Organic soils (peatlands)
-- MAGIC
-- MAGIC val parquetFileDF_organic_soil = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_organicsoil_982_2023313_1km/")
-- MAGIC parquetFileDF_organic_soil.createOrReplaceTempView("organic_soil")
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (5) LCF ##############################################################################                 100m DIM
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC //-----------------------Protected area PA2022:...........................................................
-- MAGIC
-- MAGIC ///PA 100m
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
-- MAGIC //// Fragmentation ##############################################################################                  1km!!!!
-- MAGIC //##########################################################################################################################################
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2057&fileId=1079
-- MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_Fragment_meff1km_1079_20231211_1km
-- MAGIC //meff_2015_EEA38plusUK_1km_32bit_float
-- MAGIC //meff_2018_EEA38plusUK_1km_32bit_float
-- MAGIC
-- MAGIC
-- MAGIC val fragementation = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_Fragment_meff1km_1079_20231211_1km/")             /// use load
-- MAGIC fragementation.createOrReplaceTempView("fragementation_1km")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val parquetFileDF_frag_100m = spark.sql(""" 
-- MAGIC SELECT 
-- MAGIC         
-- MAGIC         nuts3_2021.GridNum, 
-- MAGIC       AVG(meff_2015_EEA38plusUK_1km_32bit_float) as meff2015,
-- MAGIC       AVG(meff_2018_EEA38plusUK_1km_32bit_float) as meff2018,
-- MAGIC
-- MAGIC       SUM(nuts3_2021.AreaHa) as AreaHa
-- MAGIC
-- MAGIC       from nuts3_2021
-- MAGIC LEFT JOIN fragementation_1km     on nuts3_2021.GridNum1km  = fragementation_1km.GridNum
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC    group by 
-- MAGIC
-- MAGIC     nuts3_2021.GridNum
-- MAGIC """)
-- MAGIC parquetFileDF_frag_100m.createOrReplaceTempView("fragementation_100m") 
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ## 2) Building CUBE

-- COMMAND ----------

select *from fragementation_100m

-- COMMAND ----------



SELECT 
        
        nuts3_2021.Category, ----FOR ADMIN
        nuts3_2021.GridNum10km,

        lULUCF_2018.LULUCF_DESCRIPTION,
        if(LULUCF_CODE is null, 'none',LULUCF_CODE) as LULUCF_CODE,


    
      MIN(meff2015) as meff2015_min,
      MIN(meff2018) as meff2018_min,
      MAX(meff2015) as meff2015_max,---
      SUM(meff2015) as meff2015_sum,
      SUM(meff2018) as meff2018_sum,
      AVG(meff2015) as meff2015_avg,
      AVG(meff2018) as meff2018_avg,

      SUM(nuts3_2021.AreaHa) as AreaHa

      from nuts3_2021
LEFT JOIN fragementation_100m     on nuts3_2021.GridNum  = fragementation_100m.GridNum
LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum

----where Category = 615 
 
   group by 

    nuts3_2021.Category,
    nuts3_2021.GridNum10km,
    lULUCF_2018.LULUCF_CODE,
    lULUCF_2018.LULUCF_DESCRIPTION
   

   



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC /// exporting cube
-- MAGIC val tableDF_export_frag = spark.sql("""
-- MAGIC
-- MAGIC
-- MAGIC SELECT 
-- MAGIC         
-- MAGIC         nuts3_2021.Category, ----FOR ADMIN
-- MAGIC         nuts3_2021.GridNum10km,
-- MAGIC
-- MAGIC         lULUCF_2018.LULUCF_DESCRIPTION,
-- MAGIC         if(LULUCF_CODE is null, 'none',LULUCF_CODE) as LULUCF_CODE,
-- MAGIC
-- MAGIC
-- MAGIC     
-- MAGIC       MIN(meff2015) as meff2015_min,
-- MAGIC       MIN(meff2018) as meff2018_min,
-- MAGIC       MAX(meff2015) as meff2015_max,---
-- MAGIC       MAX(meff2018) as meff2018_max,---
-- MAGIC       SUM(meff2015) as meff2015_sum,
-- MAGIC       SUM(meff2018) as meff2018_sum,
-- MAGIC       AVG(meff2015) as meff2015_avg,
-- MAGIC       AVG(meff2018) as meff2018_avg,
-- MAGIC
-- MAGIC       SUM(nuts3_2021.AreaHa) as AreaHa
-- MAGIC
-- MAGIC       from nuts3_2021
-- MAGIC LEFT JOIN fragementation_100m     on nuts3_2021.GridNum  = fragementation_100m.GridNum
-- MAGIC LEFT JOIN lULUCF_2018  on nuts3_2021.GridNum = lULUCF_2018.GridNum
-- MAGIC
-- MAGIC ----where Category = 615 
-- MAGIC  
-- MAGIC    group by 
-- MAGIC
-- MAGIC     nuts3_2021.Category,
-- MAGIC     nuts3_2021.GridNum10km,
-- MAGIC     lULUCF_2018.LULUCF_CODE,
-- MAGIC     lULUCF_2018.LULUCF_DESCRIPTION
-- MAGIC    
-- MAGIC
-- MAGIC
-- MAGIC """)
-- MAGIC tableDF_export_frag
-- MAGIC     .coalesce(1) //be careful with this
-- MAGIC     .write.format("com.databricks.spark.csv")
-- MAGIC     .mode(SaveMode.Overwrite)
-- MAGIC     .option("sep","|")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
-- MAGIC     .option("emptyValue", "")
-- MAGIC     .option("header","true")
-- MAGIC     .option("treatEmptyValuesAsNulls", "true")  
-- MAGIC     
-- MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/fragmentation/fragmentation201518")
-- MAGIC
-- MAGIC   tableDF_export_frag.createOrReplaceTempView("FRAG_2015_2018")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### Reading URL of resulting frag. table: (for downloading to EEA greenmonkey)
-- MAGIC folder =("dbfs:/mnt/trainingDatabricks/ExportTable/Carbon_mapping/fragmentation/fragmentation201518")
-- MAGIC folder_output =folder[29:]
-- MAGIC for file in dbutils.fs.ls(folder):
-- MAGIC     if file.name[-2:] =="gz":
-- MAGIC         print ("SOC:...................................")
-- MAGIC         print(file.name)
-- MAGIC         print ("Exported URL:")
-- MAGIC         URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
-- MAGIC         print (URL)

-- COMMAND ----------


