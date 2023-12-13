# Databricks notebook source
# MAGIC %md # Urban GREEN - statistics on urban green inside FUA 
# MAGIC
# MAGIC ![](https://github.com/eea/ETC-DI-databricks/blob/main/images/ua_2018_legend.PNG?raw=true)

# COMMAND ----------

# MAGIC %md ## 1) Reading DIMs
# MAGIC
# MAGIC check encoding:
# MAGIC https://docs.databricks.com/en/sql/language-manual/functions/encode.html
# MAGIC  .option("encoding", "UTF-16")  /// check ENCODING

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
# MAGIC //// (2) Urban Atlas 2018    ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1552&fileId=577
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_ua2018_10m_july20_577_2020818_10m
# MAGIC //D_UA_06_12_18_693_2021224_10m
# MAGIC //val parquetFileDF_ua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ua2018_10m_july20_577_2020818_10m/")             /// use load
# MAGIC //parquetFileDF_ua2018.createOrReplaceTempView("ua2018")
# MAGIC
# MAGIC val parquetFileDF_ua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_UA_06_12_18_693_2021224_10m/")             /// use load
# MAGIC parquetFileDF_ua2018.createOrReplaceTempView("ua2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //// FOREST 2018 based on Urban Atlas 10m: ########################################################################
# MAGIC val forest_ua_2018 = spark.sql(""" 
# MAGIC select 
# MAGIC     gridnum,
# MAGIC     Category2018 as UA_2018_class_code,
# MAGIC     areaHa,'forest' as forest_ua
# MAGIC     from ua2018
# MAGIC     where Category2018 =31000         
# MAGIC                                   """)
# MAGIC forest_ua_2018.createOrReplaceTempView("forest_ua_2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (4a ) Functional urban areas  2021 ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2018&fileId=1040
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_fua_urban_audit2021_1040_2023831_10m
# MAGIC
# MAGIC // FUA 2021:
# MAGIC
# MAGIC // https://cwsblobstorage01.blob.core.windows.net/cwsblob01/Lookups/LUT_FUA_2021_r/20230901110357.263.csv
# MAGIC
# MAGIC val parquetFileDF_fua = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_fua_urban_audit2021_1040_2023831_10m/") /// use load
# MAGIC parquetFileDF_fua.createOrReplaceTempView("fua_base")
# MAGIC
# MAGIC val schema_fua = new StructType()
# MAGIC .add("raster_val",IntegerType,true)
# MAGIC .add("URAU_CODE",StringType,true)
# MAGIC .add("URAU_CATG",StringType,true)
# MAGIC .add("URAU_CATG00",StringType,true)
# MAGIC .add("CNTR_CODE",StringType,true)
# MAGIC .add("FUA_NAME",StringType,true)
# MAGIC .add("FUA_CODE",StringType,true)
# MAGIC .add("AREA_SQM",FloatType,true)
# MAGIC .add("NUTS3_2021",StringType,true)
# MAGIC .add("SHRT_ENGL",StringType,true)
# MAGIC .add("ISO3_CODE",StringType,true)
# MAGIC .add("SVRG_UN",StringType,true)
# MAGIC .add("SHRT_FREN",StringType,true)
# MAGIC .add("CAPT",StringType,true)
# MAGIC .add("SHRT_GERM",StringType,true)
# MAGIC
# MAGIC
# MAGIC val LUT_fua  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_fua)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/LUT_FUA_2021_r/20230901110357.263.csv")
# MAGIC LUT_fua.createOrReplaceTempView("LUT_fua")
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and FUA information:---------------------------------------------
# MAGIC
# MAGIC
# MAGIC val fua_2021 = spark.sql(""" 
# MAGIC        select 
# MAGIC             x
# MAGIC             ,y
# MAGIC             ,gridnum
# MAGIC             ,GridNum10km
# MAGIC             ,urban_audit_2021_fua_10m_3035
# MAGIC             ,AreaHa
# MAGIC
# MAGIC             ,URAU_CODE
# MAGIC             ,raster_val
# MAGIC             ,FUA_NAME
# MAGIC             ,FUA_CODE
# MAGIC             ,CNTR_CODE
# MAGIC             ,NUTS3_2021
# MAGIC
# MAGIC           from fua_base
# MAGIC
# MAGIC           left join   LUT_fua on LUT_fua.raster_val=fua_base.urban_audit_2021_fua_10m_3035        
# MAGIC        
# MAGIC  
# MAGIC                                   """)
# MAGIC
# MAGIC fua_2021.createOrReplaceTempView("fua_2021")
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (4b ) Functional urban areas  2018- ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1712&fileId=737
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_fua_based_on_UA2018_737_2021427_10m
# MAGIC
# MAGIC // FUA LUT:
# MAGIC //https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=91
# MAGIC // cwsblobstorage01/cwsblob01/Lookups/FUA_urban_atlas/20210428134723.06.csv
# MAGIC
# MAGIC
# MAGIC val parquetFileDF_fua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_fua_based_on_UA2018_737_2021427_10m/") /// use load
# MAGIC parquetFileDF_fua2018.createOrReplaceTempView("fua_2018base")
# MAGIC
# MAGIC val schema_fua2018 = new StructType()
# MAGIC .add("fua_category",StringType,true)
# MAGIC .add("fua_code",StringType,true)
# MAGIC .add("country",StringType,true)
# MAGIC .add("fua_name",StringType,true)
# MAGIC .add("fua_code_ua",StringType,true)
# MAGIC .add("version",StringType,true)
# MAGIC .add("area_ha",FloatType,true)
# MAGIC val LUT_fua2018  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_fua2018)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/FUA_urban_atlas/20210428134723.06.csv")
# MAGIC LUT_fua2018.createOrReplaceTempView("LUT_fua2018")
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and FUA information:---------------------------------------------
# MAGIC
# MAGIC
# MAGIC val fua_2018= spark.sql(""" 
# MAGIC        select  
# MAGIC              fua_2018base.gridnum
# MAGIC             ,fua_2018base.GridNum10km
# MAGIC             ,fua_2018base.fua_ua2018_10m
# MAGIC             ,fua_2018base.AreaHa
# MAGIC
# MAGIC             ,LUT_fua2018.fua_category
# MAGIC             
# MAGIC             ,LUT_fua2018.country
# MAGIC             ,LUT_fua2018.FUA_CODE
# MAGIC             ,LUT_fua2018.fua_name
# MAGIC             ---,LUT_fua2018.version
# MAGIC             
# MAGIC
# MAGIC           from fua_2018base
# MAGIC
# MAGIC           left join   LUT_fua2018 on LUT_fua2018.fua_category=fua_2018base.fua_ua2018_10m    
# MAGIC        
# MAGIC  
# MAGIC                                   """)
# MAGIC
# MAGIC fua_2018.createOrReplaceTempView("fua_2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// 4 c) Updated FUA2021 with  AL, BA, ME, MK, RS, TR, XK. ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and FUA information:---------------------------------------------
# MAGIC val FUA2021_updated = spark.sql(""" 
# MAGIC   SELECT 
# MAGIC         gridnum,
# MAGIC         gridnum & cast(-65536 as bigint) as GridNum100m,
# MAGIC         GridNum10km,
# MAGIC         CNTR_CODE as country,
# MAGIC         FUA_CODE  as fua_code,
# MAGIC         FUA_NAME as fua_name ,
# MAGIC         Areaha
# MAGIC         from fua_2021
# MAGIC
# MAGIC                                """)
# MAGIC FUA2021_updated.createOrReplaceTempView("FUA2021_updated")
# MAGIC
# MAGIC val FUA2018_updated = spark.sql(""" 
# MAGIC    
# MAGIC       SELECT 
# MAGIC                 gridnum,
# MAGIC                 gridnum & cast(-65536 as bigint) as GridNum100m,
# MAGIC                 GridNum10km,
# MAGIC                 country,
# MAGIC                 FUA_CODE  as fua_code,
# MAGIC                 fua_name ,
# MAGIC                 Areaha
# MAGIC
# MAGIC                 from fua_2018 where country in ('AL', 'BA', 'ME', 'MK', 'RS', 'TR', 'XK')
# MAGIC
# MAGIC                                """)
# MAGIC FUA2018_updated.createOrReplaceTempView("FUA2018_updated")
# MAGIC
# MAGIC // union both: AL_BA_ME_MK_RS_TR_XK
# MAGIC
# MAGIC
# MAGIC val FUA2021_updated_AL_BA_ME_MK_RS_TR_XK = spark.sql(""" 
# MAGIC    
# MAGIC     SELECT * 
# MAGIC
# MAGIC             from FUA2021_updated
# MAGIC
# MAGIC             UNION ALL
# MAGIC
# MAGIC             SELECT 
# MAGIC             *
# MAGIC             from FUA2018_updated 
# MAGIC
# MAGIC
# MAGIC                                """)
# MAGIC FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.createOrReplaceTempView("FUA2021_updated_AL_BA_ME_MK_RS_TR_XK")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (5) city (core city) urban areas   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2024&fileId=1046&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_city_urban_audit2021_1046_2023922_10m
# MAGIC // CITY 2021:
# MAGIC // LUT: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=154
# MAGIC // cwsblobstorage01/cwsblob01/Lookups/LUT_CITY_2021_r/20230922142519.42.csv
# MAGIC
# MAGIC val parquetFileDF_city = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_city_urban_audit2021_1046_2023922_10m/")  /// use load
# MAGIC parquetFileDF_city.createOrReplaceTempView("city_base_2021")
# MAGIC
# MAGIC val schema_city = new StructType()
# MAGIC .add("ogc_fid",IntegerType,true)
# MAGIC .add("URAU_CODE",StringType,true)
# MAGIC .add("URAU_CATG",StringType,true)
# MAGIC .add("cntr_code",StringType,true)
# MAGIC .add("urau_name",StringType,true)
# MAGIC .add("city_cptl",StringType,true)
# MAGIC .add("fua_code",StringType,true)
# MAGIC .add("area_sqm",StringType,true)
# MAGIC .add("nuts3_2021",StringType,true)
# MAGIC .add("fid",IntegerType,true)
# MAGIC .add("_wgs84x",IntegerType,true)
# MAGIC .add("_wgs84y",IntegerType,true)
# MAGIC .add("_laeax",IntegerType,true)
# MAGIC .add("_laeay",IntegerType,true)
# MAGIC .add("city_code",StringType,true)
# MAGIC .add("raster_val",IntegerType,true)
# MAGIC val LUT_city  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_city)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/LUT_CITY_2021_r/20230922142519.42.csv")
# MAGIC LUT_city.createOrReplaceTempView("LUT_city2021")
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and FUA information:---------------------------------------------
# MAGIC val city_2021 = spark.sql(""" 
# MAGIC      select 
# MAGIC       city_base_2021.gridnum,
# MAGIC       city_base_2021.urban_audit_2021_city_10m,
# MAGIC       city_base_2021.AreaHa,
# MAGIC
# MAGIC       LUT_city2021.URAU_CODE,
# MAGIC
# MAGIC       LUT_city2021.URAU_CODE,
# MAGIC       LUT_city2021.URAU_CATG,
# MAGIC       LUT_city2021.urau_name,
# MAGIC       LUT_city2021.fua_code,
# MAGIC       LUT_city2021.nuts3_2021,
# MAGIC       LUT_city2021.city_code
# MAGIC           from city_base_2021
# MAGIC            left join   LUT_city2021 on LUT_city2021.raster_val=city_base_2021.urban_audit_2021_city_10m   
# MAGIC
# MAGIC                                  """)
# MAGIC city_2021.createOrReplaceTempView("city_2021")
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (5) tree-cover density   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1669&fileId=694&successMessage=true
# MAGIC //cwsblobstorage01/cwsblob01/D_TreeCoverDens201810m_694_202134_10m/
# MAGIC
# MAGIC
# MAGIC val treecover2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_TreeCoverDens201810m_694_202134_10m//")             /// use load
# MAGIC treecover2018.createOrReplaceTempView("treecover2018")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (6) STREET tree-layer 10m  ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1683&fileId=708
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_StreetTreeUA2018_708_2021326_10m
# MAGIC
# MAGIC
# MAGIC val street_tree = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_StreetTreeUA2018_708_2021326_10m//")             /// use load
# MAGIC street_tree.createOrReplaceTempView("street_tree")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (7) LAU 10m  ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1885&fileId=910
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_LAU2020_10m_910_202298_10m
# MAGIC
# MAGIC
# MAGIC //LUT: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=140
# MAGIC //LUT: cwsblobstorage01/cwsblob01/Lookups/LAU2020_10m_attr/20220913154909.45.csv
# MAGIC
# MAGIC val parquetFileDF_lau = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_LAU2020_10m_910_202298_10m/")  /// use load
# MAGIC parquetFileDF_lau.createOrReplaceTempView("lau_2020_base")
# MAGIC
# MAGIC val schema_lau = new StructType()
# MAGIC .add("OID_",IntegerType,true)
# MAGIC .add("objectid",IntegerType,true)
# MAGIC .add("GISCO_ID",StringType,true)
# MAGIC .add("CNTR_CODE",StringType,true)
# MAGIC .add("LAU_ID",StringType,true)
# MAGIC .add("LAU_NAME",StringType,true)
# MAGIC .add("POP_2020",IntegerType,true)
# MAGIC .add("POP_DENS_2",FloatType,true)
# MAGIC .add("AREA_KM2",FloatType,true)
# MAGIC .add("YEAR",IntegerType,true)
# MAGIC .add("SHAPE_Leng",FloatType,true)
# MAGIC .add("SHAPE_Area",FloatType,true)
# MAGIC .add("fme_featur",StringType,true)
# MAGIC val LUT_lau  = spark.read.format("csv")
# MAGIC  .options(Map("delimiter"->"|"))
# MAGIC  .schema(schema_lau)
# MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups//LAU2020_10m_attr/20220913154909.45.csv")
# MAGIC LUT_lau.createOrReplaceTempView("LUT_lau2020")
# MAGIC
# MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and FUA information:---------------------------------------------
# MAGIC val lau_2020 = spark.sql(""" 
# MAGIC         SELECT 
# MAGIC         lau_2020_base.gridnum
# MAGIC         ,lau_2020_base.gridnum & cast(-65536 as bigint) as GridNum100m
# MAGIC         ,lau_2020_base.LAU2020_10m
# MAGIC         ,lau_2020_base.AreaHa
# MAGIC         ,LUT_lau2020.GISCO_ID
# MAGIC         ,LUT_lau2020.CNTR_CODE
# MAGIC         ,LUT_lau2020.LAU_ID
# MAGIC         ,LUT_lau2020.LAU_NAME
# MAGIC         ,LUT_lau2020.POP_2020
# MAGIC         ,LUT_lau2020.POP_DENS_2
# MAGIC         ,LUT_lau2020.AREA_KM2
# MAGIC         ,LUT_lau2020.YEAR
# MAGIC         from lau_2020_base
# MAGIC         LEFT JOIN LUT_lau2020 on lau_2020_base.LAU2020_10m = LUT_lau2020.objectid
# MAGIC                                """)
# MAGIC lau_2020.createOrReplaceTempView("lau_2020")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC //##########################################################################################################################################
# MAGIC //// (10) Imperviousness 2018 - version 2018 010m  ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC // https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1608&fileId=633
# MAGIC // cwsblobstorage01/cwsblob01/Dimensions/D_IMD2018_2018v_633_2021114_100m
# MAGIC
# MAGIC
# MAGIC val soil_sealing_2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_IMD2018_2018v_633_2021114_100m//")             /// use load
# MAGIC soil_sealing_2018.createOrReplaceTempView("soil_sealing_2018")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ## 2) Testing DIMs & Calculations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from street_tree
# MAGIC

# COMMAND ----------

# MAGIC %sql --- testing
# MAGIC --select 
# MAGIC --fua_code,
# MAGIC --CNTR_CODE,
# MAGIC --fua_name,
# MAGIC --(fua_2021.AreaHa) as areaha,
# MAGIC --treecover2018.category,
# MAGIC --(if(treecover2018.category>0,treecover2018.category/100,0)) as tree_area_ha_2018
# MAGIC ----100/sum(fua_ua.AreaHa) *sum(if(treecover2018.category>0,treecover2018.category/100,0))*0.00 as tree_percent
# MAGIC --
# MAGIC --from fua_2021
# MAGIC --
# MAGIC --left join treecover2018 on treecover2018.gridnum= fua_2021.gridnum
# MAGIC --
# MAGIC --where fua_2021.CNTR_CODE = 'LU' 
# MAGIC -- and treecover2018.category = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from nuts3_2021

# COMMAND ----------

# MAGIC %md ## 3) Urban indicators

# COMMAND ----------

# MAGIC %md ### 3.1) Tree-cover density by FUA (version 2021) 

# COMMAND ----------

# MAGIC %md
# MAGIC the following box calculatet the Tree-cover inside every FUA:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC the next box builded a urban-fua cube with the combination of Urban Atlas forest, Urban-street tree layer and TreecoverDensity:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // This box constructed the new Urban FUA TREE CUBE
# MAGIC
# MAGIC val tree_cube_fua = spark.sql(""" 
# MAGIC   select 
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum10km,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa as AreaHa,
# MAGIC
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name as city_name,
# MAGIC       lau_2020.LAU_ID as lau_code,
# MAGIC       lau_2020.LAU_NAME as lau_name,
# MAGIC       lau_2020.GISCO_ID, --- GISCO ID is needed!!!!!!!!!!!!!!
# MAGIC
# MAGIC       treecover2018.category /10000 as Tree_area_HRL_ha,
# MAGIC       street_tree.areaHa  as Tree_area_Street_ha,
# MAGIC       forest_ua_2018.areaHa  as Tree_area_UA18_forest_ha,
# MAGIC
# MAGIC       if(treecover2018.category>0, 0.01, if(street_tree.areaHa>0,0.01, if(forest_ua_2018.areaHa >0, 0.01,0))) as TREE_area_ha
# MAGIC
# MAGIC
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC
# MAGIC       left join treecover2018 on treecover2018.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   --value
# MAGIC       left join street_tree on street_tree.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum    -- value
# MAGIC       left join forest_ua_2018 on forest_ua_2018.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum -- value
# MAGIC
# MAGIC       LEFT JOIN city_2021 ON city_2021.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---CITY core-city outline
# MAGIC       LEFT JOIN lau_2020 ON lau_2020.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---lau  outline
# MAGIC
# MAGIC
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC       
# MAGIC                                     """)
# MAGIC
# MAGIC       tree_cube_fua.createOrReplaceTempView("tree_cube_fua")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val tree_fua = spark.sql(""" 
# MAGIC             select 
# MAGIC             fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             GISCO_ID, --- GISCO ID is needed!!!!!!!!!!!!!!
# MAGIC             lau_name,
# MAGIC             sum(AreaHa) as AreaHa,
# MAGIC             sum(Tree_area_HRL_ha) as Tree_area_HRL_ha,
# MAGIC             sum(Tree_area_Street_ha) as Tree_area_Street_ha,
# MAGIC             sum(Tree_area_UA18_forest_ha) as  Tree_area_UA18_forest_ha,
# MAGIC             sum(TREE_area_ha) as  TREE_area_ha,
# MAGIC             100.00/SUM(AreaHa) * SUM(TREE_area_ha) as Tree_area_UA18_forest_percent
# MAGIC
# MAGIC             from tree_cube_fua
# MAGIC             where fua_code is not null
# MAGIC             group by fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             GISCO_ID, 
# MAGIC             lau_name
# MAGIC                                   """)
# MAGIC
# MAGIC tree_fua
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_tree_cover1")
# MAGIC
# MAGIC     tree_fua.createOrReplaceTempView("tree_fua_indicator")
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_tree_cover1"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %md ### 3.2) Green Urban Areas - based on Urban Atlas 2018 and other data sources

# COMMAND ----------

# MAGIC %md
# MAGIC Urban Green Infrastructure, 2018
# MAGIC
# MAGIC Green infrastructure in urban areas consist of vegetated green surfaces, such as parks, trees and small forests, grasslands, but also private gardens or cemeteries. These all contribute to supporting biodiversity, pollinators, carbon sequestration, flood protection and protection against excess heats events. This dashboard facilitates the understanding of the amount of urban green in Functional Urban Areas of the EU and EEA member states.
# MAGIC
# MAGIC https://www.eea.europa.eu/data-and-maps/dashboards/urban-green-infrastructure-2018
# MAGIC
# MAGIC
# MAGIC https://jedi.discomap.eea.europa.eu/Cube/Show/218
# MAGIC
# MAGIC   Green Infrastructure Elements (10m x 10m)
# MAGIC
# MAGIC   Green Infrastructure Hubs (10m x 10m)
# MAGIC   
# MAGIC   Green Infrastructure Links (10m x 10m)
# MAGIC
# MAGIC   Green Infrastructure Physical Network (10m x 10m)
# MAGIC   
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC     gridnum,
# MAGIC     Category2018 as UA_2018_class_code,
# MAGIC     areaHa,
# MAGIC     if(Category2018 in ())
# MAGIC     'green_urban_areas' as urban_indicator
# MAGIC
# MAGIC     from ua2018
# MAGIC     where Category2018 =31000    
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### 3.3) Blue Urban Areas - bases on Urban Atlas 2018

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC     gridnum,
# MAGIC     Category2018 as UA_2018_class_code,
# MAGIC     areaHa,
# MAGIC     if(Category2018 in (50000), areaHa, 0) as urban_blue_indicator
# MAGIC
# MAGIC     from ua2018
# MAGIC     where Category2018 =50000    

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // This box constructed the new Urban FUA TREE CUBE
# MAGIC
# MAGIC val blue_cube_fua = spark.sql(""" 
# MAGIC   select 
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum10km,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa as AreaHa,
# MAGIC
# MAGIC       city_2021.city_code,
# MAGIC       city_2021.urau_name as city_name,
# MAGIC       lau_2020.LAU_ID as lau_code,
# MAGIC       lau_2020.LAU_NAME as lau_name,
# MAGIC       lau_2020.GISCO_ID, --- GISCO ID is needed!!!!!!!!!!!!!!
# MAGIC
# MAGIC       if(ua2018.Category2018 in (50000), FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa, 0) as urban_blue_indicator_2018
# MAGIC
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC       LEFT JOIN city_2021 ON city_2021.gridnum= FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---CITY core-city outline
# MAGIC       LEFT JOIN lau_2020 ON lau_2020.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---lau  outline
# MAGIC       LEFT JOIN ua2018 ON ua2018.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum   ---lau  outline
# MAGIC
# MAGIC
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC       
# MAGIC                                     """)
# MAGIC
# MAGIC       blue_cube_fua.createOrReplaceTempView("bluecube_fua")
# MAGIC
# MAGIC
# MAGIC val blue_c_cube_fua = spark.sql(""" 
# MAGIC             select 
# MAGIC             fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             GISCO_ID,
# MAGIC             lau_name,
# MAGIC             sum(AreaHa) as AreaHa,
# MAGIC             sum(urban_blue_indicator_2018) as urban_blue_indicator_2018
# MAGIC             from bluecube_fua
# MAGIC             where fua_code is not null
# MAGIC             group by fua_name,
# MAGIC             fua_code,
# MAGIC             country,
# MAGIC             city_code,
# MAGIC             city_name,
# MAGIC             lau_code,
# MAGIC             GISCO_ID,
# MAGIC             lau_name
# MAGIC                                   """)
# MAGIC
# MAGIC blue_c_cube_fua
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_blue")
# MAGIC
# MAGIC     blue_c_cube_fua.createOrReplaceTempView("blue_c_cube_fua")
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_blue"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from bluecube_fua

# COMMAND ----------

# MAGIC %md ### 3.4) Soil sealing- 2018

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC Category/100.00 as soil_sealing_area_ha
# MAGIC --max(Category) as Max
# MAGIC --,min(Category) as Min
# MAGIC
# MAGIC from soil_sealing_2018 ---100m DIM
# MAGIC  
# MAGIC where 
# MAGIC Category > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- testing FUA 10m
# MAGIC select 
# MAGIC       --FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum,
# MAGIC       --FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum100m,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country,
# MAGIC       (FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa) as AreaHa,
# MAGIC       (soil_sealing_2018.Category/100.00)/100 as soil_sealing_area_ha
# MAGIC
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC       LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum100m   
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- testing FUA 10m
# MAGIC select 
# MAGIC       --FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.gridnum,
# MAGIC       --FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum100m,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country,
# MAGIC       SUM(FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa) as AreaHa,
# MAGIC       SUM(soil_sealing_2018.Category/100.00)*100 as soil_sealing_area_ha
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC       LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum100m   
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC       group  by 
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from lau_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC -- testing LAU
# MAGIC select 
# MAGIC    
# MAGIC       lau_2020.LAU_ID as lau_code,
# MAGIC       lau_2020.LAU_NAME as lau_name,
# MAGIC       lau_2020.GISCO_ID,
# MAGIC       SUM(lau_2020.AreaHa) as AreaHa,
# MAGIC       SUM(soil_sealing_2018.Category/100.00/100.00) as soil_sealing_area_ha,
# MAGIC       SUM(POP_2020) as population_2020
# MAGIC       from lau_2020 
# MAGIC
# MAGIC       LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.gridnum = lau_2020.GridNum100m   
# MAGIC       
# MAGIC       where lau_2020.GISCO_ID is not null
# MAGIC
# MAGIC       group by 
# MAGIC
# MAGIC             lau_2020.LAU_ID,
# MAGIC             lau_2020.LAU_NAME,
# MAGIC             lau_2020.GISCO_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC show columns   from nuts3_2021
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- testing NUTS
# MAGIC select 
# MAGIC  ADM_COUNTRY
# MAGIC , ISO2
# MAGIC , LEVEL3_name
# MAGIC , LEVEL2_name
# MAGIC , LEVEL1_name
# MAGIC , LEVEL0_name
# MAGIC , LEVEL3_code
# MAGIC , LEVEL2_code
# MAGIC , LEVEL1_code
# MAGIC , LEVEL0_code
# MAGIC , EEA38_2020
# MAGIC , EEA39
# MAGIC , EU27_2020
# MAGIC , EU28
# MAGIC , EU27_2007
# MAGIC , EFTA4
# MAGIC , NUTS_EU
# MAGIC , TAA
# MAGIC ,SUM(soil_sealing_2018.Category/100.00) as soil_sealing_area_ha
# MAGIC ,SUM(nuts3_2021.AreaHa) as Areaha
# MAGIC       
# MAGIC       from nuts3_2021 
# MAGIC
# MAGIC       LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.GridNum = nuts3_2021.GridNum   
# MAGIC       
# MAGIC       where nuts3_2021.ISO2 is not null
# MAGIC
# MAGIC group by 
# MAGIC  ADM_COUNTRY
# MAGIC , ISO2
# MAGIC , LEVEL3_name
# MAGIC , LEVEL2_name
# MAGIC , LEVEL1_name
# MAGIC , LEVEL0_name
# MAGIC , LEVEL3_code
# MAGIC , LEVEL2_code
# MAGIC , LEVEL1_code
# MAGIC , LEVEL0_code
# MAGIC , EEA38_2020
# MAGIC , EEA39
# MAGIC , EU27_2020
# MAGIC , EU28
# MAGIC , EU27_2007
# MAGIC , EFTA4
# MAGIC , NUTS_EU
# MAGIC , TAA
# MAGIC
# MAGIC   

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // This box constructed the new  soil sealing 
# MAGIC //---------------------------------------------------------------------------------------------------------------------------------
# MAGIC // CUBE (1) FUA
# MAGIC val fua_soilsealing2018 = spark.sql(""" 
# MAGIC   
# MAGIC select 
# MAGIC  
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country,
# MAGIC       SUM(FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.AreaHa) as AreaHa,
# MAGIC       SUM(soil_sealing_2018.Category/100.00/100.00) as soil_sealing_area_ha
# MAGIC       from FUA2021_updated_AL_BA_ME_MK_RS_TR_XK -- new fua
# MAGIC       LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.gridnum = FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.GridNum100m   
# MAGIC       where FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country is not null
# MAGIC       group  by 
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_name,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.fua_code,
# MAGIC       FUA2021_updated_AL_BA_ME_MK_RS_TR_XK.country
# MAGIC       
# MAGIC                                     """)
# MAGIC fua_soilsealing2018
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/fua_soilsealing2018")
# MAGIC      ///fua_soilsealing2018.createOrReplaceTempView("fua_soilsealing2018")
# MAGIC
# MAGIC
# MAGIC //---------------------------------------------------------------------------------------------------------------------------------
# MAGIC
# MAGIC
# MAGIC // CUBE (2) LAU 
# MAGIC val lau_soilsealing2018 = spark.sql(""" 
# MAGIC   
# MAGIC select 
# MAGIC    
# MAGIC       lau_2020.LAU_ID as lau_code,
# MAGIC       lau_2020.LAU_NAME as lau_name,
# MAGIC       lau_2020.GISCO_ID,
# MAGIC       SUM(lau_2020.AreaHa) as AreaHa,
# MAGIC       SUM(soil_sealing_2018.Category/100.00 /100.00) as soil_sealing_area_ha,
# MAGIC       SUM(POP_2020) as population_2020
# MAGIC       from lau_2020 
# MAGIC
# MAGIC       LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.gridnum = lau_2020.GridNum100m   
# MAGIC       
# MAGIC       where lau_2020.GISCO_ID is not null
# MAGIC
# MAGIC       group by 
# MAGIC
# MAGIC             lau_2020.LAU_ID,
# MAGIC             lau_2020.LAU_NAME,
# MAGIC             lau_2020.GISCO_ID
# MAGIC       
# MAGIC                                     """)
# MAGIC lau_soilsealing2018
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/lau_soilsealing2018")
# MAGIC      ///lau_soilsealing2018.createOrReplaceTempView("lau_soilsealing2018")
# MAGIC      //---------------------------------------------------------------------------------------------------------------------------------
# MAGIC //
# MAGIC //
# MAGIC //
# MAGIC //// CUBE (3) NUTS 
# MAGIC val nuts_soilsealing2018 = spark.sql(""" 
# MAGIC
# MAGIC             select 
# MAGIC              ADM_COUNTRY
# MAGIC             , ISO2
# MAGIC             , LEVEL3_name
# MAGIC             , LEVEL2_name
# MAGIC             , LEVEL1_name
# MAGIC             , LEVEL0_name
# MAGIC             , LEVEL3_code
# MAGIC             , LEVEL2_code
# MAGIC             , LEVEL1_code
# MAGIC             , LEVEL0_code
# MAGIC             , EEA38_2020
# MAGIC             , EEA39
# MAGIC             , EU27_2020
# MAGIC             , EU28
# MAGIC             , EU27_2007
# MAGIC             , EFTA4
# MAGIC             , NUTS_EU
# MAGIC             , TAA
# MAGIC             ,SUM(soil_sealing_2018.Category/100.00) as soil_sealing_area_ha
# MAGIC             ,SUM(nuts3_2021.AreaHa) as Areaha
# MAGIC                   
# MAGIC                   from nuts3_2021 
# MAGIC
# MAGIC                   LEFT JOiN soil_sealing_2018 on  soil_sealing_2018.GridNum = nuts3_2021.GridNum   
# MAGIC                   
# MAGIC                   where nuts3_2021.ISO2 is not null
# MAGIC
# MAGIC             group by 
# MAGIC             ADM_COUNTRY
# MAGIC             , ISO2
# MAGIC             , LEVEL3_name
# MAGIC             , LEVEL2_name
# MAGIC             , LEVEL1_name
# MAGIC             , LEVEL0_name
# MAGIC             , LEVEL3_code
# MAGIC             , LEVEL2_code
# MAGIC             , LEVEL1_code
# MAGIC             , LEVEL0_code
# MAGIC             , EEA38_2020
# MAGIC             , EEA39
# MAGIC             , EU27_2020
# MAGIC             , EU28
# MAGIC             , EU27_2007
# MAGIC             , EFTA4
# MAGIC             , NUTS_EU
# MAGIC             , TAA
# MAGIC
# MAGIC             
# MAGIC
# MAGIC       
# MAGIC                                     """)
# MAGIC nuts_soilsealing2018
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
# MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/nuts_soilsealing2018")
# MAGIC      ///nuts_soilsealing2018.createOrReplaceTempView("nuts_soilsealing2018")
# MAGIC      //---------------------------------------------------------------------------------------------------------------------------------
# MAGIC //
# MAGIC
# MAGIC

# COMMAND ----------

### Reading URL of resulting table: (for downloading to EEA greenmonkey)

#### CUBE 1:################################################## FUA:
print ("CUBE- 1 :----------------------------------------------------------")
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/fua_soilsealing2018"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

#### CUBE 2:################################################## LAU:
print ("CUBE- 2 :----------------------------------------------------------")
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/lau_soilsealing2018"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)

#### CUBE 3:################################################## NUTS:
print ("CUBE- 3 :----------------------------------------------------------")
folder ="dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/nuts_soilsealing2018"
folder_output =folder[29:]
for file in dbutils.fs.ls(folder):
    if file.name[-2:] =="gz":
        print ("Exported file:")
        print(file.name)
        print ("Exported URL:")
        URL = "https://cwsblobstorage01.blob.core.windows.net/cwsblob01"+"/"+folder_output +"/"+file.name
        print (URL)
