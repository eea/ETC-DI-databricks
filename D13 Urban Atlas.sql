-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## This is the Urban workspace 
-- MAGIC ##...cities are great
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md # 1. Read dims with scala
-- MAGIC THIS BOX reads all Dimensions (DIM) and Lookuptables (LUT) that are needed for the NUTS3 and 10x10km GRID statistics
-- MAGIC info: loehnertz@space4environment.com

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
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (2) Urban Atlas 2018    ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1552&fileId=577
-- MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_ua2018_10m_july20_577_2020818_10m
-- MAGIC //D_UA_06_12_18_693_2021224_10m
-- MAGIC //val parquetFileDF_ua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ua2018_10m_july20_577_2020818_10m/")             /// use load
-- MAGIC //parquetFileDF_ua2018.createOrReplaceTempView("ua2018")
-- MAGIC
-- MAGIC val parquetFileDF_ua2018 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_UA_06_12_18_693_2021224_10m/")             /// use load
-- MAGIC parquetFileDF_ua2018.createOrReplaceTempView("ua2018")
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (3) CLC pro Backbone (BB) 2018    ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC //#cwsblobstorage01/cwsblob01/Dimensions/D_CLCplus2018bb_965_202329_10m
-- MAGIC //#https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1940&fileId=965
-- MAGIC
-- MAGIC //#Value    Class_name
-- MAGIC //#1	-	Sealed
-- MAGIC //#2	-	Woody needle leaved trees
-- MAGIC //#3	-	Woody Broadleaved deciduous trees
-- MAGIC //#4	-	Woody Broadleaved evergreen trees
-- MAGIC //#5	-	Low-growing woody plants
-- MAGIC //#6	-	Permanent herbaceous
-- MAGIC //#7	-	Periodically herbaceous
-- MAGIC //#8	-	Lichens and mosses
-- MAGIC //#9	-	Non and sparsely vegetated
-- MAGIC //#10	-	Water
-- MAGIC //#11	-	Snow and ice
-- MAGIC //#254	-	Outside area
-- MAGIC //#255	-	No data
-- MAGIC
-- MAGIC val parquetFileDF_clc_plus_bb = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_CLCplus2018bb_965_202329_10m/")             /// use load
-- MAGIC parquetFileDF_clc_plus_bb.createOrReplaceTempView("clc_plus_bb_2018")
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (4) Functional urban areas based on Urban Atlas 2018   ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC //#https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1712&fileId=737
-- MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_fua_based_on_UA2018_737_2021427_10m
-- MAGIC
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=91
-- MAGIC
-- MAGIC val parquetFileDF_fua = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_fua_based_on_UA2018_737_2021427_10m/")             /// use load
-- MAGIC parquetFileDF_fua.createOrReplaceTempView("fua")
-- MAGIC
-- MAGIC val schema_fua = new StructType()
-- MAGIC .add("fua_category",LongType,true)
-- MAGIC .add("fua_code",StringType,true)
-- MAGIC .add("country",StringType,true)
-- MAGIC .add("fua_name",StringType,true)
-- MAGIC .add("fua_code_ua",StringType,true)
-- MAGIC .add("version",StringType,true)
-- MAGIC .add("area_ha",LongType,true)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val LUT_fua  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_fua)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/FUA_urban_atlas/20210428134723.06.csv")
-- MAGIC LUT_fua.createOrReplaceTempView("LUT_fua")
-- MAGIC
-- MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and NUTS information:---------------------------------------------
-- MAGIC val fua_ua = spark.sql(""" 
-- MAGIC            
-- MAGIC select 
-- MAGIC fua.gridnum,
-- MAGIC fua.fua_ua2018_10m,
-- MAGIC fua.AreaHa,
-- MAGIC fua.GridNum10km,
-- MAGIC LUT_fua.fua_category,
-- MAGIC LUT_fua.fua_code	,
-- MAGIC LUT_fua.country	,
-- MAGIC LUT_fua.fua_name	,
-- MAGIC LUT_fua.fua_code_ua	
-- MAGIC
-- MAGIC FROM fua 
-- MAGIC   LEFT JOIN LUT_fua  ON fua.fua_ua2018_10m = LUT_fua.fua_category  
-- MAGIC   
-- MAGIC        
-- MAGIC  
-- MAGIC                                   """)
-- MAGIC
-- MAGIC fua_ua.createOrReplaceTempView("fua_ua")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (5) ore city area based on UrbanAtlas 2018   ################################################################################
-- MAGIC //##########################################################################################################################################
-- MAGIC
-- MAGIC
-- MAGIC ///https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1711&fileId=736
-- MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_core_city_ua2018_736_2021427_10m
-- MAGIC // LUT: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=92 
-- MAGIC // LUT: cwsblobstorage01/cwsblob01/Lookups/core_city_urb_atl18/20210429102501.03.csv
-- MAGIC
-- MAGIC val parquetFileDF_city = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_core_city_ua2018_736_2021427_10m/")             /// use load
-- MAGIC parquetFileDF_city.createOrReplaceTempView("city")
-- MAGIC
-- MAGIC val schema_city = new StructType()
-- MAGIC .add("cc_category",LongType,true)
-- MAGIC .add("cc_code",StringType,true)
-- MAGIC .add("URAU_CODE",StringType,true)
-- MAGIC .add("URAU_CATG",StringType,true)
-- MAGIC .add("CNTR_CODE",StringType,true)
-- MAGIC .add("URAU_NAME",StringType,true)
-- MAGIC .add("fua_code",StringType,true)
-- MAGIC .add("fua_name",StringType,true)
-- MAGIC .add("fua_short",StringType,true)
-- MAGIC .add("only_cc",StringType,true)
-- MAGIC .add("source",StringType,true)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val LUT_city  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_city)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups//core_city_urb_atl18/20210429102501.03.csv")
-- MAGIC LUT_city.createOrReplaceTempView("LUT_city")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC /// the following lines constructed a new admin table wiht GRIDNUM and NUTS information:---------------------------------------------
-- MAGIC val city_ua = spark.sql(""" 
-- MAGIC            
-- MAGIC select 
-- MAGIC city.gridnum,
-- MAGIC city.core_city_10m,
-- MAGIC city.AreaHa,
-- MAGIC city.GridNum10km,
-- MAGIC
-- MAGIC LUT_city.cc_category,
-- MAGIC LUT_city.cc_code,
-- MAGIC LUT_city.URAU_NAME as city_name	,
-- MAGIC LUT_city.fua_code	,
-- MAGIC LUT_city.fua_name	,
-- MAGIC LUT_city.only_cc	,
-- MAGIC LUT_city.CNTR_CODE	
-- MAGIC
-- MAGIC FROM city 
-- MAGIC   LEFT JOIN LUT_city  ON city.core_city_10m = LUT_city.cc_category 
-- MAGIC   
-- MAGIC        
-- MAGIC  
-- MAGIC                                   """)
-- MAGIC city_ua.createOrReplaceTempView("city_ua")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Building a CUBE

-- COMMAND ----------

---city indicator: ua-classes vs. clc-plus inside the core city:
Select 
city_ua.cc_code,
city_ua.city_name,
CONCAT("clc_plus_",CLMS_CLCplus_RASTER_2018_010m_eu_03035) as CLCplus_code,  --- class code clc plus
ua2018.Category2018 as ua18_code,

SUM(city_ua.AreaHa) /100  as Areakm2

from city_ua
left JOIN clc_plus_bb_2018 on city_ua.gridnum = clc_plus_bb_2018.gridnum
left JOIN ua2018           on city_ua.gridnum = ua2018.gridnum


---where city_ua.cc_code is not NULL
where city_ua.cc_code ='AT001'
group by 

city_ua.cc_code,
city_ua.city_name,
CLMS_CLCplus_RASTER_2018_010m_eu_03035,
ua2018.Category2018



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2 Testing DIMS and Calculations

-- COMMAND ----------

---FUA
Select * from fua_ua
where country ='LU'



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## EXPORT results

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC
-- MAGIC // Exporting the final table  ---city indicator: ua-classes vs. clc-plus inside the core city:
-- MAGIC val table_export_ua_clcplusbb = spark.sql("""
-- MAGIC
-- MAGIC         Select 
-- MAGIC         city_ua.cc_code,
-- MAGIC         city_ua.city_name,
-- MAGIC         CONCAT("clc_plus_",CLMS_CLCplus_RASTER_2018_010m_eu_03035) as CLCplus_code,  --- class code clc plus
-- MAGIC         ua2018.Category2018 as ua18_code,
-- MAGIC
-- MAGIC         SUM(city_ua.AreaHa)  as AreaHa
-- MAGIC
-- MAGIC         from city_ua
-- MAGIC         left JOIN clc_plus_bb_2018 on city_ua.gridnum = clc_plus_bb_2018.gridnum
-- MAGIC         left JOIN ua2018           on city_ua.gridnum = ua2018.gridnum
-- MAGIC
-- MAGIC
-- MAGIC         where city_ua.cc_code is not NULL
-- MAGIC         
-- MAGIC         group by 
-- MAGIC
-- MAGIC         city_ua.cc_code,
-- MAGIC         city_ua.city_name,
-- MAGIC         CLMS_CLCplus_RASTER_2018_010m_eu_03035,
-- MAGIC         ua2018.Category2018
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC             """)
-- MAGIC table_export_ua_clcplusbb
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
-- MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/UA_clcplus")
-- MAGIC
