# Databricks notebook source
# MAGIC %md # Urban GREEN - statistics on urban green inside FUA 
# MAGIC
# MAGIC ![](https://space4environment.com/fileadmin/Resources/Public/Images/Logos/S4E-Logo.png)

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
# MAGIC //// (4) Functional urban areas   ################################################################################
# MAGIC //##########################################################################################################################################
# MAGIC
# MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=2018&fileId=1040
# MAGIC //cwsblobstorage01/cwsblob01/Dimensions/D_fua_urban_audit2021_1040_2023831_10m
# MAGIC
# MAGIC // FUA 2021:
# MAGIC
# MAGIC // https://cwsblobstorage01.blob.core.windows.net/cwsblob01/Lookups/LUT_FUA_2021_r/20230901110357.263.csv
# MAGIC
# MAGIC val parquetFileDF_fua = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_fua_urban_audit2021_1040_2023831_10m/")             /// use load
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

# COMMAND ----------

# MAGIC %md ## 2) Testing DIMs & Calculations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from street_tree
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC gridnum,Category2018,areaHa,'forest' as forest_ua
# MAGIC  from ua2018
# MAGIC
# MAGIC where Category2018 =31000
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from forest_ua_2018

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC fua_code,
# MAGIC CNTR_CODE,
# MAGIC fua_name,
# MAGIC (fua_2021.AreaHa) as areaha,
# MAGIC treecover2018.category,
# MAGIC (if(treecover2018.category>0,treecover2018.category/100,0)) as tree_area_ha_2018
# MAGIC --100/sum(fua_ua.AreaHa) *sum(if(treecover2018.category>0,treecover2018.category/100,0))*0.00 as tree_percent
# MAGIC
# MAGIC from fua_2021
# MAGIC
# MAGIC left join treecover2018 on treecover2018.gridnum= fua_2021.gridnum
# MAGIC
# MAGIC where fua_2021.CNTR_CODE = 'LU' 
# MAGIC  and treecover2018.category = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --- fua area look OK
# MAGIC select
# MAGIC FUA_CODE,
# MAGIC FUA_NAME,
# MAGIC CNTR_CODE, 
# MAGIC sum(AreaHa) /100 as area_km 
# MAGIC
# MAGIC from fua_2021
# MAGIC
# MAGIC group by 
# MAGIC FUA_CODE,CNTR_CODE,FUA_NAME
# MAGIC order by FUA_CODE

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

# MAGIC %sql
# MAGIC ----TESTING ------------------ TESTING
# MAGIC select 
# MAGIC fua_2021.fua_code,
# MAGIC fua_2021.CNTR_CODE,
# MAGIC fua_2021.fua_name,
# MAGIC fua_2021.AreaHa as areaha,
# MAGIC treecover2018.category /10000 as Tree_area_HRL_ha,
# MAGIC street_tree.areaHa  as Tree_area_Street_ha,
# MAGIC forest_ua_2018.areaHa  as Tree_area_UA18_forest_ha,
# MAGIC
# MAGIC if(treecover2018.category>0, 0.01, if(street_tree.areaHa>0,0.01, if(forest_ua_2018.areaHa >0, 0.01,0))) as TREE_area_ha
# MAGIC from fua_2021
# MAGIC
# MAGIC left join treecover2018 on treecover2018.gridnum= fua_2021.gridnum 
# MAGIC left join street_tree on street_tree.gridnum= fua_2021.gridnum 
# MAGIC left join forest_ua_2018 on forest_ua_2018.gridnum= fua_2021.gridnum
# MAGIC
# MAGIC
# MAGIC
# MAGIC where fua_2021.CNTR_CODE = 'LU'
# MAGIC  --and treecover2018.category = 1
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // This box constructed the new Urban FUA TREE CUBE
# MAGIC
# MAGIC val tree_cube_fua = spark.sql(""" 
# MAGIC       select 
# MAGIC       fua_2021.fua_code,
# MAGIC       fua_2021.CNTR_CODE,
# MAGIC       fua_2021.fua_name,
# MAGIC       fua_2021.AreaHa as areaha,
# MAGIC       treecover2018.category /10000 as Tree_area_HRL_ha,
# MAGIC       street_tree.areaHa  as Tree_area_Street_ha,
# MAGIC       forest_ua_2018.areaHa  as Tree_area_UA18_forest_ha,
# MAGIC
# MAGIC       if(treecover2018.category>0, 0.01, if(street_tree.areaHa>0,0.01, if(forest_ua_2018.areaHa >0, 0.01,0))) as TREE_area_ha
# MAGIC       
# MAGIC       from fua_2021
# MAGIC       left join treecover2018 on treecover2018.gridnum= fua_2021.gridnum 
# MAGIC       left join street_tree on street_tree.gridnum= fua_2021.gridnum 
# MAGIC       left join forest_ua_2018 on forest_ua_2018.gridnum= fua_2021.gridnum
# MAGIC
# MAGIC       where fua_2021.CNTR_CODE is not null
# MAGIC  
# MAGIC                                   """)
# MAGIC
# MAGIC tree_cube_fua.createOrReplaceTempView("tree_cube_fua")
# MAGIC
# MAGIC
# MAGIC //
# MAGIC // tree_fua = spark.sql(""" 
# MAGIC //  select 
# MAGIC //   fua_code,
# MAGIC //   CNTR_CODE,
# MAGIC //   fua_name,
# MAGIC //   SUM(AreaHa) as areaha,
# MAGIC //   SUM(Tree_area_HRL_ha) as areaha ,
# MAGIC //   SUM(Tree_area_Street_ha) as Tree_area_Street_ha,
# MAGIC //   SUM(Tree_area_UA18_forest_ha) as Tree_area_UA18_forest_ha,
# MAGIC //
# MAGIC //   SUM(TREE_area_ha) as TREE_area_ha,
# MAGIC //   100.00/SUM(AreaHa) * SUM(TREE_area_ha) as Tree_area_UA18_forest_per_fua
# MAGIC //   from tree_cube_fua
# MAGIC //   where CNTR_CODE is not null
# MAGIC //   group by fua_code, CNTR_CODE, fua_name
# MAGIC //                               """)
# MAGIC //
# MAGIC //e_fua
# MAGIC // .coalesce(1) //be careful with this
# MAGIC // .write.format("com.databricks.spark.csv")
# MAGIC // .mode(SaveMode.Overwrite)
# MAGIC // .option("sep","|")
# MAGIC // .option("overwriteSchema", "true")
# MAGIC // .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
# MAGIC // .option("emptyValue", "")
# MAGIC // .option("header","true")
# MAGIC // .option("treatEmptyValuesAsNulls", "true")  
# MAGIC // 
# MAGIC // .save("dbfs:/mnt/trainingDatabricks/ExportTable/Urban_green_indicators/FUA_urban_tree_cover")
# MAGIC //
# MAGIC // tree_fua.createOrReplaceTempView("tree_fua_indicator")

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC
# MAGIC
# MAGIC val tree_fua = spark.sql(""" 
# MAGIC      select 
# MAGIC       fua_code,
# MAGIC       CNTR_CODE,
# MAGIC       fua_name,
# MAGIC       SUM(AreaHa) as areaha,
# MAGIC       SUM(Tree_area_HRL_ha) as Tree_area_HRL_ha,
# MAGIC       SUM(Tree_area_Street_ha) as Tree_area_Street_ha,
# MAGIC       SUM(Tree_area_UA18_forest_ha) as Tree_area_UA18_forest_ha,
# MAGIC       SUM(TREE_area_ha) as TREE_area_ha,
# MAGIC       100.00/SUM(AreaHa) * SUM(TREE_area_ha) as Tree_area_UA18_forest_per_fua
# MAGIC       from tree_cube_fua
# MAGIC       where CNTR_CODE is not null
# MAGIC       group by fua_code, CNTR_CODE, fua_name
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

# MAGIC %sql
# MAGIC select *from tree_fua_indicator
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select fua_code 
# MAGIC ,SUM(TREE_area_ha) as TREE_area_ha
# MAGIC ,SUM(areaha) as areaha
# MAGIC
# MAGIC from tree_cube_fua
# MAGIC where CNTR_CODE is not null
# MAGIC group by fua_code
# MAGIC

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


