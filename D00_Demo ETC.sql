-- Databricks notebook source
-- MAGIC %md # START

-- COMMAND ----------

-- MAGIC %md # 4. Read dims with scala
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC // FIRST start the cluster: ETC-ULS !!!!!!!!!!!!!!!!!!!!!!!!
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (0) ADMIN layer
-- MAGIC
-- MAGIC // the following DIM -stored on azure will be load and stored as "parquetFileDF_D_admbndEEA39v2020_531" for the SQL queries:
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1793&fileId=818
-- MAGIC val parquetFileDF_D_admbndEEA39v2020_531 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_admbndEEA39v2020_531_2020522_100m/")             /// use load
-- MAGIC parquetFileDF_D_admbndEEA39v2020_531.createOrReplaceTempView("D_admbndEEA39v2020_531")
-- MAGIC

-- COMMAND ----------

select * 

-- COMMAND ----------

-- MAGIC %md # 5. Read loaded DIM with SQL
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT  *
FROM D_admbndEEA39v2020_531 
limit 100


-- COMMAND ----------

-- MAGIC %md # 6. Read and join DIM with SQL
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (0) ADMIN layer
-- MAGIC // a) DIM
-- MAGIC // the following DIM -stored on azure will be load and stored as "parquetFileDF_D_admbndEEA39v2020_531" for the SQL queries:
-- MAGIC val parquetFileDF_D_admbndEEA39v2020_531 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_admbndEEA39v2020_531_2020522_100m/")             /// use load
-- MAGIC parquetFileDF_D_admbndEEA39v2020_531.createOrReplaceTempView("D_admbndEEA39v2020_531")
-- MAGIC
-- MAGIC //b) LUT  -  ADMIN layer lookuptable for admin category:
-- MAGIC val admin_lookuptable  = spark.read.format("csv")
-- MAGIC .options(Map("delimiter"->","))
-- MAGIC  .option("header", "true")
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/LookupTablesFiles/AdmBound_EEA39_2020_64.csv")
-- MAGIC admin_lookuptable.createOrReplaceTempView("LUT_admin")
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (1)CLC and LUT-clc for MAES
-- MAGIC //##########################################################################################################################################
-- MAGIC // a) DIM
-- MAGIC val parquetFileDF_clc18 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_A_CLC_18_210_20181129_100m/")
-- MAGIC parquetFileDF_clc18.createOrReplaceTempView("CLC_2018")
-- MAGIC // b) LUT 
-- MAGIC val lut_clc  = spark.read.format("csv")
-- MAGIC .options(Map("delimiter"->","))
-- MAGIC  .option("header", "true")
-- MAGIC    .load("dbfs:/mnt/trainingDatabricks/LookupTablesFiles/Lookup_CLC_24032021_4.csv")
-- MAGIC lut_clc.createOrReplaceTempView("LUT_clc_classes")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (3) N2k
-- MAGIC //##########################################################################################################################################
-- MAGIC // a) DIM
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val schema_n2k = new StructType()
-- MAGIC   .add("x",LongType,true)
-- MAGIC   .add("y",LongType,true)
-- MAGIC  .add("GridNum",LongType,true)
-- MAGIC   .add("MS", StringType, true) 
-- MAGIC   .add("N2k_sitecode", StringType, true) 
-- MAGIC   .add("N2k_sitename", StringType, true) 
-- MAGIC  .add("N2k_sitetype", StringType, true) 
-- MAGIC  .add("Shape_Leng",DoubleType,true)
-- MAGIC  .add("Shape_Area",DoubleType,true)
-- MAGIC  .add("GridNum10km",LongType,true)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val parquetFileDF_n2k2020_typeA = spark.read.format("csv")
-- MAGIC .options(Map("delimiter"->"|"))
-- MAGIC   .schema(schema_n2k)
-- MAGIC
-- MAGIC .load("dbfs:/mnt/trainingDatabricks/Dimensions/N2k_2020typAnet_825/")
-- MAGIC parquetFileDF_n2k2020_typeA.createOrReplaceTempView("N2k_2020_type_a")
-- MAGIC
-- MAGIC

-- COMMAND ----------

Select * from  N2k_2020_type_a

----where GridNum10km =14118163092340736
where GridNum = 14118165341667328

-- COMMAND ----------

Select * from LUT_admin


-- COMMAND ----------

Select * from D_admbndEEA39v2020_531 limit 10

-- COMMAND ----------

SELECT    *       
                     
FROM D_admbndEEA39v2020_531
   LEFT JOIN   LUT_admin  ON LUT_admin.ADM_ID = D_admbndEEA39v2020_531.Category 




-- COMMAND ----------

SELECT                
                  CLC_2018.GridNum,
                  CLC_2018.GridNum10km,          
                  CONCAT('MAES_',LUT_clc_classes.MAES_CODE) as MAES_CODE ,                  
                  CLC_2018.AreaHa
                  from CLC_2018   
                  LEFT JOIN   LUT_clc_classes  
                     ON  CLC_2018.Category  = LUT_clc_classes.LEVEL3_CODE where AreaHa =1


-- COMMAND ----------

-- MAGIC %md #  7. SQL: aggregate to 10km grid cells
-- MAGIC

-- COMMAND ----------

SELECT                CLC_2018.GridNum10km, 
SUM(CLC_2018.AreaHa) as area_ha_class_211_inside_10km_cell
                     
FROM D_admbndEEA39v2020_531
   LEFT JOIN   CLC_2018                       ON CLC_2018.GridNum = D_admbndEEA39v2020_531.GridNum 

where ClC_2018.Category = 211

group by CLC_2018.GridNum10km


-- COMMAND ----------

SELECT                                         
                  D_admbndEEA39v2020_531.Category,
                  SUM(CLC_2018.AreaHa) as area_ha_class_211_inside_10km_cell
                  
   
FROM D_admbndEEA39v2020_531
   LEFT JOIN   CLC_2018                       ON CLC_2018.GridNum = D_admbndEEA39v2020_531.GridNum 
where ClC_2018.Category = 211
group by D_admbndEEA39v2020_531.Category


-- COMMAND ----------

SELECT                
                           
                  D_admbndEEA39v2020_531.Category,
                  LUT_admin.LEVEL3_name,
                  SUM(CLC_2018.AreaHa) as area_ha_class_211_inside_10km_cell
                  
   
FROM D_admbndEEA39v2020_531
   LEFT JOIN   CLC_2018                       ON CLC_2018.GridNum = D_admbndEEA39v2020_531.GridNum 
   LEFT JOIN   LUT_admin                       ON LUT_admin.ADM_ID = D_admbndEEA39v2020_531.Category 
where ClC_2018.Category = 211
group by D_admbndEEA39v2020_531.Category,LUT_admin.LEVEL3_name


-- COMMAND ----------

-- MAGIC %md # 8. SQL/SCALA – store data under „ExportTable“:
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC
-- MAGIC val tableDF_nuts3_clc211 = spark.sql("""
-- MAGIC
-- MAGIC SELECT                
-- MAGIC                            
-- MAGIC                   D_admbndEEA39v2020_531.Category,
-- MAGIC                   LUT_admin.LEVEL3_name,
-- MAGIC                   SUM(CLC_2018.AreaHa) as area_ha_class_211_inside_10km_cell
-- MAGIC    
-- MAGIC FROM D_admbndEEA39v2020_531
-- MAGIC    LEFT JOIN   CLC_2018                       ON CLC_2018.GridNum = D_admbndEEA39v2020_531.GridNum 
-- MAGIC    LEFT JOIN   LUT_admin                       ON LUT_admin.ADM_ID = D_admbndEEA39v2020_531.Category 
-- MAGIC where ClC_2018.Category = 211
-- MAGIC group by D_admbndEEA39v2020_531.Category,LUT_admin.LEVEL3_name
-- MAGIC
-- MAGIC     """    )
-- MAGIC tableDF_nuts3_clc211.createOrReplaceTempView("nuts3_clc_211") 
-- MAGIC
-- MAGIC ////Export data: 
-- MAGIC
-- MAGIC tableDF_nuts3_clc211
-- MAGIC     .coalesce(1) //be careful with this
-- MAGIC     .write.format("com.databricks.spark.csv")
-- MAGIC     .mode(SaveMode.Overwrite)
-- MAGIC     .option("sep","|")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .option("codec", "org.apache.hadoop.io.compress.GzipCodec")  //optional
-- MAGIC     .option("emptyValue", "")
-- MAGIC     .option("header","true")
-- MAGIC     .option("treatEmptyValuesAsNulls", "true")  
-- MAGIC     .save("dbfs:/mnt/trainingDatabricks/ExportTable/NUTS3//test_1")
-- MAGIC  
-- MAGIC
