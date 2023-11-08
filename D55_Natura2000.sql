-- Databricks notebook source
-- MAGIC %md # Natura 2000
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ##  1) Reading DIMS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC //  Reading new NUTS3 data
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (0) ADMIN layer
-- MAGIC
-- MAGIC // the following DIM -stored on azure will be load and stored as "parquetFileDF_D_admbndEEA39v2020_531" for the SQL queries:
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1856&fileId=881
-- MAGIC val parquetFileDF_ADM_boundEEA38_2022 = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_ADM_boundEEA38_2022_881_2022427_100m/")             /// use load
-- MAGIC parquetFileDF_ADM_boundEEA38_2022.createOrReplaceTempView("ADM_boundEEA38_2022")
-- MAGIC
-- MAGIC
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val schema_nuts2021 = new StructType()
-- MAGIC .add("ADM_ID",LongType,true)
-- MAGIC .add("ICC",StringType,true)
-- MAGIC .add("CDDAregionCODE",StringType,true)
-- MAGIC .add("ADM_country",StringType,true)
-- MAGIC .add("LEVEL3_name",StringType,true)
-- MAGIC .add("LEVEL2_name",StringType,true)
-- MAGIC .add("LEVEL1_name",StringType,true)
-- MAGIC .add("LEVEL0_name",StringType,true)
-- MAGIC .add("LEVEL3_code",StringType,true)
-- MAGIC .add("LEVEL2_code",StringType,true)
-- MAGIC .add("LEVEL1_code",StringType,true)
-- MAGIC .add("LEVEL0_code",StringType,true)
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
-- MAGIC ///.add("TAA_DESN",StringType,true)
-- MAGIC
-- MAGIC val LUT_nuts2021  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_nuts2021)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/adm_bnd_eea38_2022/20220426143949.163.csv")
-- MAGIC LUT_nuts2021.createOrReplaceTempView("LUT_nuts2021")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val nuts3_2021 = spark.sql(""" 
-- MAGIC                SELECT 
-- MAGIC
-- MAGIC ADM_boundEEA38_2022.gridnum,
-- MAGIC ADM_boundEEA38_2022.Adm_EEA38_NUTS2021v2022,
-- MAGIC ADM_boundEEA38_2022.AreaHa,
-- MAGIC LUT_nuts2021.ADM_ID,
-- MAGIC LUT_nuts2021.ADM_country	,
-- MAGIC LUT_nuts2021.ICC	,
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
-- MAGIC FROM ADM_boundEEA38_2022 
-- MAGIC   LEFT JOIN LUT_nuts2021  ON ADM_boundEEA38_2022.Adm_EEA38_NUTS2021v2022 = LUT_nuts2021.ADM_ID 
-- MAGIC   
-- MAGIC        
-- MAGIC  
-- MAGIC                                   """)
-- MAGIC
-- MAGIC nuts3_2021.createOrReplaceTempView("nuts3_2021")
-- MAGIC
-- MAGIC
-- MAGIC ///##########################################################################################################################################
-- MAGIC //// (10) new N2k type a
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1868&fileId=893
-- MAGIC val parquetFileDF_n2k_a = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeAnet10m_893_2022524_10m/")             /// use load
-- MAGIC parquetFileDF_n2k_a.createOrReplaceTempView("N2k_type_a")
-- MAGIC
-- MAGIC
-- MAGIC ////Lookup table for Natura 2000 type A: 
-- MAGIC //Lut: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=133
-- MAGIC //cwsblobstorage01/cwsblob01/Lookups/Natura2000v2021typ_a/20220530150813.893.csv                     
-- MAGIC
-- MAGIC val schema_n2k_v2021_type_a = new StructType()
-- MAGIC .add("raster_value",LongType,true)
-- MAGIC .add("sitecode",StringType,true)
-- MAGIC .add("country",StringType,true)
-- MAGIC .add("sitetype",StringType,true)
-- MAGIC .add("sitename",StringType,true)
-- MAGIC .add("reported_area_ha",DoubleType,true)
-- MAGIC .add("year",StringType,true)
-- MAGIC
-- MAGIC val LUT_n2k_v2021_type_a  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_n2k_v2021_type_a)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/Natura2000v2021typ_a/20220530150813.893.csv")
-- MAGIC LUT_n2k_v2021_type_a.createOrReplaceTempView("LUT_n2k_v2021_type_a")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) new N2k type b
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1867&fileId=892
-- MAGIC val parquetFileDF_n2k_b = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeBnet10m_892_2022524_10m/")             /// use load
-- MAGIC parquetFileDF_n2k_b.createOrReplaceTempView("N2k_type_b")
-- MAGIC
-- MAGIC ////Lookup table for Natura 2000 type b: 
-- MAGIC //Lut: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=134
-- MAGIC //cwsblobstorage01/cwsblob01/Lookups//Natura2000v2021typ_b/20220530150849.763.csv                    
-- MAGIC
-- MAGIC val schema_n2k_v2021_type_b = new StructType()
-- MAGIC .add("raster_value",FloatType,true)
-- MAGIC .add("sitecode",StringType,true)
-- MAGIC .add("country",StringType,true)
-- MAGIC .add("sitetype",StringType,true)
-- MAGIC .add("sitename",StringType,true)
-- MAGIC .add("reported_area_ha",DoubleType,true)
-- MAGIC .add("year",StringType,true)
-- MAGIC
-- MAGIC val LUT_n2k_v2021_type_b  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_n2k_v2021_type_b)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/Natura2000v2021typ_b/20220530150849.763.csv")
-- MAGIC LUT_n2k_v2021_type_b.createOrReplaceTempView("LUT_n2k_v2021_type_b")
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) new N2k type c
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1861&fileId=886
-- MAGIC val parquetFileDF_n2k_c = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeCnet10m_886_202251_10m/")             /// use load
-- MAGIC parquetFileDF_n2k_c.createOrReplaceTempView("N2k_type_c")
-- MAGIC
-- MAGIC ////Lookup table for Natura 2000 type c: 
-- MAGIC //Lut: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=128
-- MAGIC //cwsblobstorage01/cwsblob01/Lookups/Natura2000v2021typ_c/20220510122416.383.csv                 
-- MAGIC
-- MAGIC val schema_n2k_v2021_type_c = new StructType()
-- MAGIC .add("raster_value",FloatType,true)
-- MAGIC .add("sitecode",StringType,true)
-- MAGIC .add("country",StringType,true)
-- MAGIC .add("sitetype",StringType,true)
-- MAGIC .add("sitename",StringType,true)
-- MAGIC .add("reported_area_ha",DoubleType,true)
-- MAGIC .add("year",StringType,true)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val LUT_n2k_v2021_type_c  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_n2k_v2021_type_c)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/Natura2000v2021typ_c/20220510122416.383.csv")
-- MAGIC LUT_n2k_v2021_type_c.createOrReplaceTempView("LUT_n2k_v2021_type_c")
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

Select ICC,CDDAregionCode  from LUT_nuts2021 group by ICC,CDDAregionCode

-- COMMAND ----------


select * from N2k_type_c

-- COMMAND ----------




-- COMMAND ----------

-- MAGIC %md ## 2) playing with  10m N2k dims 2021

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC //  Reading new N2k type c
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) new N2k type a
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1868&fileId=893
-- MAGIC val parquetFileDF_n2k_a = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeAnet10m_893_2022524_10m/")             /// use load
-- MAGIC parquetFileDF_n2k_a.createOrReplaceTempView("N2k_type_a")
-- MAGIC
-- MAGIC
-- MAGIC ////Lookup table for Natura 2000 type A: 
-- MAGIC //Lut: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=133
-- MAGIC //cwsblobstorage01/cwsblob01/Lookups/Natura2000v2021typ_a/20220530150813.893.csv                     
-- MAGIC
-- MAGIC val schema_n2k_v2021_type_a = new StructType()
-- MAGIC .add("raster_value",LongType,true)
-- MAGIC .add("sitecode",StringType,true)
-- MAGIC .add("country",StringType,true)
-- MAGIC .add("sitetype",StringType,true)
-- MAGIC .add("sitename",StringType,true)
-- MAGIC .add("reported_area_ha",DoubleType,true)
-- MAGIC .add("year",StringType,true)
-- MAGIC
-- MAGIC val LUT_n2k_v2021_type_a  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_n2k_v2021_type_a)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/Natura2000v2021typ_a/20220530150813.893.csv")
-- MAGIC LUT_n2k_v2021_type_a.createOrReplaceTempView("LUT_n2k_v2021_type_a")
-- MAGIC
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) new N2k type b
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1867&fileId=892
-- MAGIC val parquetFileDF_n2k_b = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeBnet10m_892_2022524_10m/")             /// use load
-- MAGIC parquetFileDF_n2k_b.createOrReplaceTempView("N2k_type_b")
-- MAGIC
-- MAGIC ////Lookup table for Natura 2000 type b: 
-- MAGIC //Lut: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=134
-- MAGIC //cwsblobstorage01/cwsblob01/Lookups//Natura2000v2021typ_b/20220530150849.763.csv                    
-- MAGIC
-- MAGIC val schema_n2k_v2021_type_b = new StructType()
-- MAGIC .add("raster_value",FloatType,true)
-- MAGIC .add("sitecode",StringType,true)
-- MAGIC .add("country",StringType,true)
-- MAGIC .add("sitetype",StringType,true)
-- MAGIC .add("sitename",StringType,true)
-- MAGIC .add("reported_area_ha",DoubleType,true)
-- MAGIC .add("year",StringType,true)
-- MAGIC
-- MAGIC val LUT_n2k_v2021_type_b  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_n2k_v2021_type_b)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/Natura2000v2021typ_b/20220530150849.763.csv")
-- MAGIC LUT_n2k_v2021_type_b.createOrReplaceTempView("LUT_n2k_v2021_type_b")
-- MAGIC
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) new N2k type c
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1861&fileId=886
-- MAGIC val parquetFileDF_n2k_c = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeCnet10m_886_202251_10m/")             /// use load
-- MAGIC parquetFileDF_n2k_c.createOrReplaceTempView("N2k_type_c")
-- MAGIC
-- MAGIC ////Lookup table for Natura 2000 type c: 
-- MAGIC //Lut: https://jedi.discomap.eea.europa.eu/LookUp/show?lookUpId=128
-- MAGIC //cwsblobstorage01/cwsblob01/Lookups/Natura2000v2021typ_c/20220510122416.383.csv                 
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val LUT_n2k_v2021_type_c  = spark.read.format("csv")
-- MAGIC  .options(Map("delimiter"->"|"))
-- MAGIC  .schema(schema_n2k_v2021_type_c)
-- MAGIC  .load("dbfs:/mnt/trainingDatabricks/Lookups/Natura2000v2021typ_c/20220510122416.383.csv")
-- MAGIC LUT_n2k_v2021_type_c.createOrReplaceTempView("LUT_n2k_v2021_type_c")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------



select *  from LUT_n2k_v2021_type_b 

where raster_value =21617

   


-- COMMAND ----------

Select * 
,gridnum & cast(-65536 as bigint) as GridNum100m
,gridnum & cast(- 16777216 as bigint) as GridNum1km
,gridnum & cast(- 4294967296 as bigint) as GridNum10km


from N2k_type_c



where GridNum10km =18587398686375936  ---- somewhere in Luxembourg

-- COMMAND ----------

Select N2k2021_10m_C_tiles, sum(AreaHa) as area_ha from N2k_type_c
where N2k2021_10m_C_tiles= 1062
group by N2k2021_10m_C_tiles

-- COMMAND ----------

select   18587399030790400 & cast(-65536 as bigint) as GridNum100m 

-- COMMAND ----------

select   GridNum & cast(-65536 as bigint) as GridNum100m ,
if (SUM(Areaha)> 0.5,  1, 0 ) as Protected_pixel,
if (SUM(Areaha)> 0.5,  1, 0 ) as area_ha,
round(SUM(Areaha),2) as protected_area_n2k_type_c_ha

from N2k_type_c
where N2k_type_c.GridNum10km =18587398686375936
group by GridNum & cast(-65536 as bigint)



-- COMMAND ----------

Select



date (right(LUT_n2k_v2021_type_b.year, 4)) as year 
, sum(AreaHa) as AreaHa


from N2k_type_b

left JOIN  LUT_n2k_v2021_type_b on LUT_n2k_v2021_type_b.raster_value = N2k_type_b.N2k_2021_type_B_sitecode 
group by date (right(LUT_n2k_v2021_type_b.year, 4))

-- COMMAND ----------

-- MAGIC %md ## 3) 10m N2k net (shadow)

-- COMMAND ----------

---1 JOIN full outer join type a,b,c for a 10km cell in lux: GridNum10km =18587398686375936  ---- somewhere in Luxembourg

Select  
if (N2k_type_a.gridnum is not null,  N2k_type_a.gridnum, if (N2k_type_b.gridnum is not null,  N2k_type_b.gridnum, N2k_type_c.gridnum )) as gridnum, --- find final gridnum

N2k_type_a.gridnum as gridnum_a,
N2k_type_b.gridnum as gridnum_b,
N2k_type_c.gridnum as gridnum_c,
N2k_2021_type_A_sitecode as n2k_site_a,
N2k_2021_type_B_sitecode as n2k_site_b,
---N2k_2021_type_C_sitecode as n2k_site_c,
N2k2021_10m_C_tiles as n2k_site_c,

LUT_n2k_v2021_type_a.sitecode as sitecode_a,
LUT_n2k_v2021_type_a.sitename  as sitename_a,
LUT_n2k_v2021_type_a.year   as year_a,

LUT_n2k_v2021_type_b.sitecode as sitecode_b,
LUT_n2k_v2021_type_b.sitename  as sitename_b,
LUT_n2k_v2021_type_b.year as year_b,


LUT_n2k_v2021_type_c.sitecode as sitecode_c,
LUT_n2k_v2021_type_c.sitename   as sitename_c,
LUT_n2k_v2021_type_c.year as year_c,


if (N2k_type_a.gridnum is not null,  'Protected by Natura 2000', 
   if (N2k_type_b.gridnum is not null,  'Protected by Natura 2000', 
   if (N2k_type_c.gridnum is not null,  'Protected by Natura 2000', 'error'  ))) as Natura2000_net   -- net n2k protection

,if (N2k_type_a.AreaHa is not null,  N2k_type_a.AreaHa, if (N2k_type_b.AreaHa is not null,  N2k_type_b.AreaHa, N2k_type_c.AreaHa )) as AreaHa --- find final gridnum




from N2k_type_a

FULL OUTER JOIN  N2k_type_b on N2k_type_a.gridnum = N2k_type_b.gridnum 
FULL OUTER JOIN  N2k_type_c on N2k_type_a.gridnum = N2k_type_c.gridnum 


left JOIN  LUT_n2k_v2021_type_a on LUT_n2k_v2021_type_a.raster_value = N2k_type_a.N2k_2021_type_A_sitecode 
left JOIN  LUT_n2k_v2021_type_b on LUT_n2k_v2021_type_b.raster_value = N2k_type_b.N2k_2021_type_B_sitecode 
left JOIN  LUT_n2k_v2021_type_c on LUT_n2k_v2021_type_c.raster_value = N2k_type_c.N2k2021_10m_C_tiles 
---left JOIN  LUT_n2k_v2021_type_c on LUT_n2k_v2021_type_c.raster_value = N2k_type_c.N2k_2021_type_C_sitecode 




where N2k_type_a.GridNum10km =18587398686375936  ---- somewhere in Luxembourg

-- COMMAND ----------

---1 JOIN full outer join type a,b,c for a 10km cell in lux: GridNum10km =18587398686375936  ---- somewhere in Luxembourg

Select  
if (N2k_type_a.gridnum is not null,  N2k_type_a.gridnum, if (N2k_type_b.gridnum is not null,  N2k_type_b.gridnum, N2k_type_c.gridnum )) as gridnum, --- find final gridnum

N2k_type_a.gridnum as gridnum_a,
N2k_type_b.gridnum as gridnum_b,
N2k_type_c.gridnum as gridnum_c,


if (N2k_type_a.gridnum is not null,  'Protected by Natura 2000', 
   if (N2k_type_b.gridnum is not null,  'Protected by Natura 2000', 
   if (N2k_type_c.gridnum is not null,  'Protected by Natura 2000', 'error'  ))) as Natura2000_net   -- net n2k protection

 if (N2k_type_b.gridnum is not null,  'Protected by Natura 2000', 
   if (N2k_type_c.gridnum is not null,  'Protected by Natura 2000', 'error'  )) as Natura2000_SCIs_BC  

,if (N2k_type_a.AreaHa is not null,  N2k_type_a.AreaHa, if (N2k_type_b.AreaHa is not null,  N2k_type_b.AreaHa, N2k_type_c.AreaHa )) as AreaHa --- find final gridnum



from N2k_type_a

FULL OUTER JOIN  N2k_type_b on N2k_type_a.gridnum = N2k_type_b.gridnum 
FULL OUTER JOIN  N2k_type_c on N2k_type_a.gridnum = N2k_type_c.gridnum 






where N2k_type_a.GridNum10km =18587398686375936  ---- somewhere in Luxembourg

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // ---Net: SCI BC
-- MAGIC
-- MAGIC val n2k_SCI_10m_v2021 = spark.sql(""" 
-- MAGIC
-- MAGIC Select  
-- MAGIC if (N2k_type_b.gridnum is not null,  N2k_type_b.gridnum, N2k_type_c.gridnum ) as gridnum, --- find final gridnum
-- MAGIC N2k_type_b.gridnum as gridnum_b,
-- MAGIC N2k_type_c.gridnum as gridnum_c,
-- MAGIC
-- MAGIC
-- MAGIC  if (N2k_type_b.gridnum is not null,  'Protected by Natura 2000', 
-- MAGIC    if (N2k_type_c.gridnum is not null,  'Protected by Natura 2000', 'error'  )) as Natura2000_SCIs_BC  
-- MAGIC
-- MAGIC ,if (N2k_type_b.AreaHa is not null,  N2k_type_b.AreaHa, N2k_type_c.AreaHa ) as AreaHa --- find final gridnum
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC from N2k_type_b
-- MAGIC
-- MAGIC FULL OUTER JOIN  N2k_type_c on N2k_type_b.gridnum = N2k_type_c.gridnum 
-- MAGIC
-- MAGIC
-- MAGIC      """)
-- MAGIC
-- MAGIC n2k_SCI_10m_v2021.createOrReplaceTempView("n2k_net_SCI_10m_v2021")
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC val n2k_SCI_10m_v2021_v2 = spark.sql(""" 
-- MAGIC select gridnum & cast(-65536 as bigint) as GridNum100m,*
-- MAGIC from n2k_net_SCI_10m_v2021
-- MAGIC      """)
-- MAGIC
-- MAGIC n2k_SCI_10m_v2021_v2.createOrReplaceTempView("n2k_net_SCI_10m_v2021_v2")
-- MAGIC
-- MAGIC
-- MAGIC val n2k_net_SCI_10m_v2021_nuts = spark.sql(""" 
-- MAGIC select
-- MAGIC
-- MAGIC LEVEL0_code,
-- MAGIC LEVEL1_code,
-- MAGIC LEVEL2_code,
-- MAGIC LEVEL3_code,
-- MAGIC SUM(n2k_net_SCI_10m_v2021_v2.AreaHa) as N2K_SCI_end2021_10m_area_ha,
-- MAGIC SUM(nuts3_2021.AreaHa) as area_ha
-- MAGIC
-- MAGIC from n2k_net_SCI_10m_v2021_v2
-- MAGIC
-- MAGIC   LEFT JOIN nuts3_2021  ON n2k_net_SCI_10m_v2021_v2.GridNum100m = nuts3_2021.gridnum 
-- MAGIC
-- MAGIC   where 
-- MAGIC TAA in ('Land area', 'Inland water')
-- MAGIC
-- MAGIC group by 
-- MAGIC LEVEL0_code,
-- MAGIC LEVEL1_code,
-- MAGIC LEVEL2_code,
-- MAGIC LEVEL3_code
-- MAGIC
-- MAGIC      """)
-- MAGIC
-- MAGIC n2k_net_SCI_10m_v2021_nuts.createOrReplaceTempView("n2k_net_SCI_10m_v2021_nuts")
-- MAGIC

-- COMMAND ----------

select * from n2k_net_SCI_10m_v2021

-- COMMAND ----------

show columns

-- COMMAND ----------

select

LEVEL0_code,
LEVEL1_code,
LEVEL2_code,
LEVEL3_code,
SUM(n2k_net_SCI_10m_v2021_v2.AreaHa) as N2K_SCI_end2021_10m_area_ha,
SUM(nuts3_2021.AreaHa) as area_ha

from n2k_net_SCI_10m_v2021_v2

  LEFT JOIN nuts3_2021  ON n2k_net_SCI_10m_v2021_v2.GridNum100m = nuts3_2021.gridnum 




  where 
TAA in ('Land area', 'Inland water')

group by 
LEVEL0_code,
LEVEL1_code,
LEVEL2_code,
LEVEL3_code


-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC val n2k_net_10m_v2021 = spark.sql(""" 
-- MAGIC               
-- MAGIC Select  
-- MAGIC if (N2k_type_a.gridnum is not null,  N2k_type_a.gridnum, if (N2k_type_b.gridnum is not null,  N2k_type_b.gridnum, N2k_type_c.gridnum )) as gridnum,
-- MAGIC
-- MAGIC
-- MAGIC if (N2k_type_a.gridnum is not null,  'Protected by Natura 2000', 
-- MAGIC    if (N2k_type_b.gridnum is not null,  'Protected by Natura 2000', 
-- MAGIC    if (N2k_type_c.gridnum is not null,  'Protected by Natura 2000', 'error'  ))) as Natura2000_net   -- net n2k protection
-- MAGIC ,if (N2k_type_a.AreaHa is not null,  N2k_type_a.AreaHa, if (N2k_type_b.AreaHa is not null,  N2k_type_b.AreaHa, N2k_type_c.AreaHa )) as AreaHa 
-- MAGIC
-- MAGIC from N2k_type_a
-- MAGIC
-- MAGIC FULL OUTER JOIN  N2k_type_b on N2k_type_a.gridnum = N2k_type_b.gridnum 
-- MAGIC FULL OUTER JOIN  N2k_type_c on N2k_type_a.gridnum = N2k_type_c.gridnum 
-- MAGIC   
-- MAGIC        
-- MAGIC  
-- MAGIC                                   """)
-- MAGIC
-- MAGIC n2k_net_10m_v2021.createOrReplaceTempView("n2k_net_10m_v2021")
-- MAGIC
-- MAGIC
-- MAGIC val n2k_net_100m_v2021_x = spark.sql(""" 
-- MAGIC               
-- MAGIC select 
-- MAGIC gridnum & cast(-65536 as bigint) as GridNum100m,
-- MAGIC gridnum & cast(- 4294967296 as bigint) as GridNum10km,
-- MAGIC
-- MAGIC if (SUM(Areaha)> 0.5,  1, 0 ) as Majority_protected_by_N2k_net_100m,
-- MAGIC Natura2000_net,
-- MAGIC SUM(AreaHa) as AreaHa
-- MAGIC from n2k_net_10m_v2021
-- MAGIC
-- MAGIC
-- MAGIC group by 
-- MAGIC gridnum & cast(-65536 as bigint) 
-- MAGIC ,Natura2000_net, 
-- MAGIC gridnum & cast(- 4294967296 as bigint)
-- MAGIC   
-- MAGIC        
-- MAGIC  
-- MAGIC                                   """)
-- MAGIC
-- MAGIC n2k_net_10m_v2021.createOrReplaceTempView("n2k_net_100m_v2021_x")
-- MAGIC
-- MAGIC
-- MAGIC val n2k_net_10m_v2021_with_sitecode_lux = spark.sql("""  
-- MAGIC Select  
-- MAGIC if (N2k_type_a.gridnum is not null,  N2k_type_a.gridnum, if (N2k_type_b.gridnum is not null,  N2k_type_b.gridnum, N2k_type_c.gridnum )) as gridnum, --- find final gridnum
-- MAGIC
-- MAGIC N2k_type_a.gridnum as gridnum_a,
-- MAGIC N2k_type_b.gridnum as gridnum_b,
-- MAGIC N2k_type_c.gridnum as gridnum_c,
-- MAGIC N2k_2021_type_A_sitecode as n2k_site_a,
-- MAGIC N2k_2021_type_B_sitecode as n2k_site_b,
-- MAGIC ---N2k_2021_type_C_sitecode as n2k_site_c,
-- MAGIC N2k2021_10m_C_tiles as n2k_site_c,
-- MAGIC
-- MAGIC LUT_n2k_v2021_type_a.sitecode as sitecode_a,
-- MAGIC LUT_n2k_v2021_type_a.sitename  as sitename_a,
-- MAGIC LUT_n2k_v2021_type_a.year   as year_a,
-- MAGIC
-- MAGIC LUT_n2k_v2021_type_b.sitecode as sitecode_b,
-- MAGIC LUT_n2k_v2021_type_b.sitename  as sitename_b,
-- MAGIC LUT_n2k_v2021_type_b.year as year_b,
-- MAGIC
-- MAGIC
-- MAGIC LUT_n2k_v2021_type_c.sitecode as sitecode_c,
-- MAGIC LUT_n2k_v2021_type_c.sitename   as sitename_c,
-- MAGIC LUT_n2k_v2021_type_c.year as year_c,
-- MAGIC
-- MAGIC
-- MAGIC if (N2k_type_a.gridnum is not null,  'Protected by Natura 2000', 
-- MAGIC    if (N2k_type_b.gridnum is not null,  'Protected by Natura 2000', 
-- MAGIC    if (N2k_type_c.gridnum is not null,  'Protected by Natura 2000', 'error'  ))) as Natura2000_net   -- net n2k protection
-- MAGIC
-- MAGIC ,if (N2k_type_a.AreaHa is not null,  N2k_type_a.AreaHa, if (N2k_type_b.AreaHa is not null,  N2k_type_b.AreaHa, N2k_type_c.AreaHa )) as AreaHa --- find final gridnum
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC from N2k_type_a
-- MAGIC
-- MAGIC FULL OUTER JOIN  N2k_type_b on N2k_type_a.gridnum = N2k_type_b.gridnum 
-- MAGIC FULL OUTER JOIN  N2k_type_c on N2k_type_a.gridnum = N2k_type_c.gridnum 
-- MAGIC
-- MAGIC
-- MAGIC left JOIN  LUT_n2k_v2021_type_a on LUT_n2k_v2021_type_a.raster_value = N2k_type_a.N2k_2021_type_A_sitecode 
-- MAGIC left JOIN  LUT_n2k_v2021_type_b on LUT_n2k_v2021_type_b.raster_value = N2k_type_b.N2k_2021_type_B_sitecode 
-- MAGIC left JOIN  LUT_n2k_v2021_type_c on LUT_n2k_v2021_type_c.raster_value = N2k_type_c.N2k2021_10m_C_tiles 
-- MAGIC ---left JOIN  LUT_n2k_v2021_type_c on LUT_n2k_v2021_type_c.raster_value = N2k_type_c.N2k_2021_type_C_sitecode 
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC where N2k_type_a.GridNum10km =18587398686375936  ---- somewhere in Luxembourg
-- MAGIC                                   """)
-- MAGIC
-- MAGIC n2k_net_10m_v2021_with_sitecode_lux.createOrReplaceTempView("n2k_net_10m_v2021_lux")
-- MAGIC

-- COMMAND ----------

select * from n2k_net_10m_v2021_lux

-- COMMAND ----------

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



-- COMMAND ----------

-- MAGIC %md ## 4) N2k 100m
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %scala
-- MAGIC
-- MAGIC //  Reading new N2k type c
-- MAGIC
-- MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
-- MAGIC import spark.sqlContext.implicits._ 
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC //##########################################################################################################################################
-- MAGIC //// (10) new N2k type a
-- MAGIC
-- MAGIC //https://jedi.discomap.eea.europa.eu/Dimension/show?dimId=1868&fileId=893
-- MAGIC val parquetFileDF_n2k_c100m = spark.read.format("delta").load("dbfs:/mnt/trainingDatabricks/Dimensions/D_N2k_2021typeCnet100m_894_202268_100m/")             /// use load
-- MAGIC parquetFileDF_n2k_c100m.createOrReplaceTempView("N2k_type_c_100m")

-- COMMAND ----------

Select * from N2k_type_c_100m

