# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Building a [Data Lakehouse] using Batch-ETL [Users]
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="600px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dl_batch.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Create & Use a Database
# MAGIC %sql
# MAGIC 
# MAGIC -- creating a database for logical organization
# MAGIC CREATE DATABASE IF NOT EXISTS Yelp;
# MAGIC USE Yelp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > **databricks file system [dbfs]** *is a storage layer of databricks that increases the overall communication from storages. this layer perform set of optimizations to speed up load and other operations that heavily rely on storage access.*

# COMMAND ----------

# DBTITLE 1,List User File[s]
# MAGIC %fs ls "dbfs:/mnt/bs-production/pq_user/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1000px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/rdd_dataframe_dataset.png'>
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC <img width="700px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dataframe_image.png'>
# MAGIC 
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="700px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dataframe_api_new.png'>

# COMMAND ----------

# DBTITLE 1,Reading Parquet to a [Scala] DataFrame
# MAGIC %scala
# MAGIC 
# MAGIC // load data into a scala dataframe
# MAGIC val ds_user = spark.read.parquet("dbfs:/mnt/bs-production/pq_user/user.parquet/")
# MAGIC 
# MAGIC // register as a temporary view
# MAGIC ds_user.createOrReplaceTempView("sc_ds_user")

# COMMAND ----------

# DBTITLE 1,Reading Parquet to a [Python] DataFrame
# MAGIC %python 
# MAGIC 
# MAGIC # load data into a python dataframe
# MAGIC ds_user = spark.read.parquet("dbfs:/mnt/bs-production/pq_user/user.parquet/")
# MAGIC 
# MAGIC # register as a temporary view [sql]
# MAGIC ds_user.createOrReplaceTempView("py_ds_user")

# COMMAND ----------

# DBTITLE 1,Reading Parquet to a [SQL] DataFrame
# MAGIC %sql
# MAGIC 
# MAGIC -- remove if not exists
# MAGIC DROP VIEW IF EXISTS ds_user;
# MAGIC 
# MAGIC -- create temporary view
# MAGIC CREATE TEMPORARY VIEW ds_user
# MAGIC USING org.apache.spark.sql.parquet
# MAGIC OPTIONS ( path "dbfs:/mnt/bs-production/pq_user/user.parquet/" );
# MAGIC 
# MAGIC -- select data
# MAGIC SELECT * FROM ds_user

# COMMAND ----------

# DBTITLE 1,Scala - Read DataFrame ~ ds_user
# MAGIC %scala
# MAGIC 
# MAGIC // cache data in memory
# MAGIC ds_user.cache()
# MAGIC 
# MAGIC // show dataframe
# MAGIC display(ds_user)

# COMMAND ----------

# DBTITLE 1,Python - Read DataFrame ~ ds_user
# MAGIC %python
# MAGIC 
# MAGIC ds_user.cache()
# MAGIC display(ds_user)

# COMMAND ----------

# DBTITLE 1,SQL - Read DataFrame ~ ds_user
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ds_user

# COMMAND ----------

# DBTITLE 1,Count Rows [Scala & SQL] ~ ds_user
# MAGIC %scala
# MAGIC 
# MAGIC // using spark sql core integration
# MAGIC val countRows = spark.sql("""SELECT COUNT(*) FROM ds_user""")
# MAGIC display(countRows)

# COMMAND ----------

# DBTITLE 1,Count Rows [Python] ~ ds_user
# MAGIC %python
# MAGIC 
# MAGIC # importing library to aggregate data
# MAGIC from pyspark.sql.functions import count
# MAGIC 
# MAGIC # select and count rows
# MAGIC ds_user.select(count("user_id")).take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="700px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/co.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Catalyst Optimizer [SQL]
# MAGIC %sql
# MAGIC 
# MAGIC -- write a dataframe/dataset or sql
# MAGIC -- if command valid, convertst into a logical plan
# MAGIC -- spark transforms the logical plan into a physical plan
# MAGIC -- spark executes this plan [rdd] in a cluster
# MAGIC EXPLAIN EXTENDED SELECT * FROM ds_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="700px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/analysis_phase.png'>
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/plan.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Schema ~ [ds_user]
# MAGIC %python 
# MAGIC 
# MAGIC # print schema
# MAGIC # you can explicitly infer a schema
# MAGIC # better to get more precision
# MAGIC # if needed
# MAGIC ds_user.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">  
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC > *Delta Lake* is an open-source storage layer that brings ACID
# MAGIC transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta_lake_1.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1300px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta_lake_gen3.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Writing DataFrame into Data Lake [Blob Storage] using Delta Format
# MAGIC %python
# MAGIC 
# MAGIC # write dataframe [ds_user] into data lake [azure blob storage]
# MAGIC # selecing a location for the proper design pattern
# MAGIC ds_user.write.mode("overwrite").format("delta").save("dbfs:/mnt/bs-production/delta/bronze_user/")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze_tb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Read from Data Lakehouse [Bronze Table] ~ bronze_user
# MAGIC %python
# MAGIC 
# MAGIC # read delta table [querying from data lakehouse]
# MAGIC bronze_user = spark.read.format("delta").load("dbfs:/mnt/bs-production/delta/bronze_user/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/udf.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC *starting this python process is expensive, but the real cost is in serializing the data to python. this is costly for two reasons: it is an expensive computation, but also, after the data enters python, spark cannot manage the memory of the worker. this means that you could potentially cause a worker to fail if it becomes resource constrained (because both the jvm and python are competing for memory on the same machine).*  
# MAGIC 
# MAGIC *recommend that you write your UDFs in **scala or java** the small amount of time it should take you to write the function in scala will always yield significant speed ups, and on top of that, you can still use the function from python*

# COMMAND ----------

# DBTITLE 1,PySpark - Creating a [Classifier Function]
# creating function in python to classify users
def usr_importance(average_stars,fans,review_count):
  if average_stars >=2 and fans > 10 and review_count >= 10:
    return "rockstar"
  if average_stars <2 and fans < 10 and review_count < 10:
    return "low"
  else:
    return "normal"

# registering udf function to spark's core
spark.udf.register("usr_importance", usr_importance) 

# COMMAND ----------

# DBTITLE 1,Classifier Function ~ DataFrame [ds_user]
# MAGIC %sql
# MAGIC 
# MAGIC -- acessing function written in python into sql engine
# MAGIC -- reading from disk [parquet file]
# MAGIC SELECT usr_importance(average_stars,fans,review_count), COUNT(*)
# MAGIC FROM bronze_user
# MAGIC GROUP BY usr_importance(average_stars,fans,review_count);

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver_tb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Applying [SQL] Transformations from Bronze to Silver
# MAGIC %python
# MAGIC 
# MAGIC # applying transformations
# MAGIC # saving the transforms into another dataframe
# MAGIC silver_user = spark.sql("""
# MAGIC   SELECT DISTINCT user_id, 
# MAGIC   name, 
# MAGIC   review_count, 
# MAGIC   useful, 
# MAGIC   fans, 
# MAGIC   average_stars, 
# MAGIC   usr_importance(average_stars,fans,review_count) AS importance, 
# MAGIC   yelping_since
# MAGIC   FROM ds_user
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # read dataframe
# MAGIC # note the new transform [importance]
# MAGIC display(silver_user)

# COMMAND ----------

# DBTITLE 1,Write Transformation into a New Delta Table [Silver]
# delta offers the following write modes
# append
# overwrite
silver_user.write.format("delta").mode("overwrite").save("dbfs:/mnt/bs-production/delta/silver_user/")

# COMMAND ----------

# DBTITLE 1,Read [Silver] Table from Data Lakehouse ~ silver_user
# MAGIC %python
# MAGIC 
# MAGIC # reading table from data lakehouse
# MAGIC # data lake - azure blob storage
# MAGIC # delta lake - data format
# MAGIC # delta lakehouse - design pattern for etl 
# MAGIC silver_user = spark.read.format("delta").load("dbfs:/mnt/bs-production/delta/silver_user/")

# COMMAND ----------

# DBTITLE 1,Count Rows [silver_user]
# 1.518.169
silver_user.count()

# COMMAND ----------

# DBTITLE 1,Delta Lakehouse - using SQL Engine to Query Table from Data Lake
# MAGIC %sql
# MAGIC 
# MAGIC -- tranparently querying delta lake table from storage
# MAGIC -- azure blob storage
# MAGIC SELECT * FROM delta.`/mnt/bs-production/delta/silver_user/`

# COMMAND ----------

