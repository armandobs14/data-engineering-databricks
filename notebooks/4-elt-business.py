# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Building a [Data Lakehouse] using Batch-ETL [Business]
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="600px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dl_batch.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > spark-sql built in functions - https://spark.apache.org/docs/2.3.0/api/sql/index.html  

# COMMAND ----------

# DBTITLE 1,Create & Use a Database
# MAGIC %sql
# MAGIC 
# MAGIC -- creating a database for logical organization
# MAGIC CREATE DATABASE IF NOT EXISTS Yelp;
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,List Business File[s]
# MAGIC %fs ls "dbfs:/mnt/bs-production/pq_business/"

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

# DBTITLE 1,Reading into a DataFrame [spark.read.parquet]
ds_business = spark.read.parquet("/mnt/bs-production/pq_business/business.parquet")
display(ds_business)

# COMMAND ----------

# DBTITLE 1,Showing Schema of DataFrame
ds_business.printSchema()

# COMMAND ----------

# DBTITLE 1,Registering DataFrame [tmp_business] into Spark SQL Context
# command that registers into the sql engine
sqlContext.registerDataFrameAsTable(ds_business, "tmp_business")

# COMMAND ----------

# DBTITLE 1,Query Data using Spark SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM tmp_business
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Describe DataFrame Info
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tmp_business

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
# MAGIC <img width="200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze_tb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Writing DataFrame into Delta Table [Data Lakehouse] = bronze_business
ds_business.write.format("delta").mode("overwrite").save("dbfs:/mnt/bs-production/delta/bronze_business/")

# COMMAND ----------

# DBTITLE 1,Reading Delta Table to DataFrame [bronze_business]
ds_business = spark.read.format("delta").load("dbfs:/mnt/bs-production/delta/bronze_business/")

# COMMAND ----------

# DBTITLE 1,Select Columns using PySpark
# selecting columns using python (pyspark)
ds_business_select = ds_business.select(['business_id','name','categories','city','state','address','latitude','longitude','review_count','stars'])
display(ds_business_select)

# COMMAND ----------

# DBTITLE 1,SparkSQL - Reviews per City
# MAGIC %sql
# MAGIC 
# MAGIC SELECT city, SUM(review_count)
# MAGIC FROM delta.`/mnt/bs-production/delta/bronze_business/`
# MAGIC GROUP BY city
# MAGIC ORDER BY SUM(review_count) DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH reviews AS 
# MAGIC (
# MAGIC SELECT city, SUM(review_count) AS review_count
# MAGIC FROM delta.`/mnt/bs-production/delta/bronze_business/`
# MAGIC GROUP BY city
# MAGIC HAVING SUM(review_count) > 1000
# MAGIC )
# MAGIC SELECT rv.city, AVG(b.stars) AS stars
# MAGIC FROM reviews AS rv
# MAGIC INNER JOIN delta.`/mnt/bs-production/delta/bronze_business/` AS b
# MAGIC ON rv.city = b.city
# MAGIC GROUP BY rv.city
# MAGIC ORDER BY stars DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT state,
# MAGIC        AVG(stars)
# MAGIC FROM delta.`/mnt/bs-production/delta/bronze_business/`
# MAGIC WHERE state NOT IN ('11', 'WAR', '01', 'NYK', 'NW', 'HH', 'QC', 'B', 'BC', 'M', 'V', 'BY', '6', 
# MAGIC 'SP', 'O', 'PO', 'XMS', 'C', 'XGM', 'CC', 'VS', 'RP', 'AG', 'SG', 'TAM', 'ON', 'AB', 'G', 'CS', 'RCC', 'HU', '10', 
# MAGIC '4', 'NI', 'NLK', 'HE', 'CMA', 'LU', 'WHT', '45', 'ST', 'CRF')
# MAGIC GROUP BY state

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver_tb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Creating Silver Tables [Transformations]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver_business;
# MAGIC 
# MAGIC CREATE TABLE silver_business
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT business_id,
# MAGIC        name,
# MAGIC        city, 
# MAGIC        state,
# MAGIC        regexp_extract(categories,"^(.+?),") AS category,
# MAGIC        categories AS subcategories,
# MAGIC        review_count,
# MAGIC        stars
# MAGIC FROM delta.`/mnt/bs-production/delta/bronze_business/`

# COMMAND ----------

# DBTITLE 1,Query Newly Created Delta Table
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM silver_business

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_business
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Optimize Delta Table for Query
display(spark.sql("OPTIMIZE silver_business"))

# COMMAND ----------

# DBTITLE 1,Verificando Nova Tabela Delta = [yelp_delta_business]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_business

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM silver_business

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/delta-cache

# COMMAND ----------

