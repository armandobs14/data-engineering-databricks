# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Building a [Data Lakehouse] using Near Real-Time ETL [Review] using [Structured Streaming]
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="600px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dl_batch.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/streaming_sql.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Spark SQL ~ [Structured Streaming]
# MAGIC > *spark-sql has the ability to perform streaming, a structured one to make it possible*  
# MAGIC > *to ingest data from different sources either - file, kafka, delta using the sql engine as underlying mechanism*  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC #### Structured Streaming [2016]
# MAGIC 
# MAGIC > *stream processing on spark SQL engine*  
# MAGIC > *deal with complex data & complex workloads*  
# MAGIC > *rich ecosystem of data sources*  
# MAGIC <br>
# MAGIC   
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/structured_streaming_1.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Use a Database
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,List Files [Data Lake] - Landing Zone
# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/review/2019"

# COMMAND ----------

# DBTITLE 1,Housekeeping [1] ~ Removing Delta Checkpoints
# MAGIC %fs 
# MAGIC 
# MAGIC rm -r "dbfs:/delta/reviews/_checkpoints/stream_ss_bronze_reviews"

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC rm -r "dbfs:/delta/reviews/_checkpoints/stream_ss_silver_reviews"

# COMMAND ----------

# DBTITLE 1,Housekeeping [2] ~ Deleting Delta Table Data
# MAGIC %sql 
# MAGIC 
# MAGIC DELETE FROM delta.`/delta/stream_ss_bronze_reviews`;
# MAGIC VACUUM delta.`/delta/stream_ss_bronze_reviews`;
# MAGIC 
# MAGIC DELETE FROM delta.`/delta/stream_ss_silver_reviews`;
# MAGIC VACUUM delta.`/delta/stream_ss_silver_reviews`;

# COMMAND ----------

# DBTITLE 1,Specifying Structured Streaming [Python] ~ [Explicit Schema]
# import libraries
from pyspark.sql.types import *

# define input location
inputPath = "dbfs:/mnt/bs-stg-files/review/2019"

# structured streaming
# must specify a schema

# json schema of the file (yelp_academic_dataset_review)
jsonSchema = StructType(
[
 StructField('business_id', StringType(), True), 
 StructField('cool', LongType(), True), 
 StructField('date', StringType(), True), 
 StructField('funny', LongType(), True), 
 StructField('review_id', StringType(), True), 
 StructField('stars', LongType(), True), 
 StructField('text', StringType(), True), 
 StructField('useful', LongType(), True), 
 StructField('user_id', StringType(), True)
]
)

# define structured streaming
df_ss_bronze_reviews = (
   spark
  .readStream
  .schema(jsonSchema)
  .option("maxFilesPerTrigger", 1)
  .json(inputPath)
)

# verify stream
df_ss_bronze_reviews.isStreaming

# COMMAND ----------

# DBTITLE 1,Specifying Structured Streaming [Python] ~ [Implicit Schema]
# MAGIC %python
# MAGIC 
# MAGIC # reading json file from blob storage
# MAGIC get_sch_reviews = spark.read.json("dbfs:/mnt/bs-stg-files/review/2019")
# MAGIC 
# MAGIC # make it available on sql engine
# MAGIC get_sch_reviews.createOrReplaceTempView("reviews") 
# MAGIC 
# MAGIC # set static schema
# MAGIC staticSchema = get_sch_reviews.schema

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

# DBTITLE 1,Read Files from Azure Blob Storage [Data Lake] ~ Location
# define structured streaming
# get static schema
# number of files per read
# location on data lake watching
df_ss_bronze_reviews = spark.readStream \
  .schema(staticSchema) \
  .format("json") \
  .option("header", "true") \
  .option("maxFilesPerTrigger", "1") \
  .load("dbfs:/mnt/bs-stg-files/review/2019/*.json") 

# verify if this is a streaming dataframe
df_ss_bronze_reviews.isStreaming

# COMMAND ----------

# DBTITLE 1,Write Stream to a Delta Lake Table
# reading previous dataframe 
# writing events in near real-time into delta
# appending data automatically
# set checkpoint for fast retrieval
df_ss_bronze_reviews \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/delta/reviews/_checkpoints/stream_ss_bronze_reviews") \
  .start("/delta/stream_ss_bronze_reviews") \

# COMMAND ----------

# DBTITLE 1,[Count] ~ Events
# MAGIC %sql
# MAGIC 
# MAGIC -- 5.996.996
# MAGIC -- 11.993.992
# MAGIC -- 17.990.988
# MAGIC -- 23.987.984
# MAGIC -- 35.981.976
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_ss_bronze_reviews`

# COMMAND ----------

# DBTITLE 1,[Read] ~ Events
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/stream_ss_bronze_reviews`
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver_tb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Structured Streaming - Init New Stream from Delta Table with [Transformation]
# creating new dataframe
# reading delta lake bronze table
# applying transformation - select and where clause
# writing into a new delta lake table
stream_silver_reviews = spark.readStream \
  .format("delta") \
  .load("/delta/stream_ss_bronze_reviews") \
  .select("business_id", "user_id", "review_id", "cool", "funny", "useful", "stars", "date") \
  .where("useful > 0") \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/delta/reviews/_checkpoints/stream_ss_silver_reviews") \
  .start("/delta/stream_ss_silver_reviews")

# COMMAND ----------

# DBTITLE 1,[Count] ~ Events
# MAGIC %sql
# MAGIC 
# MAGIC -- 2.825.211
# MAGIC -- 14.126.055
# MAGIC -- 16.951.266
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_ss_silver_reviews`

# COMMAND ----------

# DBTITLE 1,[Read] ~ Events
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/stream_ss_silver_reviews`
# MAGIC LIMIT 100

# COMMAND ----------

