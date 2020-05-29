# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Building a [Data Lakehouse] ~ Gold Dataset
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1300px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta_lake_arch.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Inspect Tables on Database
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp;
# MAGIC 
# MAGIC SHOW TABLES

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
# MAGIC <img width="200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver_tb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Silver Table = User [Batch]
# MAGIC %sql
# MAGIC 
# MAGIC --1.518.169
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/mnt/bs-production/delta/silver_user/`

# COMMAND ----------

# DBTITLE 1,Silver Table = Business [Batch]
# MAGIC %sql
# MAGIC 
# MAGIC --377.186
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_business

# COMMAND ----------

# DBTITLE 1,Silver Table = Reviews [Stream]
# MAGIC %sql
# MAGIC 
# MAGIC --19.776.477
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/stream_ss_silver_reviews`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dw_flow.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Gold DataSet for Data [Serving Layer]
# MAGIC %sql
# MAGIC 
# MAGIC -- time spent = 2.30 minutes
# MAGIC -- total rows = 39.552.954
# MAGIC 
# MAGIC -- remove if exists
# MAGIC DROP TABLE IF EXISTS gold_reviews;
# MAGIC 
# MAGIC -- create gold dataset
# MAGIC -- users, business and reviews
# MAGIC -- batch and streaming join
# MAGIC CREATE TABLE gold_reviews
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT r.review_id, 
# MAGIC        r.business_id, 
# MAGIC        r.user_id, 
# MAGIC        r.stars AS review_stars, 
# MAGIC        r.useful AS review_useful, 
# MAGIC        
# MAGIC        b.name AS store_name, 
# MAGIC        b.city AS store_city, 
# MAGIC        b.state AS store_state, 
# MAGIC        b.category AS store_category, 
# MAGIC        b.review_count AS store_review_count, 
# MAGIC        b.stars AS store_stars, 
# MAGIC        
# MAGIC        u.name AS user_name, 
# MAGIC        u.average_stars AS user_average_stars, 
# MAGIC        u.importance AS user_importance
# MAGIC        
# MAGIC FROM delta.`/delta/stream_ss_silver_reviews` AS r
# MAGIC INNER JOIN silver_business AS b
# MAGIC ON r.business_id = b.business_id
# MAGIC INNER JOIN delta.`/mnt/bs-production/delta/silver_user/` AS u
# MAGIC ON r.user_id = u.user_id

# COMMAND ----------

# DBTITLE 1,Select Gold DataSet Data
# MAGIC %sql
# MAGIC 
# MAGIC -- 16.57 seconds for 39.552.954 rows with order by
# MAGIC -- delta table performance without partitioning
# MAGIC SELECT *
# MAGIC FROM gold_reviews
# MAGIC ORDER BY user_importance

# COMMAND ----------

