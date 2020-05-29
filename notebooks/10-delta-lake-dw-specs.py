# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Delta Lake as a Data Warehouse [Dw]
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/spark_delta.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Use Database and Show Tables ~ Gold_Reviews
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp;
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# DBTITLE 1,Query Delta Lake Table ~ gold_reviews
# MAGIC %sql
# MAGIC 
# MAGIC -- query table structure
# MAGIC -- count ~ 39.552.954
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta_features.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Describe Table History ~ Metadata Information
# MAGIC %sql
# MAGIC 
# MAGIC -- capturing metadata information
# MAGIC DESCRIBE HISTORY gold_reviews

# COMMAND ----------

# DBTITLE 1,Time Travel Feature - Table Version
# MAGIC %sql
# MAGIC 
# MAGIC -- set the table version
# MAGIC SELECT * 
# MAGIC FROM gold_reviews VERSION AS OF 0

# COMMAND ----------

# DBTITLE 1,Delta Lake Table Optimization ~ gold_reviews
# MAGIC %sql
# MAGIC 
# MAGIC -- optimize delta lake table
# MAGIC -- fast aggregations and query
# MAGIC OPTIMIZE gold_reviews

# COMMAND ----------

# DBTITLE 1,Delta Lake Table Optimization ~ gold_reviews ~[z-ordering]
# MAGIC %sql
# MAGIC 
# MAGIC -- technique to colocate related info in the same set of files
# MAGIC -- design to optimize queries for specific columns
# MAGIC -- not using partitioning yet
# MAGIC OPTIMIZE gold_reviews ZORDER BY (store_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- delta lake with z-ordering 
# MAGIC -- 31.077.321
# MAGIC 
# MAGIC SELECT store_name, 
# MAGIC        store_city, 
# MAGIC        COUNT(*) AS Q
# MAGIC FROM gold_reviews
# MAGIC WHERE user_importance = "rockstar"
# MAGIC GROUP BY store_name, store_city
# MAGIC ORDER BY Q DESC
# MAGIC LIMIT 10

# COMMAND ----------

