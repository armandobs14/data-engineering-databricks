# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Converting JSON to Apache Parquet
# MAGIC > since Data Lake has any type of data one of the best practices specially for Spark is to transform  
# MAGIC > the data format before processing, this will allow a better and fast execution for a lot of reasons.  
# MAGIC > that we will address in this notebook.  
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="700px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dl_pq.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > more info about this data format **Apache Parquet**  
# MAGIC > https://parquet.apache.org/  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="50px" src="https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/apache-parquet.png">
# MAGIC 
# MAGIC > **apache parquet** is a columnar data format type that accelerates and optimizes reads and writes.   
# MAGIC > it's suitable for Big Data Processing due some interesting capabilities that will be explained more shortly.

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/user/2019/"

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/business/2019/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > on the folder above, all the files are in the JSON format, the go to for Apache Spark processing is to utilize Parquet, one of the reasons is that since   
# MAGIC it's a columnar storage with an embedded schema, Spark can compute and process much fast than normal files suchs as - TSV, CSV, JSON.  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > Apache Spark is not limited to Apache Parquet but as explained before is the perfect match whenever you want to scale your processing.  
# MAGIC > Have in mind that Spark can easily read the following types as well using the newest DataFrame API = **(spark.read.)**
# MAGIC <br>
# MAGIC 
# MAGIC * Parquet
# MAGIC * JSON
# MAGIC * CSV
# MAGIC * ORC
# MAGIC * JDBC

# COMMAND ----------

# DBTITLE 1,Reading JSON Files using DataFrame [API]
df_business = spark.read.json("dbfs:/mnt/bs-stg-files/business/2019/*.json")
df_user = spark.read.json("dbfs:/mnt/bs-stg-files/user/2019/*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > the data will be loaded into a 2 different dataframes inside of Spark.  
# MAGIC > **dataframe** is a Spark structured similar to a database table in a relational system but spread across machines (workers)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > one of the greatest features of Spark is the ability to infer schemas whenever is possible, this feature is available under the **dataframe** api  
# MAGIC > **spark.read.json** automatically infers the JSON schema, code also available on **sql**  
# MAGIC > https://spark.apache.org/docs/latest/sql-data-sources-json.html
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > CREATE TEMPORARY VIEW jsonTable  
# MAGIC   USING org.apache.spark.sql.json  
# MAGIC   OPTIONS (path "examples/src/main/resources/people.json")  
# MAGIC 
# MAGIC > SELECT * FROM jsonTable

# COMMAND ----------

# DBTITLE 1,DataFrame = users
display(df_user)

# COMMAND ----------

# DBTITLE 1,DataFrame = business
display(df_business)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Benefits to Convert to Apache Parquet
# MAGIC 
# MAGIC > **1** - acept nested and complex structures  
# MAGIC > **2** - compression-efficient and schema encoding  
# MAGIC > **3** - allows less disk read and fast queries  
# MAGIC > **4** - less I/O overhead due the batch-read ability  
# MAGIC > **5** - increases the scan read  

# COMMAND ----------

# DBTITLE 1,Reading DataFrames & Writing in [Production Zone] = Apache Parquet Format
df_business.write.mode("overwrite").parquet("/mnt/bs-production/pq_business/business.parquet")
df_user.write.mode("overwrite").parquet("/mnt/bs-production/pq_user/user.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > acessing converted files from *json* to *parquet*

# COMMAND ----------

# DBTITLE 1,LS [Production Zone] on Data Lake [Azure Blob Storage]
# MAGIC %fs ls "dbfs:/mnt/bs-production/"

# COMMAND ----------

