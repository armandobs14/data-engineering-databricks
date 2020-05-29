# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Azure Synapse Analytics as a Data Warehouse [Dw]
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/spark_synapse.png'>
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

# DBTITLE 1,Reading Gold DataSet into a DataFrame
# MAGIC %scala
# MAGIC 
# MAGIC // reading into a new dataframe
# MAGIC val df_gold_reviews_synapse = spark.table("gold_reviews")

# COMMAND ----------

# DBTITLE 1,Verify Class Installed on Apache Spark env
# MAGIC %scala
# MAGIC 
# MAGIC // validate class installed on cluster
# MAGIC Class.forName("com.databricks.spark.sqldw.DefaultSource")

# COMMAND ----------

# DBTITLE 1,Declaring Variables 
# MAGIC %scala
# MAGIC 
# MAGIC // set up azure blob storage account
# MAGIC // to hide sensitive information use key vault
# MAGIC val blobStorage = "brzluanmoreno.blob.core.windows.net"
# MAGIC val blobContainer = "sqldw"
# MAGIC val blobAccessKey =  "j+LfL1qL8y6GxUFkCriWknFUPzaWeyJKETnqfHVRG9kKvGA5Wsd4xGi8tQ8QHrvGFtueDdtuigibtyJxpCHCwg=="

# COMMAND ----------

# DBTITLE 1,Local Temporário para Polybase
# MAGIC %scala
# MAGIC 
# MAGIC // declaring location where stage data will reside
# MAGIC // polybase is the fastest way to send data to synapse
# MAGIC val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

# COMMAND ----------

# DBTITLE 1,Set Blob Storage Account
# MAGIC %scala
# MAGIC 
# MAGIC // azure blob storage information
# MAGIC val acntInfo = "fs.azure.account.key."+ blobStorage
# MAGIC sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > Server=tcp:owshq.database.windows.net,1433;Initial Catalog=owshq;Persist Security Info=False;User ID=luanmoreno;Password={your_password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;

# COMMAND ----------

# DBTITLE 1,Configuring Access to Azure Synapse Analytics Cluster
# MAGIC %scala
# MAGIC 
# MAGIC // setting up info
# MAGIC // connect to azure synapse analytics
# MAGIC // pass the database and server info
# MAGIC val dwDatabase = "owshq"
# MAGIC val dwServer = "onewaysolution" 
# MAGIC val dwUser = "luanmoreno"
# MAGIC val dwPass = "qq11ww22!!@@"
# MAGIC val dwJdbcPort =  "1433"
# MAGIC val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ".database.windows.net:" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
# MAGIC val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ".database.windows.net:" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // set up parquet writer ~ fast interaction with engine
# MAGIC spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")

# COMMAND ----------

# DBTITLE 1,Required Step on Azure Synapse Analytics ~ Create a Master Key
# MAGIC %md
# MAGIC 
# MAGIC > CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'qq11ww22!!@@';  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/synapse_analytics_info.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Azure Synapse Analytics
# MAGIC 
# MAGIC > server name = onewaysolution.database.windows.net  
# MAGIC > selected tier mode - gen2: dw200c  
# MAGIC > concurrency = 8  
# MAGIC > price = $3 USD per hour = $1,584.00 per month 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > gen2: dw3000c  
# MAGIC > price - $45.30 usd per hour & $23,918.40 per month

# COMMAND ----------

# DBTITLE 1,Insert into Azure Synapse Analytics = [39.552.954]
# MAGIC %scala
# MAGIC 
# MAGIC // insert rows into synapse analytics
# MAGIC // uses polybase = stage data into blob storage
# MAGIC // send data directly to the slave nodes
# MAGIC // time spent = ??
# MAGIC 
# MAGIC df_gold_reviews_synapse.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "ft_reviews").option("forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Azure Synapse Analytics ~ [Query Editor] 
# MAGIC %sql
# MAGIC 
# MAGIC -- olap query with
# MAGIC -- group and order by
# MAGIC -- tier = gen2: dw3000c
# MAGIC -- 39.552.954 
# MAGIC -- ? seconds
# MAGIC 
# MAGIC SELECT TOP 10  
# MAGIC     store_name,     
# MAGIC     store_city,   
# MAGIC     COUNT(*) AS Q  
# MAGIC FROM dbo.ft_reviews  
# MAGIC WHERE user_importance = 'rockstar'  
# MAGIC GROUP BY store_name, store_city  
# MAGIC ORDER BY Q DESC  

# COMMAND ----------

