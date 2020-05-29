# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Azure SQL Database as a Data Warehouse [Dw]
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/spark_azure_sqldb.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > *download conector [maven] = https://search.maven.org/search?q=a:azure-sqldb-spark [azure-sqldb-spark-1.0.2.jar]*

# COMMAND ----------

# DBTITLE 1,Use Database and Show Tables ~ Gold_Reviews
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp;
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# DBTITLE 1,Reading Gold DataSet into a New Scala DataFrame
# MAGIC %scala
# MAGIC 
# MAGIC // reading delta table into a dataframe
# MAGIC val df_gold_reviews = spark.table("gold_reviews")
# MAGIC 
# MAGIC // display data
# MAGIC display(df_gold_reviews)

# COMMAND ----------

# DBTITLE 1,Reading Gold DataSet into a New Python DataFrame
# MAGIC %python
# MAGIC 
# MAGIC # reading using pyspark
# MAGIC df_gold_reviews = spark.table("gold_reviews")
# MAGIC 
# MAGIC # show table
# MAGIC display(df_gold_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="1200px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/db_model.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Azure SQL Database
# MAGIC 
# MAGIC > server name = onewaysolution.database.windows.net  
# MAGIC > service tier options = basic, standard, premium, general purpose [vcpu], hyperscale, business critical  
# MAGIC > selected tier mode - premium with 250 dtus  
# MAGIC > price = $930 USD per month

# COMMAND ----------

# DBTITLE 1,Creating Structure on Azure SQL Database
# MAGIC %sql
# MAGIC 
# MAGIC -- drop table if exists
# MAGIC DROP TABLE gold_reviews
# MAGIC 
# MAGIC -- create table for load
# MAGIC -- azure sql database
# MAGIC CREATE TABLE gold_reviews
# MAGIC (
# MAGIC   review_id VARCHAR(50),
# MAGIC   business_id VARCHAR(50),
# MAGIC   user_id VARCHAR(50),
# MAGIC   review_stars BIGINT,
# MAGIC   review_useful BIGINT,
# MAGIC   store_name VARCHAR(100),
# MAGIC   store_city VARCHAR(100),
# MAGIC   store_state VARCHAR(100),
# MAGIC   store_category VARCHAR(50),
# MAGIC   store_review_count BIGINT,
# MAGIC   store_stars FLOAT,
# MAGIC   user_name VARCHAR(50),
# MAGIC   user_average_stars FLOAT,
# MAGIC   user_importance VARCHAR(20)
# MAGIC );
# MAGIC 
# MAGIC -- create columstore index for olap queries
# MAGIC CREATE CLUSTERED COLUMNSTORE INDEX cci_gold_reviews ON gold_reviews
# MAGIC 
# MAGIC -- query data
# MAGIC SELECT * FROM gold_reviews  
# MAGIC SELECT COUNT(*) FROM gold_reviews

# COMMAND ----------

# DBTITLE 1,Verify Installed Class into Apache Spark Cluster
# MAGIC %scala
# MAGIC 
# MAGIC // validates sql server driver installation
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# DBTITLE 1,Configuring Variables to Send Data to Azure SQL Database
# MAGIC %scala
# MAGIC 
# MAGIC // specify hostname and database name
# MAGIC val jdbcHostname = "onewaysolution.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "sales"
# MAGIC 
# MAGIC // building url to push data
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // init properties
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC // user and password
# MAGIC // best practices to use a key vault
# MAGIC connectionProperties.put("user", s"luanmoreno")
# MAGIC connectionProperties.put("password", s"qq11ww22!!@@")

# COMMAND ----------

# DBTITLE 1,SQL Server Driver Class and Open Connection to Database
# MAGIC %scala
# MAGIC 
# MAGIC // try and init db connectivity
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# DBTITLE 1,Read Test ~ gold_reviews
# MAGIC %scala
# MAGIC 
# MAGIC // read table from azure sql database
# MAGIC val read_df_gold_reviews = spark.read.jdbc(jdbcUrl, "gold_reviews", connectionProperties)

# COMMAND ----------

# DBTITLE 1,Table Truncation ~ gold_reviews
# MAGIC %scala
# MAGIC 
# MAGIC // import classes to query sql server
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC // submit truncate operation
# MAGIC val query = """
# MAGIC               |TRUNCATE TABLE gold_reviews;
# MAGIC             """.stripMargin
# MAGIC 
# MAGIC // config values for connectivity
# MAGIC val config = Config(Map(
# MAGIC   "url"               -> "onewaysolution.database.windows.net",
# MAGIC   "databaseName"      -> "sales",
# MAGIC   "user"              -> "luanmoreno",
# MAGIC   "password"          -> "qq11ww22!!@@",
# MAGIC   "queryCustom"       -> query
# MAGIC ))
# MAGIC 
# MAGIC // init config
# MAGIC sqlContext.sqlDBQuery(config)

# COMMAND ----------

# DBTITLE 1,Mapping Conversion from Apache Spark to JDBC
# MAGIC %md
# MAGIC 
# MAGIC > https://db.apache.org/ojb/docu/guides/jdbc-types.html

# COMMAND ----------

# DBTITLE 1,Insert into Azure SQL DB = [39.552.954]
# MAGIC %scala
# MAGIC 
# MAGIC // ~ 18 minutes
# MAGIC 
# MAGIC // libraries import
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC  
# MAGIC // send dataframe to azure sql database
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> "onewaysolution.database.windows.net",
# MAGIC   "databaseName" -> "sales",
# MAGIC   "dbTable"      -> "gold_reviews",
# MAGIC   "user"         -> "luanmoreno",
# MAGIC   "password"     -> "qq11ww22!!@@"
# MAGIC ))
# MAGIC 
# MAGIC // init process save mode
# MAGIC df_gold_reviews.write.mode(SaveMode.Overwrite).sqlDB(config)

# COMMAND ----------

# DBTITLE 1,Insert into Azure SQL DB = [39.552.954] with Bulk Copy Operation
# MAGIC %scala
# MAGIC 
# MAGIC // import libraries
# MAGIC // bulk copy operation 
# MAGIC // 39.552.954 ~ 17.47 minutes
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC // config values
# MAGIC // set up batch size and table lock
# MAGIC val config = Config(Map(
# MAGIC   "url"               -> "onewaysolution.database.windows.net",
# MAGIC   "databaseName"      -> "sales",
# MAGIC   "user"              -> "luanmoreno",
# MAGIC   "password"          -> "qq11ww22!!@@", 
# MAGIC   "dbTable"           -> "gold_reviews", 
# MAGIC   "bulkCopyBatchSize" -> "2500",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyTimeout"   -> "800"
# MAGIC ))
# MAGIC 
# MAGIC // init proccess
# MAGIC df_gold_reviews.bulkCopyToSqlDB(config)

# COMMAND ----------

# DBTITLE 1,Azure SQL Database ~ [Query Editor] 
# MAGIC %sql
# MAGIC 
# MAGIC -- olap query with
# MAGIC -- group and order by
# MAGIC -- tier = premium p2: 250 dtus
# MAGIC -- 39.552.954 
# MAGIC -- ? seconds
# MAGIC 
# MAGIC SELECT TOP 10  
# MAGIC     store_name,     
# MAGIC     store_city,   
# MAGIC     COUNT(*) AS Q  
# MAGIC FROM dbo.gold_reviews  
# MAGIC WHERE user_importance = 'rockstar'  
# MAGIC GROUP BY store_name, store_city  
# MAGIC ORDER BY Q DESC  

# COMMAND ----------

