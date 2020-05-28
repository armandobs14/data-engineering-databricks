# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Databricks e Azure Synapse Analytics [Inserting Data into Database]
# MAGIC 
# MAGIC > conectando no Azure SQL Database  
# MAGIC > inserindo tabelas
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > **connection string:**  
# MAGIC > Server=tcp:owshq.database.windows.net,1433;Initial Catalog=owshq;Persist Security Info=False;User ID=luanmoreno;Password={your_password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;
# MAGIC 
# MAGIC > {your_password_here} = qq11ww22!!@@  
# MAGIC 
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Utilizando Banco de Dados [Spark]
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,Mostrando Tabelas
# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# DBTITLE 1,Lendo DataSet Gold em Scala
# MAGIC %scala
# MAGIC 
# MAGIC val df_gold_reviews_synapse = spark.table("gold_reviews")

# COMMAND ----------

# DBTITLE 1,Verificando Classe em Scala para Inserção dos Dados
# MAGIC %scala
# MAGIC 
# MAGIC Class.forName("com.databricks.spark.sqldw.DefaultSource")

# COMMAND ----------

# DBTITLE 1,Declarando Variável
# MAGIC %scala
# MAGIC 
# MAGIC val blobStorage = "brzluanmoreno.blob.core.windows.net"
# MAGIC val blobContainer = "sqldw"
# MAGIC val blobAccessKey =  "j+LfL1qL8y6GxUFkCriWknFUPzaWeyJKETnqfHVRG9kKvGA5Wsd4xGi8tQ8QHrvGFtueDdtuigibtyJxpCHCwg=="

# COMMAND ----------

# DBTITLE 1,Local Temporário para Polybase
# MAGIC %scala
# MAGIC 
# MAGIC val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

# COMMAND ----------

# DBTITLE 1,Informações do Blob Storage
# MAGIC %scala
# MAGIC 
# MAGIC val acntInfo = "fs.azure.account.key."+ blobStorage
# MAGIC sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > Server=tcp:owshq.database.windows.net,1433;Initial Catalog=owshq;Persist Security Info=False;User ID=luanmoreno;Password={your_password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;

# COMMAND ----------

# DBTITLE 1,Configurações de Acesso para o Azure Synapse Analytics (Azure SQL DB)
# MAGIC %scala
# MAGIC 
# MAGIC  val dwDatabase = "owshq"
<<<<<<< HEAD
# MAGIC  val dwServer = "owshq" 
=======
# MAGIC  val dwServer = "onewaysolution" 
>>>>>>> master
# MAGIC  val dwUser = "luanmoreno"
# MAGIC  val dwPass = "qq11ww22!!@@"
# MAGIC  val dwJdbcPort =  "1433"
# MAGIC  val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC  val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ".database.windows.net:" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
# MAGIC  val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ".database.windows.net:" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.conf.set(
# MAGIC    "spark.sql.parquet.writeLegacyFormat",
# MAGIC    "true")

# COMMAND ----------

# DBTITLE 1,Processo Obrigatório - [1] 
# MAGIC %md
# MAGIC 
# MAGIC > CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'qq11ww22!!@@';  

# COMMAND ----------

<<<<<<< HEAD
# DBTITLE 1,Inserindo Dados com Scala = 1.43 minutes
=======
# MAGIC %md
# MAGIC 
# MAGIC ### Gen2: DW3000c
# MAGIC ### 45.30 USD per Hour

# COMMAND ----------

# DBTITLE 1,Inserindo Dados com Scala = ? minutes [32 milhões]
>>>>>>> master
# MAGIC %scala
# MAGIC 
# MAGIC df_gold_reviews_synapse.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "ft_reviews").option("forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()

# COMMAND ----------

<<<<<<< HEAD
# DBTITLE 1,Inserindo Dados com Spark SQL = 1.11 minutes
=======
# DBTITLE 1,Inserindo Dados com Spark SQL = ? minutes [32 milhões]
>>>>>>> master
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ft_tb_reviews;
# MAGIC 
# MAGIC CREATE TABLE ft_tb_reviews
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
<<<<<<< HEAD
# MAGIC   url 'jdbc:sqlserver://owshq.database.windows.net:1433;database=owshq;user=luanmoreno@owshq;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
=======
# MAGIC   url 'jdbc:sqlserver://onewaysolution.database.windows.net:1433;database=owshq;user=luanmoreno@onewaysolution;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
>>>>>>> master
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'ft_tb_reviews',
# MAGIC   tempDir 'wasbs://sqldw@brzluanmoreno.blob.core.windows.net/tempDirs'
# MAGIC )
# MAGIC AS SELECT * FROM gold_reviews

# COMMAND ----------

<<<<<<< HEAD
=======
# MAGIC %md 
# MAGIC 
# MAGIC ### Query for Azure Synapse Analytics [Azure SQL Data Warehouse]
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

>>>>>>> master
