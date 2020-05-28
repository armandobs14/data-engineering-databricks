# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # The Layered Big Data Architecture
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1400" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/layered.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Data Lake = Production Zone [mount]
# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://stg-advworks@brzluanmoreno.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/adv-works-stg-files",
# MAGIC   extra_configs = {"fs.azure.account.key.brzluanmoreno.blob.core.windows.net":dbutils.secrets.get(scope = "az-blob-storage-luanmoreno", key = "key-az-blob-storage")})

# COMMAND ----------

# DBTITLE 1,Lendo Arquivos do Data Lake [Azure Blob Storage]
# MAGIC %fs ls "dbfs:/mnt/adv-works-stg-files/AdventureWorks"

# COMMAND ----------

# DBTITLE 1,Criação
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS AdventureWorks

# COMMAND ----------

# DBTITLE 1,Usando
# MAGIC %sql
# MAGIC 
# MAGIC USE AdventureWorks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/parquet-json.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Carregando DataFrame [DF] do Data Lake
<<<<<<< HEAD
# sales data
df_sales_order_header = spark.read.parquet("dbfs:/mnt/adv-works-stg-files/AdventureWorks/Sales_SalesOrderHeader.parquet")
df_sales_order_detail = spark.read.parquet("dbfs:/mnt/adv-works-stg-files/AdventureWorks/Sales_SalesOrderDetail.parquet")

# product data
df_product = spark.read.parquet("dbfs:/mnt/adv-works-stg-files/AdventureWorks/Production_Product.parquet")
=======
# MAGIC %python
# MAGIC 
# MAGIC # sales data
# MAGIC df_sales_order_header = spark.read.parquet("dbfs:/mnt/adv-works-stg-files/AdventureWorks/Sales_SalesOrderHeader.parquet")
# MAGIC df_sales_order_detail = spark.read.parquet("dbfs:/mnt/adv-works-stg-files/AdventureWorks/Sales_SalesOrderDetail.parquet")
# MAGIC 
# MAGIC # product data
# MAGIC df_product = spark.read.parquet("dbfs:/mnt/adv-works-stg-files/AdventureWorks/Production_Product.parquet")
>>>>>>> master

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1400" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta-arch.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Stage 1 - [Bronze Table]
# MAGIC 
# MAGIC > *lembre-se que esse layer não possui modificações, o dado vem direto do Data Lake para o Delta Lake*
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > *using **append mode** you can atomically add new data to an existing delta table*  
# MAGIC >  *using **overwrite mode** to atomically replace all of the data in a table*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > estamos lendo as informações do **Data Lake** e transportando para o **Delta Lake**, assim temos a camada de entrada *stage*  
# MAGIC > além disso ganhamos a velocidade e otimizações vindas do Delta Lake para consulta de dados

# COMMAND ----------

# DBTITLE 1,Escrevendo [DataFrame] para Delta Lake
# sales data
df_sales_order_header.write.format("delta").mode("append").save("/delta/bronze_sales_order_header/")
df_sales_order_detail.write.format("delta").mode("append").save("/delta/bronze_sales_order_detail/")

# product data
df_product.write.format("delta").mode("append").save("/delta/bronze_product/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="1000px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/mysql-case.png'>
# MAGIC   
# MAGIC <br>

# COMMAND ----------

<<<<<<< HEAD
=======
# MAGIC %md
# MAGIC 
# MAGIC ## MySQL  
# MAGIC > server name = owshqmysqldb  
# MAGIC > mysql -u root  
# MAGIC > files location = /home/luanmoreno/script-repo/files  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > DELETE FROM sales.sales_order_header;  
# MAGIC > USE sales;  
# MAGIC > SELECT COUNT(*) FROM sales_order_header;  
# MAGIC > SHOW COLUMNS FROM sales_order_header;
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > 
# MAGIC LOAD DATA LOCAL INFILE '/home/luanmoreno/script-repo/files/mysql_sales_salesorderheader_1.csv'  
# MAGIC INTO TABLE sales_order_header  
# MAGIC FIELDS TERMINATED BY ','  
# MAGIC IGNORE 1 ROWS;  
# MAGIC 
# MAGIC > 
# MAGIC LOAD DATA LOCAL INFILE '/home/luanmoreno/script-repo/files/mysql_sales_salesorderheader_2.csv'  
# MAGIC INTO TABLE sales_order_header  
# MAGIC FIELDS TERMINATED BY ','  
# MAGIC IGNORE 1 ROWS;  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > SELECT COUNT(*) FROM sales_order_header;  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC >  
# MAGIC LOAD DATA LOCAL INFILE '/home/luanmoreno/script-repo/files/mysql_sales_salesorderheader_row_1.csv'  
# MAGIC INTO TABLE sales_order_header  
# MAGIC FIELDS TERMINATED BY ','  
# MAGIC IGNORE 1 ROWS;  
# MAGIC 
# MAGIC >  
# MAGIC LOAD DATA LOCAL INFILE '/home/luanmoreno/script-repo/files/mysql_sales_salesorderheader_row_2.csv'  
# MAGIC INTO TABLE sales_order_header  
# MAGIC FIELDS TERMINATED BY ','  
# MAGIC IGNORE 1 ROWS;  
# MAGIC 
# MAGIC >  
# MAGIC LOAD DATA LOCAL INFILE '/home/luanmoreno/script-repo/files/mysql_sales_salesorderheader_row_3.csv'  
# MAGIC INTO TABLE sales_order_header  
# MAGIC FIELDS TERMINATED BY ','  
# MAGIC IGNORE 1 ROWS;  
# MAGIC 
# MAGIC >  
# MAGIC LOAD DATA LOCAL INFILE '/home/luanmoreno/script-repo/files/mysql_sales_salesorderheader_row_4.csv'  
# MAGIC INTO TABLE sales_order_header  
# MAGIC FIELDS TERMINATED BY ','  
# MAGIC IGNORE 1 ROWS;  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ## Apache Kafka  
# MAGIC > 
# MAGIC curl -s "http://localhost:8083/connectors?expand=status" | \  
# MAGIC jq 'to_entries[] | [.key, .value.status.connector.state,.value.status.tasks[].state]|join(":|:")' | \  
# MAGIC column -s : -t| sed 's/\"//g'| sort  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > deploy connector  
# MAGIC curl -X POST -H "Content-Type: application/json" --data '{ "name": "src-mysql-sales-order-header", "config": { "connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector", "key.converter":"io.confluent.connect.avro.AvroConverter", "key.converter.schema.registry.url":"http://localhost:8081", "value.converter":"io.confluent.connect.avro.AvroConverter", "value.converter.schema.registry.url":"http://localhost:8081", "connection.url":"jdbc:mysql://owshqmysqldb.eastus2.cloudapp.azure.com:3306/sales?user=kafka_reader&password=demo@pass123", "connection.attempts":"2", "query":"SELECT * FROM sales_order_header", "mode":"incrementing", "topic.prefix":"src-mysql--sales-order-header", "incrementing.column.name":"SalesOrderID", "tasks.max":"2", "validate.non.null":"false" } }' http://localhost:8083/connectors  
# MAGIC 
# MAGIC > connector task  
# MAGIC curl localhost:8083/connectors/src-mysql-sales-order-header/tasks | jq  
# MAGIC 
# MAGIC > connector status   
# MAGIC curl localhost:8083/connectors/src-mysql-sales-order-header/status | jq  
# MAGIC 
# MAGIC > delete connector  
# MAGIC curl -X DELETE localhost:8083/connectors/src-mysql-sales-order-header
# MAGIC 
# MAGIC > list topics  
# MAGIC kafka-topics --list --zookeeper localhost:2181  
# MAGIC 
# MAGIC > read topic  
# MAGIC kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 --property print.key=true --from-beginning --topic src-mysql--sales-order-header  

# COMMAND ----------

>>>>>>> master
# DBTITLE 1,Lendo Dados do Apache Kafka para o Delta Lake [scala]
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.avro.functions.from_avro
# MAGIC import org.apache.avro.SchemaBuilder
# MAGIC 
# MAGIC //kafka
<<<<<<< HEAD
# MAGIC val kafkaBrokers = "13.77.73.67:9092"
# MAGIC 
# MAGIC //schema registry
# MAGIC val schemaRegistryAddr = "http://13.77.73.67:8081"
# MAGIC 
# MAGIC //topic
# MAGIC val mysql_topic = "mysql-ingest-sales_order_header"
# MAGIC 
# MAGIC //avro
# MAGIC val mysql_avro = "mysql-ingest-sales_order_header-value"
=======
# MAGIC val kafkaBrokers = "52.177.14.43:9092"
# MAGIC 
# MAGIC //schema registry
# MAGIC val schemaRegistryAddr = "http://52.177.14.43:8081"
# MAGIC 
# MAGIC //topic
# MAGIC val mysql_topic = "src-mysql--sales-order-header"
# MAGIC 
# MAGIC //avro
# MAGIC val mysql_avro = "src-mysql--sales-order-header-value"
>>>>>>> master
# MAGIC 
# MAGIC //mysql ~ kafka
# MAGIC val df_kafka_mysql = spark
# MAGIC   .readStream //lendo dados em streaming
# MAGIC   .format("kafka")
# MAGIC   .option("kafka.bootstrap.servers", kafkaBrokers)
# MAGIC   .option("subscribe", mysql_topic)
# MAGIC   .option("startingOffsets", "earliest")
# MAGIC   .load()
# MAGIC   .select(from_avro($"value", mysql_avro, schemaRegistryAddr).as("value"))
# MAGIC   .writeStream //escrevendo dados em streaming
# MAGIC   .format("delta") 
# MAGIC   .outputMode("append")
<<<<<<< HEAD
# MAGIC   .option("checkpointLocation", "/delta/mysql/checkpoints/mysql-ingest-sales-order-header-str7") 
# MAGIC   .start("/delta/bronze/ingest-bronze-sales-order-header_1")
=======
# MAGIC   .option("checkpointLocation", "/delta/mysql/checkpoints/mysql-ingest-sales-order-header-owshq") 
# MAGIC   .start("/delta/bronze/ingest-bronze-sales-order-header-owshq")
>>>>>>> master

# COMMAND ----------

# MAGIC %sql
# MAGIC 
<<<<<<< HEAD
# MAGIC SELECT COUNT(*) AS amt
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header_1`
=======
# MAGIC --15.501
# MAGIC --15.502
# MAGIC --15.503
# MAGIC --15.504
# MAGIC --15.505
# MAGIC 
# MAGIC SELECT COUNT(*) AS amt
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header-owshq`
>>>>>>> master

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Stage 2 - Silver Table
# MAGIC > quando uma modificação é realizada movemos para a **silver table**
# MAGIC > ela é a tabela responsável por receber informações que foram transformadas no **ETL** = filtros, limpeza e melhorias
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="300" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > implementação do merge para adicionar somente a diferença  
# MAGIC > https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html

# COMMAND ----------

# DBTITLE 1,Transformações = [/delta/bronze_sales_order_header/]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver_sales_order_header;
# MAGIC 
# MAGIC CREATE TABLE silver_sales_order_header
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT SalesOrderID,
# MAGIC        CustomerID,
# MAGIC        SalesPersonID,
# MAGIC        TerritoryID,
# MAGIC        OrderDate,
# MAGIC        Status,
# MAGIC        SalesOrderNumber,
# MAGIC        PurchaseOrderNumber,
# MAGIC        AccountNumber,
# MAGIC        CreditCardApprovalCode,
# MAGIC        TotalDue,
# MAGIC        SubTotal,
# MAGIC        TaxAmt,
# MAGIC        Freight,
# MAGIC        ModifiedDate
# MAGIC FROM delta.`/delta/bronze_sales_order_header/`
# MAGIC WHERE PurchaseOrderNumber <> 'null'
# MAGIC   AND Status = 5

# COMMAND ----------

# DBTITLE 1,Transformações = [/delta/bronze_sales_order_detail/]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver_sales_order_detail;
# MAGIC 
# MAGIC CREATE TABLE silver_sales_order_detail
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT SalesOrderID,
# MAGIC        SalesOrderDetailID,
# MAGIC        ProductID,
# MAGIC        CarrierTrackingNumber,
# MAGIC        UnitPrice,
# MAGIC        LineTotal,
# MAGIC        OrderQty,
# MAGIC        ModifiedDate
# MAGIC FROM delta.`/delta/bronze_sales_order_detail/`

# COMMAND ----------

# DBTITLE 1,Transformações = [/delta/bronze_product/]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver_product;
# MAGIC 
# MAGIC CREATE TABLE silver_product
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC        ProductID,
# MAGIC        Name,
# MAGIC        ProductNumber,
# MAGIC        Color,
# MAGIC        StandardCost,
# MAGIC        ListPrice,
# MAGIC        ModifiedDate
# MAGIC FROM delta.`/delta/bronze_product/`
# MAGIC WHERE StandardCost <> 0
# MAGIC   AND Color <> 'null'

# COMMAND ----------

# DBTITLE 1,Transformações = [/delta/bronze/ingest-bronze-sales-order-header]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT value.SalesOrderID,
# MAGIC        value.CustomerID,
# MAGIC        value.SalesPersonID,
# MAGIC        value.TerritoryID,
# MAGIC        value.OrderDate,
# MAGIC        value.Status,
# MAGIC        value.SalesOrderNumber,
# MAGIC        value.PurchaseOrderNumber,
# MAGIC        value.AccountNumber,
# MAGIC        value.CreditCardApprovalCode,
# MAGIC        value.TotalDue,
# MAGIC        value.SubTotal,
# MAGIC        value.TaxAmt,
# MAGIC        value.Freight,
# MAGIC        value.ModifiedDate
<<<<<<< HEAD
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header_1`
=======
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header-owshq`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header-owshq`
>>>>>>> master

# COMMAND ----------

# DBTITLE 1,Delta Lake = Unified Batch & Streaming Process
# MAGIC %python
# MAGIC 
# MAGIC df_insert_silver_soh = spark.sql("""
# MAGIC SELECT value.SalesOrderID,
# MAGIC        value.CustomerID,
# MAGIC        value.SalesPersonID,
# MAGIC        value.TerritoryID,
# MAGIC        value.OrderDate,
# MAGIC        value.Status,
# MAGIC        value.SalesOrderNumber,
# MAGIC        value.PurchaseOrderNumber,
# MAGIC        value.AccountNumber,
# MAGIC        value.CreditCardApprovalCode,
# MAGIC        value.TotalDue,
# MAGIC        value.SubTotal,
# MAGIC        value.TaxAmt,
# MAGIC        value.Freight,
# MAGIC        value.ModifiedDate
<<<<<<< HEAD
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header_1`""")
=======
# MAGIC FROM delta.`/delta/bronze/ingest-bronze-sales-order-header-owshq`""")
>>>>>>> master
# MAGIC 
# MAGIC df_insert_silver_soh.write.format("delta").mode("append").save("/delta/silver_sales_order_header")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/silver_sales_order_header`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > silver tables  
# MAGIC 
# MAGIC * silver_sales_order_header (batch & stream)
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC * silver_sales_order_detail
# MAGIC * silver_product  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_sales_order_header

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM silver_sales_order_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM silver_product

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)
# MAGIC 
# MAGIC 
# MAGIC > geralmente segregamos o Delta Lake em 3 partes  
# MAGIC 
# MAGIC > 1 - Bronze = **Staging**  
# MAGIC > 2 - Silver = **Transformações**  
# MAGIC > 3 - Gold = **Dw/DataSet** 
# MAGIC 
# MAGIC 
# MAGIC <img width="800" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta-lake-store.png'>
# MAGIC 
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Criação da Dimensão de Produto = [dim_product]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS dim_product;
# MAGIC 
# MAGIC CREATE TABLE dim_product
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY ProductID ASC) AS sk_productid, 
# MAGIC        ProductID AS bk_productid,
# MAGIC        Name,
# MAGIC        Color,
# MAGIC        ModifiedDate
# MAGIC FROM silver_product AS sp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS fact_sales;
# MAGIC 
# MAGIC CREATE TABLE fact_sales
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY ssoh.SalesOrderID ASC) AS sk_sales,
# MAGIC        sp.sk_productid,
# MAGIC        ssoh.OrderDate,
# MAGIC        ssoh.Status,
# MAGIC        ssoh.SalesOrderNumber,
# MAGIC        ssoh.PurchaseOrderNumber,
# MAGIC        ssoh.TotalDue,
# MAGIC        ssod.CarrierTrackingNumber,
# MAGIC        ssod.UnitPrice,
# MAGIC        ssod.LineTotal
# MAGIC FROM silver_sales_order_header AS ssoh
# MAGIC INNER JOIN silver_sales_order_detail AS ssod
# MAGIC ON ssoh.SalesOrderID = ssod.SalesOrderID
# MAGIC INNER JOIN dim_product AS sp
# MAGIC ON sp.bk_productid = ssod.ProductID

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM dim_product

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
<<<<<<< HEAD
# MAGIC FROM fact_sales

# COMMAND ----------

=======
# MAGIC FROM fact_sales
>>>>>>> master
