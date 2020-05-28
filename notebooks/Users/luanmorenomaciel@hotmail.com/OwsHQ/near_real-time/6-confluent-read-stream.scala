// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC ### Apache Kafka (Confluent)
// MAGIC 
// MAGIC > kafka-confluent-streaming

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <br>
// MAGIC 
// MAGIC <img width="1000px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/mysql-case.png'>
// MAGIC   
// MAGIC <br>

// COMMAND ----------

// DBTITLE 1,Kafka Broker
// MAGIC %sh telnet 13.77.73.67 9092

// COMMAND ----------

// DBTITLE 1,Zookeeper
// MAGIC %sh telnet 13.77.73.67 2181

// COMMAND ----------

// DBTITLE 1,Schema Registry
// MAGIC %sh telnet 13.77.73.67 8081

// COMMAND ----------

// DBTITLE 1,Confluent REST Proxy
// MAGIC %sh telnet 13.77.73.67 8082

// COMMAND ----------

// DBTITLE 1,Libraries
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.avro.SchemaBuilder

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/sql-data-sources-avro.html

// COMMAND ----------

// DBTITLE 1,Var
//kafka
val kafkaBrokers = "13.77.73.67:9092"

//schema registry
val schemaRegistryAddr = "http://13.77.73.67:8081"

//topic
val mysql_topic = "mysql-ingest-sales_order_header"

//avro
val mysql_avro = "mysql-ingest-sales_order_header-value"

// COMMAND ----------

// DBTITLE 1,MySQL ~ Apache Kafka ~ Structured Streaming - readStream
//kafka
val df_kafka_mysql = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokers)
  .option("subscribe", mysql_topic)
  .option("startingOffsets", "earliest")
  .load()
  .select(from_avro($"value", mysql_avro, schemaRegistryAddr).as("value"))

// COMMAND ----------

// DBTITLE 1,Streaming de Dados [Validação e Verificação]
df_kafka_mysql.isStreaming

// COMMAND ----------

// DBTITLE 1,Lendo Streaming e Salvando e uma (Delta Table)
df_kafka_mysql 
  .writeStream
  .format("delta") 
  .outputMode("append")
  .option("checkpointLocation", "/delta/mysql/checkpoints/mysql-ingest-sales-order-header") 
  .start("/delta/bronze/ingest-sales-order-header")

// COMMAND ----------

// DBTITLE 1,[mysql] = Utilizando %sql para Acessar Tabela [Delta]
// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) AS amt
// MAGIC FROM delta.`/delta/bronze/ingest-sales-order-header`

// COMMAND ----------

// DBTITLE 1,Integração em Streaming com %sql
// MAGIC %sql
// MAGIC 
// MAGIC SELECT value.*
// MAGIC FROM delta.`/delta/bronze/ingest-sales-order-header`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC > limpeza dos dados

// COMMAND ----------

// MAGIC %fs rm -r "/delta/mysql/checkpoints/mysql-ingest-sales-order-header"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DELETE FROM delta.`/delta/bronze/ingest-sales-order-header`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC VACUUM delta.`/delta/bronze/ingest-sales-order-header`

// COMMAND ----------

