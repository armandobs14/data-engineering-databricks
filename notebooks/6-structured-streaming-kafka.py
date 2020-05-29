# Databricks notebook source
# DBTITLE 1,Kafka Broker
# MAGIC %sh telnet 52.252.51.41 9092

# COMMAND ----------

# DBTITLE 1,Apache Zookeeper
# MAGIC %sh telnet 52.252.51.41 2181

# COMMAND ----------

# DBTITLE 1,Schema Registry
# MAGIC %sh telnet 52.252.51.41 8081

# COMMAND ----------

# DBTITLE 1,Confluent REST Proxy
# MAGIC %sh telnet 52.252.51.41 8082

# COMMAND ----------

# DBTITLE 1,Libraries
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.avro.functions.from_avro
# MAGIC import org.apache.avro.SchemaBuilder

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > https://spark.apache.org/docs/latest/sql-data-sources-avro.html

# COMMAND ----------

# DBTITLE 1,Var
# MAGIC %scala
# MAGIC 
# MAGIC //kafka
# MAGIC val kafkaBrokers = "52.252.51.41:9092"
# MAGIC 
# MAGIC //schema registry
# MAGIC val schemaRegistryAddr = "http://52.252.51.41:8081"
# MAGIC 
# MAGIC //topic
# MAGIC val mssql_topic = "yelp_reviews"
# MAGIC 
# MAGIC //avro
# MAGIC val mssql_avro = "yelp_reviews-value"

# COMMAND ----------

# DBTITLE 1,MSSQL ~ Apache Kafka ~ Structured Streaming - readStream
# MAGIC %scala
# MAGIC 
# MAGIC //kafka
# MAGIC val df_kafka_mssql = spark
# MAGIC   .readStream
# MAGIC   .format("kafka")
# MAGIC   .option("kafka.bootstrap.servers", kafkaBrokers)
# MAGIC   .option("subscribe", mssql_topic)
# MAGIC   .option("startingOffsets", "earliest")
# MAGIC   .load()
# MAGIC   .select(from_avro($"value", mssql_avro, schemaRegistryAddr).as("value"))
# MAGIC 
# MAGIC // is streaming ? 
# MAGIC df_kafka_mssql.isStreaming

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC df_kafka_mssql 
# MAGIC   .writeStream
# MAGIC   .format("delta") 
# MAGIC   .outputMode("append")
# MAGIC   .option("checkpointLocation", "dbfs:/delta/reviews/_checkpoints/stream_kafka_bronze_reviews") 
# MAGIC   .start("/delta/stream_kafka_bronze_reviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- 19.863
# MAGIC SELECT COUNT(*) AS amt
# MAGIC FROM delta.`/delta/stream_kafka_bronze_reviews`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT value.*
# MAGIC FROM delta.`/delta/stream_kafka_bronze_reviews`

# COMMAND ----------

