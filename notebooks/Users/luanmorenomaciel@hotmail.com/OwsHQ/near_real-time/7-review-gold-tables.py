# Databricks notebook source
# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > agora que possuímos todas as silver tables (transformações) iremos criar nosso Dw(StarSchema)?/DataSet para consumo dos dados.  
# MAGIC > iremos realizar isso utilizando spark-sql para popular as informações vindas as diversas tabelas

# COMMAND ----------

# DBTITLE 1,Utilizando o Banco de Dados Yelp
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,Mostrando Tabelas do Banco de Dados Yelp
# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Stage 1 - Bronze Table
# MAGIC > escrevendo a tabela de parquet para delta, fazendo isso iremos ganhar em armazenamento, velocidade e teremos diversas opções que estão listadas acima  
# MAGIC > as tabela são as réplicas do **azure blob storage**  
# MAGIC 
# MAGIC > *lembre-se que esse layer não possui modificações, o dado vem direto do Data Lake para o Delta Lake Bronze Table*
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze-table.png'>
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC > bronze_users  
# MAGIC > bronze_business  
# MAGIC > stream_bronze_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC #### Stage 2 - Silver Table
# MAGIC > quando uma modificação é realizada movemos para a **silver table**
# MAGIC > ela é a tabela responsável por receber informações que foram transformadas no **ETL** = filtros, limpeza e melhorias
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="300" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver-table.png'>
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC > silver_users  
# MAGIC > silver_business  
# MAGIC > stream_silver_reviews

# COMMAND ----------

# DBTITLE 1,Dimensão = silver_users
# MAGIC %sql
# MAGIC 
# MAGIC --1.518.169
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_users
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Dimensão = silver_business [batch]
# MAGIC %sql
# MAGIC 
# MAGIC --188.593
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_business
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Dimensão = silver_business [stream]
# MAGIC %sql
# MAGIC 
# MAGIC --348
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_restaurant_week

# COMMAND ----------

# DBTITLE 1,Fato = stream_silver_reviews
# MAGIC %sql
# MAGIC 
# MAGIC --22.601.688
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_silver_reviews`
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --22.601.688
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/stream_silver_reviews`
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM delta.`/delta/reviews`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC VACUUM delta.`/delta/reviews`

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
# MAGIC > é nesse momento que o *Data Scientist* assume a parte de ML + Pipeline
# MAGIC 
# MAGIC <img width="600" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta-lake-store.png'>
# MAGIC 
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Criando DataSet [Gold] no Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC --48.46
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gold_reviews;
# MAGIC 
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
# MAGIC FROM delta.`/delta/stream_silver_reviews` AS r
# MAGIC INNER JOIN silver_business AS b
# MAGIC ON r.business_id = b.business_id
# MAGIC INNER JOIN silver_users AS u
# MAGIC ON r.user_id = u.user_id

# COMMAND ----------

# DBTITLE 1,Quantidade de Registros [gold_reviews]
# MAGIC %sql
# MAGIC 
# MAGIC --22.601.688
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold_reviews

# COMMAND ----------

# DBTITLE 1,Lendo Registros [gold_reviews]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exploração dos Dados [gold_reviews]

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- + 22 mi
# MAGIC 
# MAGIC SELECT store_name, store_city, COUNT(*) AS Q
# MAGIC FROM gold_reviews
# MAGIC WHERE user_importance = "rockstar"
# MAGIC GROUP BY store_name, store_city
# MAGIC ORDER BY Q DESC
# MAGIC LIMIT 5

# COMMAND ----------

