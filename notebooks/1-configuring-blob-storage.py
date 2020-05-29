# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure Blob Storage e Azure Databricks
# MAGIC > configuring Azure Blob Storage with Azure Databricks. 
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/blob_storage_azure_databricks.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > manual to connect to Azure Blob Storage   
# MAGIC > https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > this tutotial will guide you through the connectivity process between Azure Blob Storage and Azure Databricks. Since Databricks (Apache Spark) is a processing engine we often connect in a Data Lake to read files from it. In order to securely connect into Blob Storage, we have to configure a secure vault to hide sensitive information and not expose on the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### databricks command-line interface [CLI]
# MAGIC 
# MAGIC > https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html  
# MAGIC > https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html
# MAGIC 
# MAGIC 1. installing databricks-cli
# MAGIC > pip install databricks-cli  
# MAGIC 
# MAGIC 2. create a secret and scope
# MAGIC > databricks secrets create-scope --scope az-bs-brzluanmoreno   
# MAGIC > databricks secrets list-scopes  
# MAGIC > databricks secrets put --scope az-bs-brzluanmoreno --key key-brzluanmoreno
# MAGIC <br>
# MAGIC 
# MAGIC > this will add a layer of security where credentials will be kept save in a key vault (key-value) pair storage
# MAGIC <br>
# MAGIC 
# MAGIC > if you want, set up using Azure Cloud Shell.  
# MAGIC > [Databricks CLI & Azure Cloud Shell](https://docs.microsoft.com/pt-pt/azure/azure-databricks/databricks-cli-from-azure-cloud-shell)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html  
# MAGIC 
# MAGIC > the first command we gonna use is the **dbutil**. This is a Databricks command responsible to query the underling storages. Note that to mount a storage we need to pass the value of the Azure Blob Storage, in this case this information is stored on the Azure Key Vault.

# COMMAND ----------

# DBTITLE 1,Mount Storage [bs-stg-files] = Landing Zone
# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://bs-stg-files@brzluanmoreno.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/bs-stg-files",
# MAGIC   extra_configs = {"fs.azure.account.key.brzluanmoreno.blob.core.windows.net":dbutils.secrets.get(scope = "az-blob-storage-luanmoreno", key = "key-az-blob-storage")})

# COMMAND ----------

# DBTITLE 1,List Landing Zone
display(dbutils.fs.ls("/mnt/bs-stg-files/"))

# COMMAND ----------

# DBTITLE 1,Mount Storage [bs-production] = Production Zone
# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://bs-production@brzluanmoreno.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/bs-production",
# MAGIC   extra_configs = {"fs.azure.account.key.brzluanmoreno.blob.core.windows.net":dbutils.secrets.get(scope = "az-blob-storage-luanmoreno", key = "key-az-blob-storage")})

# COMMAND ----------

# DBTITLE 1,List Production Zone
display(dbutils.fs.ls("/mnt/bs-production/"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > remove the mounted storages if you want

# COMMAND ----------

# DBTITLE 1,Remove Mounts
dbutils.fs.unmount("/mnt/bs-stg-files")
dbutils.fs.unmount("/mnt/bs-production")