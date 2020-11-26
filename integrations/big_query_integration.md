https://docs.databricks.com/data/data-sources/google/bigquery.html
https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/

### create gcp resources
```
perms file
sa-google-bigquery.json

service account [sa]
databricks-bigquery@silver-charmer-243611.iam.gserviceaccount.com

google cloud storage [bucket]
owshq-dbrs-bigquery
```

### configure databricks-cli
```sh
# install cli
pip3 install databricks-cli

# start configuration process
# https://eastus2.azuredatabricks.net
# token = dapifc13727ecd821c8be57d89ca94084c2b

databricks configure --token

# list fs
databricks fs ls

# copy file to
dbfs cp sa-google-bigquery.json dbfs:/gcp/sa-google-bigquery.json
dbfs ls dbfs:/gcp
```

### set on databricks cluster
```sh
# set env configuration [edit]
GOOGLE_APPLICATION_CREDENTIALS=/dbfs/gcp/sa-google-bigquery.json


```
