# Apache Spark [PySpark]
> building an elt pipeline [python & sql]


### submit [local]
```bash
# set utf-8
export PYTHONIOENCODING=utf8

# submit app locally
spark-submit \
    --master local \
    /Users/luanmorenomaciel/BitBucket/databricks/project/yelp-elt-py/local.py
```

### submit [cluster] on hdinsight
````bash
# copy folder /data to wasb location

# connect into driver node
ssh sshuser@OwsHQ-ssh.azurehdinsight.net
LuanMoreno@153624

# list files inside of wasb
hdfs dfs -ls wasb://owshq-hdinsight-spark@owshqspark.blob.core.windows.net
hdfs dfs -ls wasb://owshq-hdinsight-spark@owshqspark.blob.core.windows.net/data

# set utf-8
export PYTHONIOENCODING=utf8

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    wasb://owshq-hdinsight-spark@owshqspark.blob.core.windows.net/data/cluster.py
````

### spark history server
```bash
# verify job [id]
application_1590424329692_0015

# history server address
https://owshq.azurehdinsight.net/sparkhistory/
```