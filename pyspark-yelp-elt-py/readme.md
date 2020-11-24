# Apache Spark [PySpark]
> building an elt pipeline [python & sql]

### submit [local]
```bash
# set utf-8
export PYTHONIOENCODING=utf8

# submit app locally
spark-submit \
--master local \
/Users/luanmorenomaciel/BitBucket/apache-spark/pyspark-yelp-elt-py/local.py

# local spark ui
http://luans-mbp.box:4040/jobs/
```

### submit [cluster] on hdinsight
````bash
# create hdinsight cluster [spark] ~ OwsHQ-ssh.azurehdinsight.net
- subscription = visual studio enterprise with msdn
- cluster type = spark

# copy files to storage location [owshq-spark-hdinsight-cluster]

# connect into driver node
ssh sshuser@OwsHQ-ssh.azurehdinsight.net

# list files inside of wasb
hdfs dfs -ls wasb://owshq-spark-hdinsight-cluster@owshqhdistorage.blob.core.windows.net/
hdfs dfs -ls wasb://owshq-spark-hdinsight-cluster@owshqhdistorage.blob.core.windows.net/data

# move pyfile to cluster location

# set utf-8
export PYTHONIOENCODING=utf8

$SPARK_HOME/bin/spark-submit \
--master yarn \
wasb://owshq-spark-hdinsight-cluster@owshqhdistorage.blob.core.windows.net/data/cluster.py
````

### spark history server
```bash
# verify job [id]

# history server address
https://owshq.azurehdinsight.net/sparkhistory/
```
