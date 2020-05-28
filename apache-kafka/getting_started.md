# Apache Kafka [Getting Started]


### kafka-confluent-streaming [development]
```sh
# confluent vm
dev-kafka-confluent.eastus2.cloudapp.azure.com
```

### service status
```sh
# checking status
sudo systemctl status confluent-zookeeper
sudo systemctl status confluent-server
sudo systemctl status confluent-schema-registry
sudo systemctl status confluent-control-center
sudo systemctl status confluent-kafka-connect
sudo systemctl status confluent-kafka-rest
sudo systemctl status confluent-ksqldb
```

### create topics
```sh
# create topics with retention period
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic yelp_reviews --config retention.ms=5184000000

# describe topic
kafka-topics --bootstrap-server localhost:9092 --describe --topic yelp_reviews

# get offset
kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic yelp_reviews topicName:partitionID:offset
```
### topics
```sh
# list topics
kafka-topics --list --zookeeper localhost:2181

# cout rows
kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic yelp_reviews --time -1 --offsets 1 | awk -F ":" '{sum += $3} END {print sum}'

# read topic
kafka-avro-console-consumer \
--bootstrap-server localhost:9092 \
--property schema.registry.url=http://localhost:8081 \
--property print.key=true \
--from-beginning \
--topic yelp_reviews
```

```sh
# delete topic
kafka-topics --zookeeper localhost:2181 --delete --topic users
```
