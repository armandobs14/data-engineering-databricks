> ssh luanmoreno@kafka-confluent-streaming.eastus2.cloudapp.azure.com

### Initializing Apache Kafka [Confluent]
```sh
confluent local start
confluent local status
```

> user = kafka_reader
> pwd = demo@pass123

### Connecting on SQL Server
```sh
mssql-cli -S sql-server-dev-ip.eastus2.cloudapp.azure.com -d Yelp -U kafka_reader
```

### Query Table
```sql
SELECT TOP 10 * FROM restaurant_week_2019
SELECT COUNT(*) FROM restaurant_week_2019
```
### Configuring Kafka Connect for SQL Server
```sh
/home/luanmoreno/confluent-5.3.0/etc/kafka-connect-jdbc

source-mssql-yelp.properties

name=source-mssql-yelp
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://localhost:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081
connection.url=jdbc:sqlserver://sql-server-dev-ip.eastus2.cloudapp.azure.com;database=Yelp;username=kafka_reader;password=demo@pass123;
connection.attempts=2
query=SELECT * FROM dbo.restaurant_week_2019
mode=bulk
topic.prefix=mssql-restaurant-week
poll.interval.ms=86400000
tasks.max=2
validate.non.null=false
```

### Start Kafka Connect
```sh
confluent local load source-mssql-yelp -- -d /home/luanmoreno/confluent-5.3.0/etc/kafka-connect-jdbc/source-mssql-yelp.properties
confluent local status source-mssql-yelp
```

### Read Topic
```sh
kafka-topics --list --zookeeper localhost:2181

kafka-avro-console-consumer \
--bootstrap-server localhost:9092 \
--property schema.registry.url=http://localhost:8081 \
--property print.key=true \
--from-beginning \
--topic mssql-restaurant-week

kafka-topics --describe --zookeeper localhost:2181 --topic mssql-restaurant-week
```

### Get Schema Registry Information
```sh
curl --silent -X GET http://localhost:8083/connectors/ | jq .
curl --silent -X GET http://localhost:8081/subjects/ | jq .
curl --silent -X GET http://localhost:8081/subjects/mssql-restaurant-week-value/versions/latest | jq .
```
