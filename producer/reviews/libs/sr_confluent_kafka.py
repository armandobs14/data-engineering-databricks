"""
filename: confluent_kafka.py
name: confluent_kafka

description:
working with kafka confluent. reading data from read files class and sending to
kafka using schema registry option. also using some producer settings
"""

# import libraries
from libs import read_files
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from configs import config
from random import seed

# incremental seed of 1
seed(1)


class Kafka(object):

    # use __slots__ to explicitly declare all schema members
    __slots__ = ["review_id", "business_id", "user_id", "stars", "useful", "date"]

    # define init function based on the expected input
    def __init__(self, review_id=None, business_id=None, user_id=None, stars=None, useful=None, date=None):

        self.review_id = review_id
        self.business_id = business_id
        self.user_id = user_id
        self.stars = stars
        self.useful = useful
        self.date = date

    # avro does not support code generation
    # need to provide dict representation
    def to_dict(self):

        return {
            "review_id": self.review_id,
            "business_id": self.business_id,
            "user_id": self.user_id,
            "stars": self.stars,
            "useful": self.useful,
            "date": self.date
        }

    # delivery reports for producer.poll
    # callback with extra argument
    def on_delivery(self, err, msg, obj):

        if err is not None:
            print('message delivery failed for user {} with error {}'.format(obj.name, err))
        else:
            print('message successfully produced to {} [{}] at offset {}'.format(msg.topic(), msg.partition(), msg.offset()))

    def avro_producer(self, broker, schema_registry, topic, gen_dt_rows):

        # avro schema [key] & [value]
        key_schema_str = config.key_schema_str
        value_schema_str = config.value_schema_str

        # load avro definition
        key_schema = avro.loads(key_schema_str)
        value_schema = avro.loads(value_schema_str)

        # get data to insert
        get_data = read_files.CSV().csv_reader(gen_dt_rows)

        # init producer using key & value schema
        producer = AvroProducer(
            {
                # client id
                "client.id": 'sr-py-yelp-stream-app',
                # kafka broker server
                "bootstrap.servers": broker,
                # schema registry url
                "schema.registry.url": schema_registry,
                # eos = exactly once semantics [options]
                "enable.idempotence": "true",
                "max.in.flight.requests.per.connection": 1,
                "retries": 100,
                "acks": "all",
                # max number of messages batched in one message set
                "batch.num.messages": 1000,
                # delay in ms to wait for messages in queue
                "queue.buffering.max.ms": 100,
                # max number of messages on queue
                "queue.buffering.max.messages": 1000,
                # wait messages in queue before send to brokers (batch)
                "linger.ms": 100
            },
            default_key_schema=key_schema,
            default_value_schema=value_schema)

        # loop to insert data
        inserts = 0
        while inserts < len(get_data):

            # instantiate new records, execute callbacks
            record = Kafka()

            try:

                # map columns and access using dict values
                record.review_id = get_data[inserts]['review_id']
                record.business_id = get_data[inserts]['business_id']
                record.user_id = get_data[inserts]['user_id']
                record.stars = get_data[inserts]['stars']
                record.useful = get_data[inserts]['useful']
                record.date = get_data[inserts]['date']

                # print(record.to_dict())

                # server on_delivery callbacks from previous asynchronous produce()
                producer.poll(0)

                # message passed to the delivery callback will already be serialized.
                # to aid in debugging we provide the original object to the delivery callback.
                producer.produce(
                    topic=topic,
                    key={'review_id': record.review_id},
                    value=record.to_dict(),
                    callback=lambda err, msg, obj=record: self.on_delivery(err, msg, obj)
                )

            except BufferError:
                print("buffer full")
                producer.poll(0.1)

            except ValueError:
                print("invalid input")
                raise

            except KeyboardInterrupt:
                raise

            # increment values
            inserts += 1

        print("flushing records...")

        # buffer messages to send
        producer.flush()
