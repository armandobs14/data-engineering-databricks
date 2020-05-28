"""
python3.7 main.py stream

the main function calls the confluent kafka class that handles with the kafka operations
the cli uses arg parse to receive how many events will be generated and send to kafka.
"""

# import libraries
from libs import sr_confluent_kafka
import argparse
import sys

# main
if __name__ == '__main__':

    # instantiate arg parse
    parser = argparse.ArgumentParser(description='streaming app')

    # add parameters to arg parse
    parser.add_argument('stream', type=str, choices=['confluent', 'kafka'], help='stream processing')
    parser.add_argument('broker', type=str, help='apache kafka broker')
    parser.add_argument('schema_registry', type=str, nargs="?", help='confluent schema registry')
    parser.add_argument('topic', type=str, help='topic name')
    parser.add_argument('gen_dt_rows', type=int, help='amount of events')

    # invoke help if null
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    # cli call

    # python3.7 main.py
    # python3.7 main.py 'confluent' 'dev-kafka-confluent.eastus2.cloudapp.azure.com' 'http://dev-kafka-confluent.eastus2.cloudapp.azure.com:8081' 'yelp_reviews' 1

    # decide - confluent or kafka [process]
    if sys.argv[1] == 'confluent':
        # send schema to confluent schema registry
        sr_confluent_kafka.Kafka().avro_producer(args.broker, args.schema_registry, args.topic, args.gen_dt_rows)








