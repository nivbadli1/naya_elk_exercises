"""
Step 2 - NYC Taxi Consumer -> Elasticsearch
-------------------------------------------
Reads the trip records from the Kafka topic "my_trip" and indexes each one
as a document in the Elasticsearch index "nyctaxi".

Flow:  Kafka topic "my_trip"  -->  Elasticsearch index "nyctaxi"
Next step: the scripts in 03_queries/ search and aggregate the "nyctaxi" index.

Install the client libraries (the ES client version should match the server):
    pip install kafka-python
    pip install elasticsearch==7.13.2
"""

# KafkaConsumer is the client class used to read messages from Kafka
from kafka import KafkaConsumer
# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch  # type: ignore
# json - used to convert the Kafka message bytes back into a Python dict
import json

# ---------------- Kafka configuration ----------------
# The topic the producer writes to
kafka_topic = 'my_trip'
# Address of the Kafka broker in the course environment
kafka_bootstrap_servers = ['course-kafka:9092']

# ---------------- Elasticsearch configuration ----------------
# Connect to Elasticsearch using its container hostname and REST port
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])
# Name of the index the documents will be stored in (created automatically on first write)
es_index = 'nyctaxi'

# Create a Kafka consumer
consumer = KafkaConsumer(
    # Subscribe to the taxi topic
    kafka_topic,
    # Broker(s) to connect to
    bootstrap_servers=kafka_bootstrap_servers,
    # If this group has no saved offset, start from the oldest message in the topic
    auto_offset_reset='earliest',
    # Consumer group name - Kafka remembers the offset per group
    group_id="taxi_consumer",
    # Automatically commit (save) the offset of messages we already read
    enable_auto_commit=True,
    # Turn each message from bytes -> UTF-8 string -> Python dict
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages from Kafka and write them to Elasticsearch (runs until stopped)
for message in consumer:
    # The message value is already a dict thanks to the deserializer
    document = message.value
    # Index (insert) the dict as a new JSON document; Elasticsearch generates the _id
    es.index(index=es_index, body=document)

# Printed only if the loop ends (e.g. the consumer is closed)
print("Finished consuming messages and indexing to Elasticsearch.")
