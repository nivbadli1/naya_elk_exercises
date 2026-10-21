"""
Homework solution - Step 2: Crypto Consumer -> Elasticsearch
------------------------------------------------------------
Creates the Elasticsearch index "cryptoprices" with an explicit mapping (if it doesn't exist yet),
then reads the messages from the Kafka topic "crypto_prices" and indexes each one as a document.

Flow:  Kafka topic "crypto_prices"  -->  Elasticsearch index "cryptoprices"
Next step: the 3 query scripts, then the Kibana dashboard.

Requires the Elasticsearch client that matches the server:  pip install "elasticsearch==7.13.2"
"""

# KafkaConsumer is the client class used to read messages from Kafka
from kafka import KafkaConsumer
# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch
# json - used to convert the Kafka message bytes back into a Python dict
import json

# ---------------- Kafka configuration ----------------
# The topic the producer writes to
KAFKA_TOPIC = 'crypto_prices'
# Address of the Kafka broker in the course environment
KAFKA_BOOTSTRAP_SERVERS = ['course-kafka:9092']
# Consumer group name - Kafka remembers the offset per group
CONSUMER_GROUP = 'crypto_consumer_group'

# ---------------- Elasticsearch configuration ----------------
# Elasticsearch container hostname
ES_HOST = 'elasticsearch'
# Elasticsearch REST port
ES_PORT = 9200
# Plain HTTP (no TLS in the lab setup)
ES_SCHEME = 'http'
# The index we write to
ES_INDEX = 'cryptoprices'

# Connect to Elasticsearch
es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': ES_SCHEME}])

# Create the index with an explicit mapping, but only if it doesn't exist yet.
# Without this, dynamic mapping would guess the types from the first document.
if not es.indices.exists(index=ES_INDEX):
    # The mapping: the type of every field
    mapping = {
        "mappings": {
            "properties": {
                # Exact value - used for filters and aggregations
                "id": {"type": "keyword"},
                # Exact value
                "symbol": {"type": "keyword"},
                # Full-text field + a "name.keyword" sub-field for aggregations
                "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                # Numbers - allow avg / range / sort without scripts
                "current_price": {"type": "float"},
                "market_cap": {"type": "long"},
                "market_cap_rank": {"type": "integer"},
                "price_change_24h": {"type": "float"},
                "price_change_percentage_24h": {"type": "float"},
                # Date - lets Kibana use it as the time field
                "timestamp": {"type": "date"}
            }
        }
    }

    # Create the index with the mapping
    es.indices.create(index=ES_INDEX, body=mapping)
    # Log it
    print(f"Created Elasticsearch index: {ES_INDEX} with custom mapping")

# Create the Kafka consumer
consumer = KafkaConsumer(
    # Subscribe to the crypto topic
    KAFKA_TOPIC,
    # Broker(s) to connect to
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    # If this group has no saved offset, start from the oldest message in the topic
    auto_offset_reset='earliest',
    # Consumer group name
    group_id=CONSUMER_GROUP,
    # Automatically commit (save) the offset of messages we already read
    enable_auto_commit=True,
    # Turn each message from bytes -> UTF-8 string -> Python dict
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Log that the consumer is ready
print(f"Starting Crypto Price Consumer - reading from topic: {KAFKA_TOPIC}")
print(f"Indexing data to Elasticsearch: {ES_INDEX}")

# Counter for progress logs
message_count = 0
# Consume messages and index them (runs until stopped with Ctrl+C)
for message in consumer:
    try:
        # The message value is already a dict thanks to the deserializer
        document = message.value

        # Stable document id: coin + timestamp. If the same message is read twice
        # (e.g. after a restart), it overwrites the same document instead of duplicating it.
        doc_id = f"{document['id']}_{document['timestamp']}"
        # Index (insert or overwrite) the document
        es.index(index=ES_INDEX, id=doc_id, body=document)

        # Count it, and log every 10 documents
        message_count += 1
        if message_count % 10 == 0:
            print(f"Successfully indexed {message_count} documents to Elasticsearch")

    except Exception as e:
        # Log the error and continue with the next message instead of crashing
        print(f"Error processing message: {e}")
