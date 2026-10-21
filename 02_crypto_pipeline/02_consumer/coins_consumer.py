"""
Step 2 - Crypto Consumer -> Elasticsearch
-----------------------------------------
Reads the coin messages from the four Kafka topics (crypto_bitcoin, crypto_ethereum,
crypto_solana, crypto_tether), keeps only the fields we need, and indexes each one
as a document in the Elasticsearch index "crypto_data".

Flow:  Kafka topics crypto_*  -->  Elasticsearch index "crypto_data"  -->  Kibana dashboard
Same pattern as 01_nyc_taxi_to_elasticsearch/02_consumer/taxi_consumer.py.

Install the client libraries (the ES client version must match the server):
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
# The four topics the producer writes to (one topic per coin)
kafka_topics = ['crypto_bitcoin', 'crypto_ethereum', 'crypto_solana', 'crypto_tether']
# Address of the Kafka broker in the course environment
kafka_bootstrap_servers = ['course-kafka:9092']

# ---------------- Elasticsearch configuration ----------------
# Connect to Elasticsearch using its container hostname and REST port
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])
# Name of the index the documents will be stored in (created automatically on first write)
es_index = 'crypto_data'

# Create a Kafka consumer
consumer = KafkaConsumer(
    # Subscribe to all four crypto topics at once
    *kafka_topics,
    # Broker(s) to connect to
    bootstrap_servers=kafka_bootstrap_servers,
    # If this group has no saved offset, start from the oldest message in each topic
    auto_offset_reset='earliest',
    # Consumer group name - Kafka remembers the offset per group
    group_id="crypto_consumer",
    # Automatically commit (save) the offset of messages we already read
    enable_auto_commit=True,
    # Turn each message from bytes -> UTF-8 string -> Python dict
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Log that the consumer is ready
print(f"Consumer started. Listening on topics: {kafka_topics}")

# Consume messages from Kafka and write them to Elasticsearch (runs until stopped)
for message in consumer:
    # The message value is the full CoinGecko record for one coin, already a dict
    item = message.value

    # Keep only the fields we need, with clearer names
    document = {
        # Coin id, e.g. "bitcoin"
        "currency": item["id"],
        # When CoinGecko last updated the price, e.g. "2026-10-21T12:30:09.761Z"
        # (ISO date string -> Elasticsearch maps it as a "date", so Kibana can use it as the time field)
        "timestamp": item["last_updated"],
        # Current price in USD (a JSON number -> mapped as a number)
        "price": item["current_price"],
        # 24h trading volume in USD
        "volume": item["total_volume"],
        # Market capitalization in USD
        "market_cap": item["market_cap"]
    }

    # Build a stable document id from the coin and its update time. If the producer fetches the
    # same price twice, the second write overwrites the first instead of creating a duplicate.
    doc_id = f"{document['currency']}_{document['timestamp']}"

    # Index (insert or overwrite) the document in Elasticsearch
    es.index(index=es_index, id=doc_id, body=document)

    # Log what was indexed
    print(f"Indexed {document['currency']}: price={document['price']} at {document['timestamp']}")
