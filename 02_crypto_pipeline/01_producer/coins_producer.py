"""
Step 1 - Crypto Producer
------------------------
Fetches live market data for Bitcoin, Ethereum, Solana and Tether from the
CoinGecko API and sends each coin to its own Kafka topic: crypto_<coin_id>
(crypto_bitcoin, crypto_ethereum, crypto_solana, crypto_tether).

Flow:  CoinGecko API  -->  Kafka topics crypto_*
Next step: 02_airflow/dag_ssh.py runs this script; 03_consumers/ read the topics.

Before running, set your free CoinGecko demo API key:
    export COINGECKO_API_KEY="<your key>"
"""

# KafkaProducer is the client class used to send messages to Kafka
from kafka import KafkaProducer
# json - used to encode the data
import json
# os - used to read the API key from an environment variable
import os
# time - for timestamps (currently not used)
import time
# requests - used to make HTTP requests
import requests
# datetime - for timestamp-related operations (currently not used)
from datetime import datetime

# CoinGecko API endpoint that returns market data for coins
url = "https://api.coingecko.com/api/v3/coins/markets"
# Query parameters: prices in USD, and the list of coins we want
params = {'vs_currency': 'usd', 'ids': 'bitcoin,ethereum,solana,tether'}
# Request headers: ask for JSON and pass the API key (read from the environment, never hard-coded)
headers = {"accept": "application/json", "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY", "")}

# Create a KafkaProducer instance
producer = KafkaProducer(
    # Kafka broker address in the course environment
    bootstrap_servers='course-kafka:9092',
    # Serialize every value: dict -> JSON string -> UTF-8 bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_and_publish():
    # Send a GET request to the CoinGecko API to fetch cryptocurrency data
    response = requests.get(url, headers=headers, params=params)

    # Parse the JSON response into a list of dicts (one dict per coin)
    data = response.json()

    # Iterate over each cryptocurrency item in the response
    for item in data:
        # Build the Kafka topic name dynamically from the coin id, e.g. "crypto_bitcoin"
        topic = f"crypto_{item['id']}"

        # Log the data being sent to the Kafka topic
        print(f"Producing message to {topic}: value={json.dumps(item)}")

        # Send the message to the matching Kafka topic (the serializer turns it into bytes)
        producer.send(topic, value=item)

    # Make sure all buffered messages are actually sent before we exit
    producer.flush()

    # Log successful publication
    print("Published data successfully")


# Main entry point - runs only when the file is executed directly
if __name__ == '__main__':
    # Fetch the data once and publish it to Kafka
    fetch_and_publish()
