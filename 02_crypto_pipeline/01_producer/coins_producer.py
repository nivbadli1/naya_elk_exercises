"""
Step 1 - Crypto Producer
------------------------
Fetches live market data for Bitcoin, Ethereum, Solana and Tether from the
CoinGecko API and sends each coin to its own Kafka topic: crypto_<coin_id>
(crypto_bitcoin, crypto_ethereum, crypto_solana, crypto_tether).
It repeats every 60 seconds until you stop it with Ctrl+C.

Flow:  CoinGecko API  -->  Kafka topics crypto_*
Next step: the Spark consumers in 02_consumers/ read these topics.
Start the consumers BEFORE this producer - Spark reads only new messages by default.

Optional: a free CoinGecko demo API key raises the rate limit:
    export COINGECKO_API_KEY="<your key>"
"""

# KafkaProducer is the client class used to send messages to Kafka
from kafka import KafkaProducer
# json - used to encode the data
import json
# os - used to read the API key from an environment variable
import os
# time - used to wait between API calls
import time
# requests - used to make HTTP requests
import requests
# datetime - used to print a readable timestamp after each round
from datetime import datetime
# urllib3 - the HTTP library used by requests; used here only to silence SSL warnings
import urllib3

# The class network inspects HTTPS traffic, so certificate verification fails
# (SSLCertVerificationError). We call the API with verify=False below - acceptable for a
# public read-only API in a lab, never in production code.
# This line hides the "InsecureRequestWarning" that verify=False prints on every request.
urllib3.disable_warnings()

# CoinGecko API endpoint that returns market data for coins
url = "https://api.coingecko.com/api/v3/coins/markets"
# Query parameters: prices in USD, and the list of coins we want
params = {'vs_currency': 'usd', 'ids': 'bitcoin,ethereum,solana,tether'}
# Request headers: ask for JSON
headers = {"accept": "application/json"}
# Read the API key from the environment (never hard-code keys in code)
api_key = os.getenv("COINGECKO_API_KEY")
# Add the key to the headers only if it was set - the API also works without a key
if api_key:
    headers["x-cg-demo-api-key"] = api_key

# Seconds to wait between API calls (CoinGecko refreshes prices about once a minute)
INTERVAL_SECONDS = 60

# Create a KafkaProducer instance
producer = KafkaProducer(
    # Kafka broker address in the course environment
    bootstrap_servers='course-kafka:9092',
    # Serialize every value: dict -> JSON string -> UTF-8 bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_and_publish():
    # Send a GET request to the CoinGecko API to fetch cryptocurrency data (skip SSL verification)
    response = requests.get(url, headers=headers, params=params, verify=False)

    # Raise an error for HTTP errors such as 429 (rate limit) instead of sending bad data
    response.raise_for_status()

    # Parse the JSON response into a list of dicts (one dict per coin)
    data = response.json()

    # Iterate over each cryptocurrency item in the response
    for item in data:
        # Build the Kafka topic name dynamically from the coin id, e.g. "crypto_bitcoin"
        topic = f"crypto_{item['id']}"

        # Log the data being sent to the Kafka topic
        print(f"Producing message to {topic}: price={item['current_price']}")

        # Send the message to the matching Kafka topic (the serializer turns it into bytes)
        producer.send(topic, value=item)

    # Make sure all buffered messages are actually sent
    producer.flush()

    # Log successful publication with the current time
    print(f"[{datetime.now()}] Published data successfully")


# Main entry point - runs only when the file is executed directly
if __name__ == '__main__':
    # Run forever: fetch + publish, then wait, so Kibana gets a new data point every minute
    while True:
        try:
            # Fetch the data and publish it to Kafka
            fetch_and_publish()
        except requests.exceptions.RequestException as e:
            # API error (e.g. no internet or rate limit) - log it and try again next round
            print(f"[{datetime.now()}] API error: {e}")
        # Wait before the next round
        time.sleep(INTERVAL_SECONDS)
