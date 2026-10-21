"""
Homework solution - Step 1: Crypto Producer
-------------------------------------------
Every 60 seconds, fetches the top 15 cryptocurrencies by market cap from the CoinGecko API
and sends each coin as one message to the Kafka topic "crypto_prices".

Flow:  CoinGecko API  -->  Kafka topic "crypto_prices"
Next step: crypto_consumer.py reads this topic and writes to Elasticsearch.
"""

# KafkaProducer is the client class used to send messages to Kafka
from kafka import KafkaProducer
# time - used to wait between API calls
import time
# json - used to serialize the messages
import json
# requests - used to call the HTTP API
import requests
# datetime / timezone - used to add a UTC timestamp to every message
from datetime import datetime, timezone
# urllib3 - the HTTP library used by requests; used here only to silence SSL warnings
import urllib3

# The class network inspects HTTPS traffic, so certificate verification fails.
# We call the API with verify=False below (fine for a public API in a lab, never in production),
# and this line hides the "InsecureRequestWarning" that verify=False prints on every request.
urllib3.disable_warnings()

# ---------------- Configuration ----------------
# The Kafka topic we write to
KAFKA_TOPIC = 'crypto_prices'
# Address of the Kafka broker in the course environment
KAFKA_BOOTSTRAP_SERVERS = ['course-kafka:9092']
# CoinGecko endpoint that returns market data for many coins at once
API_ENDPOINT = 'https://api.coingecko.com/api/v3/coins/markets'
# Query parameters for the API call
API_PARAMS = {
    # Prices in US dollars
    'vs_currency': 'usd',
    # Sort by market cap, biggest first
    'order': 'market_cap_desc',
    # Return 15 coins
    'per_page': 15,
    # First page of results
    'page': 1,
    # Don't return the 7-day price history (sparkline) - we don't need it
    'sparkline': False
}
# Seconds to wait between API calls
INTERVAL_SECONDS = 60

# Create the Kafka producer once; it serializes every value: dict -> JSON string -> UTF-8 bytes
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Log that the producer is ready
print(f"Starting Crypto Price Producer - sending to topic: {KAFKA_TOPIC}")

# Run forever, until stopped with Ctrl+C
while True:
    try:
        # Call the API (skipping SSL verification because of the class network)
        response = requests.get(API_ENDPOINT, params=API_PARAMS, verify=False)

        # 200 = success
        if response.status_code == 200:
            # Parse the JSON response into a list of dicts (one dict per coin)
            crypto_data = response.json()
            # One timestamp for the whole batch, in UTC ISO format, e.g. "2026-10-21T12:30:00+00:00"
            batch_timestamp = datetime.now(timezone.utc).isoformat()

            # Send each coin as a separate message
            for crypto in crypto_data:
                # Keep only the fields that are in our Elasticsearch mapping
                message = {
                    # Coin id, e.g. "bitcoin"
                    'id': crypto['id'],
                    # Trading symbol, e.g. "btc"
                    'symbol': crypto['symbol'],
                    # Display name, e.g. "Bitcoin"
                    'name': crypto['name'],
                    # Current price in USD
                    'current_price': crypto['current_price'],
                    # Market capitalization in USD
                    'market_cap': crypto['market_cap'],
                    # Rank by market cap (1 = biggest)
                    'market_cap_rank': crypto['market_cap_rank'],
                    # Price change in USD over the last 24 hours
                    'price_change_24h': crypto['price_change_24h'],
                    # Price change in percent over the last 24 hours
                    'price_change_percentage_24h': crypto['price_change_percentage_24h'],
                    # When we recorded this data point
                    'timestamp': batch_timestamp
                }

                # Send the message to Kafka (the serializer turns it into bytes)
                producer.send(KAFKA_TOPIC, value=message)
                # Log what was sent
                print(f"Sent: {message['name']} - ${message['current_price']}")

            # Make sure all buffered messages were actually delivered
            producer.flush()
            # Log the end of the batch
            print(f"Batch completed at {datetime.now()}")
        else:
            # Any other status code, e.g. 429 = too many requests (rate limit)
            print(f"API Error: Status code {response.status_code}")

    except Exception as e:
        # Network or parsing error - log it and try again in the next round
        print(f"Error: {e}")

    # Wait before the next API call
    time.sleep(INTERVAL_SECONDS)
