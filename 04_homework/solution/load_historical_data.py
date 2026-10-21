"""
Homework solution - Optional: load 30 days of history
-----------------------------------------------------
The live producer adds only one data point per minute, so the Kibana charts look empty at first.
This script fetches 30 days of daily prices for the same 15 coins and sends them to the same
Kafka topic ("crypto_prices"), so the dashboard has history from the start.

Run it ONCE, with the consumer running, before (or alongside) crypto_producer.py.
"""

# KafkaProducer is the client class used to send messages to Kafka
from kafka import KafkaProducer
# time - used to wait between API calls (rate limit)
import time
# json - used to serialize the messages
import json
# requests - used to call the HTTP API
import requests
# datetime / timezone - used to convert the API timestamps to ISO dates
from datetime import datetime, timezone
# urllib3 - used here only to silence SSL warnings
import urllib3

# The class network inspects HTTPS traffic - skip verification (verify=False below) and hide the warning
urllib3.disable_warnings()

# ---------------- Configuration ----------------
# The Kafka topic we write to (same as the live producer)
KAFKA_TOPIC = 'crypto_prices'
# Address of the Kafka broker
KAFKA_BOOTSTRAP_SERVERS = ['course-kafka:9092']
# CoinGecko API base URL
API_BASE = 'https://api.coingecko.com/api/v3'

# Create the Kafka producer: dict -> JSON string -> UTF-8 bytes
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Log that we started
print(f"Starting Crypto Historical Data Loader - sending to topic: {KAFKA_TOPIC}")

# The coins to load history for: CoinGecko id -> (symbol, display name).
# The symbol and name must match exactly what the live producer gets from the API,
# otherwise Elasticsearch aggregations would split one coin into two buckets.
coins_info = {
    'bitcoin': ('btc', 'Bitcoin'),
    'ethereum': ('eth', 'Ethereum'),
    'tether': ('usdt', 'Tether'),
    'ripple': ('xrp', 'XRP'),
    'binancecoin': ('bnb', 'BNB'),
    'usd-coin': ('usdc', 'USDC'),
    'solana': ('sol', 'Solana'),
    'dogecoin': ('doge', 'Dogecoin'),
    'tron': ('trx', 'TRON'),
    'cardano': ('ada', 'Cardano'),
    'lido-staked-ether': ('steth', 'Lido Staked Ether'),
    'wrapped-bitcoin': ('wbtc', 'Wrapped Bitcoin'),
    'leo-token': ('leo', 'LEO Token'),
    'usds': ('usds', 'USDS'),
    'the-open-network': ('ton', 'Toncoin')
}
# The list of ids to loop over (a coin that hits the rate limit is appended again to retry it)
coins = list(coins_info)

# How many days back to load (the free API allows up to 365)
days = 30


# Fetch the history of every coin and send it to Kafka
def load_historical_data():
    # Total number of messages sent
    total_records = 0

    # Loop over the coins (a coin that hit the rate limit is appended again to retry it)
    for coin_id in coins:
        print(f"Fetching historical data for {coin_id}...")

        # Endpoint that returns the price history of one coin
        url = f"{API_BASE}/coins/{coin_id}/market_chart"
        # Query parameters
        params = {
            # Prices in US dollars
            'vs_currency': 'usd',
            # How many days back
            'days': days,
            # One data point per day
            'interval': 'daily'
        }

        try:
            # Call the API (skipping SSL verification because of the class network)
            response = requests.get(url, params=params, verify=False)

            # 200 = success
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json()

                # The data comes as lists of pairs: [[timestamp_ms, value], ...]
                prices = data['prices']
                market_caps = data['market_caps']

                print(f"Got {len(prices)} historical data points for {coin_id}")
                # Messages sent for this coin
                coin_records = 0

                # One message per day
                for i in range(len(prices)):
                    # Timestamp in milliseconds
                    timestamp_ms = prices[i][0]
                    # Price on that day
                    price = prices[i][1]
                    # Market cap on that day (0 if missing)
                    market_cap = market_caps[i][1] if i < len(market_caps) else 0

                    # Daily price change compared to the previous day (0 for the first day)
                    price_change_24h = 0
                    price_change_percentage_24h = 0
                    if i > 0:
                        price_change_24h = price - prices[i - 1][1]
                        price_change_percentage_24h = (price_change_24h / prices[i - 1][1]) * 100

                    # Convert the timestamp to an ISO date string in UTC
                    date = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()

                    # Look up the symbol and display name of this coin
                    symbol, name = coins_info[coin_id]

                    # Build the message - same fields as the live producer
                    message = {
                        'id': coin_id,
                        'symbol': symbol,
                        'name': name,
                        'current_price': price,
                        'market_cap': market_cap,
                        # Not available in historical data
                        'market_cap_rank': 0,
                        'price_change_24h': price_change_24h,
                        'price_change_percentage_24h': price_change_percentage_24h,
                        'timestamp': date
                    }

                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, value=message)
                    coin_records += 1

                    # Log progress every 5 messages, and at the end
                    if coin_records % 5 == 0 or coin_records == len(prices):
                        print(f"Sent {coin_records}/{len(prices)} records for {name}")

                # Add to the total
                total_records += coin_records
                print(f"Historical data for {coin_id} completed. Total records: {coin_records}")

                # Wait between requests to avoid the rate limit
                time.sleep(2)

            # 429 = too many requests (rate limit)
            elif response.status_code == 429:
                print(f"Rate limit hit for {coin_id}. Waiting 60 seconds...")
                time.sleep(60)
                # Add the coin back to the end of the list to retry it
                coins.append(coin_id)

            else:
                # Any other error
                print(f"Error fetching data for {coin_id}. Status code: {response.status_code}")

        except Exception as e:
            # Network or parsing error - log it and move on to the next coin
            print(f"Error processing {coin_id}: {e}")

    # Make sure all buffered messages were delivered
    producer.flush()
    print(f"Historical data loading completed. Total records sent to Kafka: {total_records}")


# Run the loader
load_historical_data()
print("Historical data loading finished. You can now start the regular producer.")
