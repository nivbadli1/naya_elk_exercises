# Homework - Crypto Prices Data Pipeline

> **Independent exercise, done after the lesson.** Build the full pipeline yourself:
> Kafka producer → Kafka consumer → Elasticsearch (with an explicit mapping) → 3 analysis queries → Kibana dashboard.
>
> Put your files inside this folder (`04_homework/`). For the dashboard part, follow
> [`kibana_dashboard_guide.md`](kibana_dashboard_guide.md).

## Overview
This exercise demonstrates how to build a data pipeline using Apache Kafka and Elasticsearch to process and analyze cryptocurrency price data in real-time. The pipeline fetches data from a public cryptocurrency API, processes it through Kafka, and indexes it into Elasticsearch for analysis and visualization with Kibana.

## Architecture
```
Cryptocurrency API → Kafka Producer → Kafka Topic → Kafka Consumer → Elasticsearch → Kibana
```

## Technical Requirements
- Python 3.7+
- Apache Kafka
- Elasticsearch 7.x
- Kibana
- Python Libraries:
  - `kafka-python`
  - `elasticsearch==7.13.2`. The client must match the server version. A newer client fails with `UnsupportedProductError`.
  - `requests`

```bash
pip install kafka-python "elasticsearch==7.13.2" requests
```

> **SSL note:** on the class network, calls to the CoinGecko API fail with
> `SSLError ... CERTIFICATE_VERIFY_FAILED`. Add `verify=False` to `requests.get(...)` and call
> `urllib3.disable_warnings()` once at the top of the script, as in the examples below.

## Exercise Components

### 1. Kafka Producer
Create a Python script that:
- Fetches cryptocurrency data from the CoinGecko API (https://api.coingecko.com/api/v3/coins/markets)
- Processes the data to extract relevant fields
- Sends each record to the Kafka topic named `crypto_prices`

API Endpoint Details:
```
URL: https://api.coingecko.com/api/v3/coins/markets
Parameters:
  - vs_currency: usd
  - order: market_cap_desc
  - per_page: 15
  - page: 1
  - sparkline: false
```

Complete API Request Example:
```python
import requests
import urllib3

# Skip the SSL warning caused by verify=False (class network)
urllib3.disable_warnings()

api_url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 15,
    'page': 1,
    'sparkline': False
}

response = requests.get(api_url, params=params, verify=False)
if response.status_code == 200:
    crypto_data = response.json()
    # Process data
    for crypto in crypto_data:
        print(f"{crypto['name']}: ${crypto['current_price']}")
else:
    print(f"Error: {response.status_code}")
```

Sample Response Structure:
```json
[
  {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "image": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png?1547033579",
    "current_price": 48350.23,
    "market_cap": 943950902238,
    "market_cap_rank": 1,
    "fully_diluted_valuation": 1012314453492,
    "total_volume": 20830364623,
    "high_24h": 48698.84,
    "low_24h": 47351.46,
    "price_change_24h": 778.82,
    "price_change_percentage_24h": 1.63815,
    "market_cap_change_24h": 14403300369,
    "market_cap_change_percentage_24h": 1.54963,
    "circulating_supply": 19538887,
    "total_supply": 21000000,
    "max_supply": 21000000,
    "ath": 69045,
    "ath_change_percentage": -30.01,
    "ath_date": "2021-11-10T14:24:11.849Z",
    "atl": 67.81,
    "atl_change_percentage": 71071.66,
    "atl_date": "2013-07-06T00:00:00.000Z",
    "last_updated": "2023-02-15T12:30:09.761Z"
  },
  ...
]
```

### 2. Kafka Consumer
Create a Python script that:
- Connects to the Kafka topic `crypto_prices`
- Consumes messages from the topic
- Creates an Elasticsearch index named `cryptoprices` with appropriate mapping
- Indexes each record into Elasticsearch

Elasticsearch Index Details:
- Index name: `cryptoprices`
- Required fields:
  - id (cryptocurrency id)
  - symbol
  - name
  - current_price
  - market_cap
  - market_cap_rank
  - price_change_24h
  - price_change_percentage_24h
  - timestamp

Detailed Elasticsearch Mapping:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `keyword` | Unique identifier for the cryptocurrency (e.g. `bitcoin`) |
| `symbol` | `keyword` | Trading symbol (e.g. `btc`) |
| `name` | `text` + `keyword` sub-field | Full name of the cryptocurrency (e.g. `Bitcoin`) |
| `current_price` | `float` | Current price in USD |
| `market_cap` | `long` | Market capitalization in USD |
| `market_cap_rank` | `integer` | Rank by market capitalization |
| `price_change_24h` | `float` | Absolute price change in USD over 24 hours |
| `price_change_percentage_24h` | `float` | Percentage price change over 24 hours |
| `timestamp` | `date` | When the data was recorded |

The same mapping as JSON, ready for Kibana Dev Tools (`PUT /cryptoprices`):
```json
{
  "mappings": {
    "properties": {
      "id": { "type": "keyword" },
      "symbol": { "type": "keyword" },
      "name": {
        "type": "text",
        "fields": {
          "keyword": { "type": "keyword", "ignore_above": 256 }
        }
      },
      "current_price": { "type": "float" },
      "market_cap": { "type": "long" },
      "market_cap_rank": { "type": "integer" },
      "price_change_24h": { "type": "float" },
      "price_change_percentage_24h": { "type": "float" },
      "timestamp": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis"
      }
    }
  }
}
```

Implementation Example for Index Creation:
```python
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Define index name
index_name = 'cryptoprices'

# Check if index exists
if not es.indices.exists(index=index_name):
    # Define mapping
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "symbol": {"type": "keyword"},
                "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "current_price": {"type": "float"},
                "market_cap": {"type": "long"},
                "market_cap_rank": {"type": "integer"},
                "price_change_24h": {"type": "float"},
                "price_change_percentage_24h": {"type": "float"},
                "timestamp": {"type": "date"}
            }
        }
    }
    
    # Create the index with mapping
    es.indices.create(index=index_name, body=mapping)
    print(f"Created Elasticsearch index: {index_name} with custom mapping")
```

### 3. Data Analysis with Elasticsearch
Create three Python scripts that perform the following queries:

1. **Average Price Analysis**: Calculate the average price for each cryptocurrency and sort by highest average price.

2. **Price Change Analysis**: Identify the top gainers and losers based on 24-hour price change percentage.

3. **Market Analysis**: Analyze market trends including:
   - Market cap distribution
   - Average price change across all cryptocurrencies
   - Individual cryptocurrency trends

### 4. Kibana Dashboard
Create a Kibana dashboard that visualizes:
- Price trends over time
- Market cap comparison
- 24-hour price changes
- Cryptocurrency performance ranking

## Instructions

### Step 1: Set up the Producer
1. Create a file named `crypto_producer.py`
2. Implement the code to fetch data from the CoinGecko API every 60 seconds
3. Send each record to the Kafka topic `crypto_prices`

### Step 2: Set up the Consumer
1. Create a file named `crypto_consumer.py`
2. Connect to the Kafka topic `crypto_prices`
3. Create the Elasticsearch index with appropriate mapping
4. Index the data into Elasticsearch

### Step 3: Implement Analysis Queries
Create three Python files:
1. `crypto_avg_prices.py` - For average price analysis
2. `crypto_price_changes.py` - For price change analysis
3. `crypto_market_trends.py` - For market trend analysis

### Step 4: Create Kibana Dashboard
Full step-by-step instructions: [`kibana_dashboard_guide.md`](kibana_dashboard_guide.md)

1. Access Kibana interface
2. Create index pattern for the `cryptoprices` index
3. Create visualizations:
   - Line chart for price trends
   - Bar chart for market cap comparison
   - Heat map for price changes
   - Data table for cryptocurrency ranking
4. Combine visualizations into a dashboard

## Questions for Discussion

After completing the exercise, think about these questions:

1. **Data Flow**: Describe the path that data takes from the API to the Kibana dashboard. What happens at each step?

2. **Data Structure**: How does the data structure change as it moves through the pipeline? Why is proper mapping important in Elasticsearch?

3. **Real-time vs. Batch**: What are the advantages of processing this data in real-time rather than in batches?

4. **Use Cases**: What business decisions could be made using the data visualized in your Kibana dashboard?

5. **Scaling**: How would you modify this pipeline if you needed to process data for 1,000 cryptocurrencies instead of 15?

## Sample Code

### Producer Example (Partial)
```python
from kafka import KafkaProducer
import requests
import urllib3
import json
import time

# Skip the SSL warning caused by verify=False (class network)
urllib3.disable_warnings()

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers=['course-kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# API endpoint
api_url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 15,
    'page': 1,
    'sparkline': False
}

# Fetch and send data
response = requests.get(api_url, params=params, verify=False)
crypto_data = response.json()

# Process each cryptocurrency
for crypto in crypto_data:
    # Send to Kafka
    producer.send('crypto_prices', value=crypto)
```

### Kibana Dashboard Creation Steps
1. Go to Kibana → Management → Stack Management → Index Patterns
2. Create a new index pattern for `cryptoprices`
3. Go to Kibana → Dashboard → Create new dashboard
4. Add visualizations:
   - Create a line visualization for price over time
   - Create a bar chart for market cap comparison
   - Create a data table showing cryptocurrency statistics
5. Arrange visualizations on the dashboard
6. Save the dashboard

## Resources
- [CoinGecko API Documentation](https://www.coingecko.com/api/documentation)
- [Kafka Python Documentation](https://kafka-python.readthedocs.io/)
- [Elasticsearch Python Client](https://elasticsearch-py.readthedocs.io/)
- [Kibana User Guide](https://www.elastic.co/guide/en/kibana/current/index.html)

---

## Solution (after submission)

Try to solve the exercise on your own first. After you submit, compare your work with the
reference solution in [`solution/`](solution/README.md). Its README has the full run order.
