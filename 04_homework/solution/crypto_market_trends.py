"""
Homework solution - Query 3: Market trends
------------------------------------------
1. Market cap distribution - a histogram of market cap in 10-billion-dollar buckets.
2. Overall market trend - the average 24h price change across all coins.
3. Trend per coin - the average 24h price change of every coin.
"""

# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Index name
index_name = 'cryptoprices'

# Size of one histogram bucket: 10 billion dollars
BUCKET_SIZE = 10_000_000_000

# Query 1: market cap histogram
query_market_cap_hist = {
    # Return only the aggregation
    "size": 0,
    "aggs": {
        # Our name for the aggregation
        "market_cap_ranges": {
            # histogram = buckets of a fixed numeric width
            "histogram": {
                # market_cap is mapped as long, so this works directly
                "field": "market_cap",
                # 10 billion dollar intervals
                "interval": BUCKET_SIZE,
                # Skip empty buckets (otherwise we get one line per 10B up to Bitcoin's market cap)
                "min_doc_count": 1
            }
        }
    }
}

# Query 2 + 3: average price change - per coin, and over all coins
query_market_trend = {
    "size": 0,
    "aggs": {
        # Per coin: one bucket per name, with the average change inside
        "top_cryptos": {
            "terms": {
                "field": "name.keyword",
                "size": 10
            },
            "aggs": {
                "avg_price_change": {
                    "avg": {
                        "field": "price_change_percentage_24h"
                    }
                }
            }
        },
        # Overall: a single average over all documents (a metric aggregation without buckets)
        "overall_market_trend": {
            "avg": {
                "field": "price_change_percentage_24h"
            }
        }
    }
}

# Run both queries
market_cap_response = es.search(index=index_name, body=query_market_cap_hist)
market_trend_response = es.search(index=index_name, body=query_market_trend)

# Display results - market cap histogram
print("Market Cap Distribution:")
print("-" * 50)
for bucket in market_cap_response['aggregations']['market_cap_ranges']['buckets']:
    # 'key' is the lower bound of the bucket - convert to billions of dollars
    min_value = bucket['key'] / 1_000_000_000
    max_value = (bucket['key'] + BUCKET_SIZE) / 1_000_000_000
    # Number of documents in this range
    count = bucket['doc_count']
    print(f"${min_value:.1f}B - ${max_value:.1f}B: {count} documents")

# Display results - overall market trend (None if the index is empty)
overall_trend = market_trend_response['aggregations']['overall_market_trend']['value']
print("\nOverall Market Trend:")
print("-" * 50)
if overall_trend is not None:
    print(f"Average 24-hour price change: {overall_trend:.2f}%")
else:
    print("No data yet - is the consumer running?")

# Display results - trend per coin
print("\nPrice Change Trends by Cryptocurrency:")
print("-" * 50)
for bucket in market_trend_response['aggregations']['top_cryptos']['buckets']:
    # Coin name
    name = bucket['key']
    # Average change for this coin
    avg_change = bucket['avg_price_change']['value']
    # Skip coins that have no value
    if avg_change is None:
        continue
    # Up or down arrow
    trend_icon = "📈" if avg_change > 0 else "📉"
    print(f"{name}: {avg_change:.2f}% {trend_icon}")

# Print the trend query for Kibana Dev Tools
print("\n")
print("Note: Equivalent Kibana/Dev Tools query for market trends:")
print("""
GET /cryptoprices/_search
{
  "size": 0,
  "aggs": {
    "top_cryptos": {
      "terms": {
        "field": "name.keyword",
        "size": 10
      },
      "aggs": {
        "avg_price_change": {
          "avg": {
            "field": "price_change_percentage_24h"
          }
        }
      }
    },
    "overall_market_trend": {
      "avg": {
        "field": "price_change_percentage_24h"
      }
    }
  }
}
""")
