"""
Homework solution - Query 2: Top gainers and losers (24h price change)
----------------------------------------------------------------------
Finds the 5 coins with the biggest rise and the 5 with the biggest drop in the last 24 hours.

Concepts: range query, sort, _source filtering, collapse.
The index holds many documents per coin (one per minute), so we use "collapse" on "id"
to get only ONE document per coin - otherwise the top 5 could be the same coin 5 times.
"""

# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Index name
index_name = 'cryptoprices'

# Query: gainers. We ask for 15 (all coins) because "size" is applied before we pick the top 5 in Python
query_top_gainers = {
    # Return up to 15 documents - one per coin after collapse
    "size": 15,
    # Only coins whose 24h change is positive
    "query": {
        "range": {
            "price_change_percentage_24h": {
                "gt": 0
            }
        }
    },
    # Sort by the newest data first, so collapse keeps the latest document of each coin
    "sort": [
        {"timestamp": {"order": "desc"}}
    ],
    # One document per coin (id is a keyword field)
    "collapse": {"field": "id"},
    # Return only these fields
    "_source": ["name", "symbol", "current_price", "price_change_percentage_24h", "timestamp"]
}

# Query: losers - same structure, with a negative change
query_top_losers = {
    # Return up to 15 documents - one per coin after collapse
    "size": 15,
    # Only coins whose 24h change is negative
    "query": {
        "range": {
            "price_change_percentage_24h": {
                "lt": 0
            }
        }
    },
    # Newest data first
    "sort": [
        {"timestamp": {"order": "desc"}}
    ],
    # One document per coin
    "collapse": {"field": "id"},
    # Return only these fields
    "_source": ["name", "symbol", "current_price", "price_change_percentage_24h", "timestamp"]
}

# Run both queries
gainers_response = es.search(index=index_name, body=query_top_gainers)
losers_response = es.search(index=index_name, body=query_top_losers)

# Sort the latest document of each coin by the % change: biggest rise first / biggest drop first
gainers = sorted((hit['_source'] for hit in gainers_response['hits']['hits']),
                 key=lambda s: s['price_change_percentage_24h'], reverse=True)[:5]
losers = sorted((hit['_source'] for hit in losers_response['hits']['hits']),
                key=lambda s: s['price_change_percentage_24h'])[:5]

# Display the top gainers
print("Top 5 Gainers in the Last 24 Hours:")
print("-" * 70)
for source in gainers:
    print(f"{source['name']} ({source['symbol'].upper()}): +{source['price_change_percentage_24h']:.2f}%, Current Price: ${source['current_price']}")

# Display the top losers
print("\nTop 5 Losers in the Last 24 Hours:")
print("-" * 70)
for source in losers:
    print(f"{source['name']} ({source['symbol'].upper()}): {source['price_change_percentage_24h']:.2f}%, Current Price: ${source['current_price']}")

# Print the gainers query for Kibana Dev Tools
print("\n")
print("Note: Equivalent Kibana/Dev Tools query for top gainers:")
print("""
GET /cryptoprices/_search
{
  "size": 15,
  "query": {
    "range": {
      "price_change_percentage_24h": { "gt": 0 }
    }
  },
  "sort": [
    { "timestamp": { "order": "desc" } }
  ],
  "collapse": { "field": "id" },
  "_source": ["name", "symbol", "current_price", "price_change_percentage_24h", "timestamp"]
}
""")
