"""
Homework solution - Query 1: Average price per cryptocurrency
-------------------------------------------------------------
SQL equivalent:
    SELECT name, AVG(current_price) AS average_price
    FROM cryptoprices GROUP BY name ORDER BY average_price DESC
"""

# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Index name
index_name = 'cryptoprices'

# Aggregation query: average price by cryptocurrency
query_body_avg_price = {
    # Return only the aggregation, no documents
    "size": 0,
    "aggs": {
        # Our name for the bucket aggregation
        "avg_price_by_crypto": {
            # One bucket per coin name (GROUP BY name)
            "terms": {
                # Exact name value
                "field": "name.keyword",
                # Up to 20 buckets (we have 15 coins)
                "size": 20,
                # Sort the buckets by the sub-aggregation below, highest first
                "order": {
                    "average_price": "desc"
                }
            },
            # Sub-aggregation, calculated inside every bucket
            "aggs": {
                "average_price": {
                    # current_price is mapped as float, so avg works directly (no script needed)
                    "avg": {
                        "field": "current_price"
                    }
                }
            }
        }
    }
}

# Run the query
response = es.search(index=index_name, body=query_body_avg_price)

# Print a title
print("Average Price by Cryptocurrency:")
print("-" * 50)
# One line per bucket (coin)
for bucket in response['aggregations']['avg_price_by_crypto']['buckets']:
    print(f"Cryptocurrency: {bucket['key']}, Average Price: ${bucket['average_price']['value']:.2f}")

# Print the same query for Kibana Dev Tools
print("\n")
print("Note: Equivalent Kibana/Dev Tools query:")
print("""
POST /cryptoprices/_search
{
  "size": 0,
  "aggs": {
    "avg_price_by_crypto": {
      "terms": {
        "field": "name.keyword",
        "size": 20,
        "order": {
          "average_price": "desc"
        }
      },
      "aggs": {
        "average_price": {
          "avg": {
            "field": "current_price"
          }
        }
      }
    }
  }
}
""")
