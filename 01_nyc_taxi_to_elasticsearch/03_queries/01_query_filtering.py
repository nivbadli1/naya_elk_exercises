"""
Step 3.1 - Filtering query (bool + term)
----------------------------------------
Finds trips that had exactly 1 passenger AND were paid with payment type 2.

Concepts: bool query, "must" clause, term query, .keyword sub-field.
All fields arrive from the API as strings, so Elasticsearch maps them as "text"
with a "keyword" sub-field. A term query needs an exact value, so we use "<field>.keyword".
"""

# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch

# Connect to Elasticsearch using its container hostname and REST port
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Define the index name we want to search
index_name = 'nyctaxi'

# The query body (Elasticsearch Query DSL, written as a Python dict)
query_body_filter = {
    # "query" defines which documents match
    "query": {
        # A bool query combines several conditions
        "bool": {
            # "must" = every condition in the list has to match (logical AND)
            "must": [
                # Exact match: passenger_count is "1"
                {"term": {"passenger_count.keyword": "1"}},
                # Exact match: payment_type is "2"
                {"term": {"payment_type.keyword": "2"}}
            ]
        }
    }
}

# Run the search on the index (returns 10 hits by default)
response = es.search(index=index_name, body=query_body_filter)

# Print a title
print("Filtered Trips:")
# Every matching document is in response['hits']['hits']
for hit in response['hits']['hits']:
    # '_source' holds the original JSON document we indexed
    print(hit['_source'])


# Same query for Kibana Dev Tools (http://localhost:5601 -> Dev Tools):
# GET /nyctaxi/_search
# {
#   "query": {
#     "bool": {
#       "must": [
#         { "term": { "passenger_count.keyword": "1" } },
#         { "term": { "payment_type.keyword": "2" } }
#       ]
#     }
#   }
# }
