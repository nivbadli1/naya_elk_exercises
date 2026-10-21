"""
Step 3.2 - Bucket aggregation (terms)
-------------------------------------
Counts how many trips each vendor has - like SQL:
    SELECT vendorid, COUNT(*) FROM nyctaxi GROUP BY vendorid

Concepts: "size": 0, terms aggregation, buckets, doc_count.
"""

# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch

# Connect to Elasticsearch using its container hostname and REST port
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Define the index name we want to analyze
index_name = 'nyctaxi'


# Total Trips by Vendor
query_body_vendor_aggregation = {
    # We don't need individual documents, just the aggregation result
    "size": 0,
    # "aggs" defines the aggregations to calculate
    "aggs": {
        # Our name for this aggregation (used to read the result)
        "trips_by_vendor": {
            # terms = create one bucket per distinct value (GROUP BY)
            "terms": {
                # Group by the exact (keyword) value of vendorid
                "field": "vendorid.keyword"
            }
        }
    }
}

# Run the search with the aggregation
response = es.search(index=index_name, body=query_body_vendor_aggregation)

# Print a title
print("Total Trips by Vendor:")
# Every distinct vendor is one bucket in the result
for bucket in response['aggregations']['trips_by_vendor']['buckets']:
    # 'key' is the vendor id, 'doc_count' is how many documents (trips) are in the bucket
    print(f"Vendor ID: {bucket['key']}, Total Trips: {bucket['doc_count']}")


# Same query for Kibana Dev Tools:
# POST /nyctaxi/_search
# {
#     "size": 0,
#     "aggs": {
#         "trips_by_vendor": {
#             "terms": {
#                 "field": "vendorid.keyword"
#             }
#         }
#     }
# }
