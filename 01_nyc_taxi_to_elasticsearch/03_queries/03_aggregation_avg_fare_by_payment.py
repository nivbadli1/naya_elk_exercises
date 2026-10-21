"""
Step 3.3 - Nested aggregation + Painless script
-----------------------------------------------
Calculates the average fare amount per payment type - like SQL:
    SELECT payment_type, AVG(fare_amount) FROM nyctaxi GROUP BY payment_type

Concepts: sub-aggregation (metric inside a bucket), avg aggregation, Painless script.
fare_amount was indexed as a string, so we cannot run "avg" on the field directly.
A script converts the string to a number (Double) for every document at query time.
"""

# Elasticsearch is the Python client used to talk to the Elasticsearch REST API
from elasticsearch import Elasticsearch  # type: ignore

# Connect to Elasticsearch using its container hostname and REST port
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Aggregation: Average Fare Amount by Payment Type
# Define the index name we want to analyze
index_name = 'nyctaxi'

query_body_avg_fare = {
    # Return only aggregation results, no documents
    "size": 0,
    # Top-level aggregations
    "aggs": {
        # Our name for the bucket aggregation
        "avg_fare_by_payment_type": {
            # One bucket per payment type (GROUP BY payment_type)
            "terms": {
                "field": "payment_type.keyword"
            },
            # Sub-aggregation - calculated separately inside every bucket
            "aggs": {
                # Our name for the metric
                "average_fare": {
                    # Average of the values returned by the script
                    "avg": {
                        # Painless script that runs on each document
                        "script": {
                            # If the field exists and is not empty -> parse it to a Double,
                            # if parsing fails or the field is missing -> return null (ignored by avg)
                            "source": """
                                if (doc['fare_amount.keyword'].size() > 0 && !doc['fare_amount.keyword'].empty) {
                                    try {
                                        return Double.parseDouble(doc['fare_amount.keyword'].value);
                                    } catch (Exception e) {
                                        return null;
                                    }
                                } else {
                                    return null;
                                }
                                """
                        }
                    }
                }
            }
        }
    }
}

# Run the search with the aggregation
response = es.search(index=index_name, body=query_body_avg_fare)

# Print a title
print("Average Fare Amount by Payment Type:")
# Loop over the payment type buckets
for bucket in response['aggregations']['avg_fare_by_payment_type']['buckets']:
    # 'key' is the payment type; the sub-aggregation result is under bucket['average_fare']['value']
    print(f"Payment Type: {bucket['key']}, Average Fare: {bucket['average_fare']['value']}")


# Same query for Kibana Dev Tools:
# POST /nyctaxi/_search
# {
#   "size": 0,
#   "aggs": {
#     "avg_fare_by_payment_type": {
#       "terms": {
#         "field": "payment_type.keyword"
#       },
#       "aggs": {
#         "average_fare": {
#           "avg": {
#             "script": {
#               "source": """
#                 if (doc['fare_amount.keyword'].size() > 0 && !doc['fare_amount.keyword'].empty) {
#                   try {
#                     return Double.parseDouble(doc['fare_amount.keyword'].value);
#                   } catch (Exception e) {
#                     return null;
#                   }
#                 } else {
#                   return null;
#                 }
#               """
#             }
#           }
#         }
#       }
#     }
#   }
# }
