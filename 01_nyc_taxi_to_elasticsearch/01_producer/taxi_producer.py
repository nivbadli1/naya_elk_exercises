"""
Step 1 - NYC Taxi Producer
--------------------------
Reads NYC taxi trip records from the NYC Open Data public API and sends
each record, one by one, to the Kafka topic "my_trip".

Flow:  NYC Open Data API  -->  Kafka topic "my_trip"
Next step: 02_consumer/taxi_consumer.py reads this topic and writes to Elasticsearch.
"""

# KafkaProducer is the client class used to send messages to Kafka
from kafka import KafkaProducer
# time - for sleeping between messages; json - to serialize records; requests - to call the HTTP API
import time, json, requests
# datetime - used to print a readable timestamp after each full API read
from datetime import datetime
# urllib3 - the HTTP library used by requests; used here only to silence SSL warnings
import urllib3

# The class network inspects HTTPS traffic, so certificate verification fails
# (SSLCertVerificationError). We call the API with verify=False below - acceptable for a
# public read-only dataset in a lab, never in production code.
# This line hides the "InsecureRequestWarning" that verify=False prints on every request.
urllib3.disable_warnings()

# Create the Kafka producer ONCE, outside the loop (one connection reused for all messages)
producer = KafkaProducer(bootstrap_servers="course-kafka:9092")

# Run forever: keep pulling data from the API and streaming it to Kafka
while True:
    # ======== Read from Remote API ==================== #
    # Send an HTTPS GET request to the NYC taxi trips dataset, skipping certificate verification
    response = requests.get(url="https://data.cityofnewyork.us/resource/gi8d-wdg5.json", verify=False)
    # Parse the JSON response once into a list of trips (each trip is a dict)
    trips = response.json()
    # Loop over every trip record in the list
    for row in trips:
        # Print the record so we can see what is being sent
        print(row)
        # Wait 1 second between messages to simulate a real-time stream
        time.sleep(1)
        # ============== Send to Kafka ========================== #
        # Convert the dict to a JSON string, encode it to bytes, and send it to topic "my_trip"
        producer.send(topic="my_trip", value=json.dumps(row).encode('utf-8'))
    # Make sure all buffered messages were actually delivered to Kafka
    producer.flush()
    # After all records were sent, print the current date and time
    print(datetime.fromtimestamp(time.time()))
    # Wait 3 seconds before reading the API again
    time.sleep(3)
