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


# Run forever: keep pulling data from the API and streaming it to Kafka
while True:
    # ======== Read from Remote API ==================== #
    # Send an HTTP GET request to the NYC taxi trips dataset (returns a JSON list of trips)
    response = requests.get(url="https://data.cityofnewyork.us/resource/gi8d-wdg5.json")
    # Loop over the index of every record in the returned JSON list
    for line in range(len(response.json())):
        # Take a single trip record (a Python dict) by its index
        row = response.json()[line]
        # Print the record so we can see what is being sent
        print(row)
        # Wait 1 second between messages to simulate a real-time stream
        time.sleep(1)
        # ============== Send to Kafka ========================== #
        # Create a producer that connects to the Kafka broker of the course environment
        producer = KafkaProducer(bootstrap_servers="course-kafka:9092")
        # Convert the dict to a JSON string, encode it to bytes, and send it to topic "my_trip"
        producer.send(topic="my_trip", value=json.dumps(row).encode('utf-8'))
    # After all records were sent, print the current date and time
    print(datetime.fromtimestamp(time.time()))
    # Wait 3 seconds before reading the API again
    time.sleep(3)
