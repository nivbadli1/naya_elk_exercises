"""
Step 3.2 - Spark Structured Streaming: Kafka -> Elasticsearch
-------------------------------------------------------------
Reads all four crypto topics from Kafka, parses the JSON, and writes every
micro-batch to the Elasticsearch index "crypto_data". Then build a Kibana
dashboard on top of the index (price over time per currency).

Flow:  Kafka topics crypto_*  -->  Spark  -->  Elasticsearch index crypto_data  -->  Kibana
"""

# logging - prints structured log messages instead of plain print()
import logging
# SparkSession is the entry point to Spark SQL / Structured Streaming
from pyspark.sql import SparkSession
# col - reference a column; from_json - parse a JSON string column into a struct
from pyspark.sql.functions import col, from_json
# Types used to describe the JSON schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configure logging to show INFO level messages and above
logging.basicConfig(level=logging.INFO)
# Create a logger for this file
logger = logging.getLogger(__name__)

# Wrap everything in try/except so any error is logged
try:
    # Initialize a SparkSession with the Kafka and Elasticsearch connector packages
    spark = SparkSession.builder \
        .appName("CryptoKafkaConsumer") \
        .config("spark.master", "local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.1.2") \
        .getOrCreate()
    # spark-sql-kafka         - lets Spark read from Kafka
    # elasticsearch-spark-30  - lets Spark write to Elasticsearch (format "org.elasticsearch.spark.sql")

    # Define the schema of the JSON messages
    schema = StructType([
        # Coin id, e.g. "bitcoin"
        StructField("id", StringType(), True),
        # Coin symbol, e.g. "btc"
        StructField("symbol", StringType(), True),
        # Coin display name
        StructField("name", StringType(), True),
        # Current price in USD
        StructField("current_price", DoubleType(), True),
        # Market capitalization in USD
        StructField("market_cap", DoubleType(), True),
        # 24h trading volume in USD
        StructField("total_volume", DoubleType(), True),
        # When CoinGecko last updated the price
        StructField("last_updated", TimestampType(), True)
    ])

    # Read a stream from Kafka - subscribe to all four crypto topics (comma separated)
    crypto_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("subscribe", "crypto_bitcoin,crypto_ethereum,crypto_solana,crypto_tether") \
        .load()

    # Kafka delivers the message value as binary - cast it to a string
    crypto_df = crypto_df.selectExpr("CAST(value AS STRING)")

    # Parse the JSON string with our schema into a "data" struct, then flatten it to columns
    crypto_df = crypto_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

    # Select the relevant columns and rename them
    crypto_df = crypto_df.select(
        # id -> currency
        col("id").alias("currency"),
        # last_updated -> timestamp (a real date type, so Kibana can use it as the time field)
        col("last_updated").alias("timestamp"),
        # current_price -> price
        col("current_price").alias("price"),
        # total_volume -> volume
        col("total_volume").alias("volume"),
        # keep market_cap as is
        col("market_cap")
    )

    # Function that is called for every micro-batch of the stream
    def write_to_elasticsearch(df, epoch_id):
        # Catch errors per batch so one bad batch does not stop the whole stream
        try:
            # Write the batch to Elasticsearch
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "http://elasticsearch:9200") \
                .option("es.resource", "crypto_data/_doc") \
                .mode("append") \
                .save()
            # format       - the Elasticsearch Spark connector
            # es.nodes     - Elasticsearch address
            # es.resource  - target index (crypto_data) / document type (_doc)
            # mode("append") - add new documents
            # Log success
            logger.info(f"Batch {epoch_id} written to Elasticsearch successfully.")
        except Exception as e:
            # Log the error for this batch
            logger.error(f"Error writing batch {epoch_id} to Elasticsearch: {e}")

    # Start the stream: call write_to_elasticsearch for every micro-batch
    query = crypto_df.writeStream \
        .foreachBatch(write_to_elasticsearch) \
        .option("checkpointLocation", "s3a://elastic-checkpoint/") \
        .start()
    # checkpointLocation - where Spark saves its progress (Kafka offsets) so a restart
    # continues from where it stopped. Here it is an S3/MinIO bucket of the course environment.

    # Keep the application running until the stream is stopped
    query.awaitTermination()

# Log any error that happened while building or running the stream
except Exception as e:
    logger.error(f"Error in streaming process: {e}")
