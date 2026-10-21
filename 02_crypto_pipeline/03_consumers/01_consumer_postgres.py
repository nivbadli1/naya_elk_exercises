"""
Step 3.1 - Spark Structured Streaming: Kafka -> PostgreSQL
----------------------------------------------------------
Reads Bitcoin messages from the Kafka topic "crypto_bitcoin", parses the JSON,
and appends every micro-batch to the PostgreSQL table "crypto_data".

Flow:  Kafka topic crypto_bitcoin  -->  Spark  -->  PostgreSQL table crypto_data
Compare with 02_consumer_elasticsearch.py, which writes the same data to Elasticsearch.
"""

# SparkSession is the entry point to Spark SQL / Structured Streaming
from pyspark.sql import SparkSession
# col - reference a column; from_json - parse a JSON string column into a struct
from pyspark.sql.functions import col, from_json
# Types used to describe the JSON schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


# Create a SparkSession with Kafka and PostgreSQL support
spark = SparkSession.builder \
    .appName("CryptoKafkaConsumer") \
    .config("spark.master", "local[*]") \
    .config("spark.jars", "/opt/driver/postgresql-42.5.6.jar") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()
# appName      - name of the job in the Spark UI
# spark.master - run locally using all CPU cores
# spark.jars   - PostgreSQL JDBC driver jar (needed to write to Postgres)
# spark.jars.packages - Kafka connector for Spark Structured Streaming

# Define the schema of the JSON messages (only the fields we need)
schema = StructType([
    # Coin id, e.g. "bitcoin"
    StructField("id", StringType(), True),
    # Coin symbol, e.g. "btc"
    StructField("symbol", StringType(), True),
    # Coin display name, e.g. "Bitcoin"
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

# Read a stream of messages from Kafka
crypto_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "crypto_bitcoin") \
    .load()
# readStream - streaming (not batch) read
# format("kafka") - the source is Kafka
# kafka.bootstrap.servers - the broker address
# subscribe - the topic to read

# Kafka delivers the message value as binary - cast it to a string
crypto_df = crypto_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON string with our schema into a "data" struct, then flatten it to columns
crypto_df = crypto_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Select the relevant columns and rename them to match the PostgreSQL table
crypto_df = crypto_df.select(
    # id -> currency
    col("id").alias("currency"),
    # last_updated -> dates
    col("last_updated").alias("dates"),
    # current_price -> price
    col("current_price").alias("price"),
    # total_volume -> volume
    col("total_volume").alias("volume"),
    # keep market_cap as is
    col("market_cap")
)


# Function that is called for every micro-batch of the stream
def write_to_postgresql(df, epoch_id):
    # If the batch has no rows, there is nothing to write
    if df.count() == 0:
        # Log the empty batch
        print(f"Batch {epoch_id} is empty, nothing to write.")
    else:
        # Log how many rows are in the batch
        print(f"Batch {epoch_id} has {df.count()} records, writing to PostgreSQL.")
        # Display the data in the current batch
        df.show()
        # Write the batch to PostgreSQL using JDBC
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "crypto_data") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .mode("append") \
            .save()
        # url      - JDBC address of the PostgreSQL database
        # driver   - JDBC driver class
        # dbtable  - target table
        # user / password - database credentials (course defaults)
        # mode("append") - add rows, don't overwrite the table


# Start the stream: call write_to_postgresql for every micro-batch, and keep running until stopped
crypto_df.writeStream \
    .foreachBatch(write_to_postgresql) \
    .start() \
    .awaitTermination()
