# Elasticsearch Lesson - Data Engineering Course

Hands-on exercises for the Elasticsearch lesson. Data is streamed from public APIs into
**Kafka**, written into **Elasticsearch**, and then searched and analyzed with the
Python client and **Kibana**.

The folders are numbered. Work through them in order.

| Folder | Where | What |
|--------|-------|------|
| `00_setup` | In class | Start Elasticsearch + Kibana |
| `01_nyc_taxi_to_elasticsearch` | In class | Kafka → Elasticsearch, first queries and aggregations |
| `02_crypto_pipeline` | In class (demo) | End-to-end pipeline: Airflow + Kafka + Spark → PostgreSQL / Elasticsearch |
| `03_kibana_guided` | In class | Guided Kibana exercise on the `nyctaxi` data: Dev Tools, Discover, dashboard |
| `04_homework` | **At home** | Build a crypto pipeline on your own: Kafka → Elasticsearch with a mapping → queries → Kibana dashboard |

```
naya_elk_exercises/
├── 00_setup/
│   └── docker-compose.yml                         # Elasticsearch + Kibana
├── 01_nyc_taxi_to_elasticsearch/                  # Part 1 - core Elasticsearch exercises
│   ├── 01_producer/taxi_producer.py               # NYC API  -> Kafka
│   ├── 02_consumer/taxi_consumer.py               # Kafka    -> Elasticsearch
│   └── 03_queries/
│       ├── 01_query_filtering.py                  # bool + term query
│       ├── 02_aggregation_trips_by_vendor.py      # terms aggregation (GROUP BY)
│       └── 03_aggregation_avg_fare_by_payment.py  # nested aggregation + script
├── 02_crypto_pipeline/                            # Part 2 - end-to-end pipeline
│   ├── 01_producer/coins_producer.py              # CoinGecko API -> Kafka
│   ├── 02_airflow/dag_ssh.py                      # Airflow runs the producer over SSH
│   └── 03_consumers/
│       ├── 01_consumer_postgres.py                # Spark: Kafka -> PostgreSQL
│       └── 02_consumer_elasticsearch.py           # Spark: Kafka -> Elasticsearch -> Kibana
├── 03_kibana_guided/
│   └── kibana_exercise.md                         # Part 3 - guided Kibana exercise (answers included)
└── 04_homework/                                   # Homework - done independently
    ├── README.md                                  # The assignment
    └── kibana_dashboard_guide.md                  # Step-by-step dashboard guide
```

---

## Run Order - Step by Step

Producers and consumers run forever, until you stop them with `Ctrl+C`. Give each one its own
terminal, and keep it open while you continue to the next step.

### Part 1 - NYC Taxi

| Step | Terminal | Run | Wait until / Check |
|------|----------|-----|--------------------|
| 1 | T1 | `cd 00_setup && docker compose up -d` | http://localhost:9200 returns JSON and http://localhost:5601 opens Kibana |
| 2 | T1 | `pip install -r requirements.txt` | Install finishes without errors |
| 3 | T2 | `python 01_nyc_taxi_to_elasticsearch/01_producer/taxi_producer.py` | Trip records are printed, one per second. **Keep it running.** |
| 4 | T3 | `python 01_nyc_taxi_to_elasticsearch/02_consumer/taxi_consumer.py` | No output, which is expected. **Keep it running.** |
| 5 | Kibana Dev Tools | `GET /nyctaxi/_count` | `count` is greater than 0 and grows each time you run it |
| 6 | Kibana Dev Tools | `GET /nyctaxi/_mapping` | Every field is `text` + `keyword`. This is the reason for `.keyword` in the queries. |
| 7 | T1 | `python 01_nyc_taxi_to_elasticsearch/03_queries/01_query_filtering.py` | A list of filtered trips |
| 8 | T1 | `python 01_nyc_taxi_to_elasticsearch/03_queries/02_aggregation_trips_by_vendor.py` | Trip count per vendor |
| 9 | T1 | `python 01_nyc_taxi_to_elasticsearch/03_queries/03_aggregation_avg_fare_by_payment.py` | Average fare per payment type |
| 10 | T2, T3 | `Ctrl+C` | Stop the producer and the consumer |

> In Part 1 you may start the consumer before or after the producer. The consumer uses
> `auto_offset_reset='earliest'`, so it also reads the messages that were sent before it started.

### Part 2 - Crypto Pipeline

Elasticsearch and Kibana from Part 1 must still be running.

| Step | Terminal | Run | Wait until / Check |
|------|----------|-----|--------------------|
| 1 | T2 | `spark-submit 02_crypto_pipeline/03_consumers/02_consumer_elasticsearch.py` | Spark starts and waits for data. **Keep it running.** |
| 2 | T3 *(optional)* | `spark-submit 02_crypto_pipeline/03_consumers/01_consumer_postgres.py` | Prints `Batch N ...` lines. **Keep it running.** |
| 3 | T1 | `export COINGECKO_API_KEY="<your key>"` then `python 02_crypto_pipeline/01_producer/coins_producer.py` | `Producing message to crypto_...` lines, then `Published data successfully` |
| 4 | T2 | - | Log line `Batch N written to Elasticsearch successfully.` |
| 5 | Kibana Dev Tools | `GET /crypto_data/_search` | Documents for 4 currencies |
| 6 | Airflow UI | Copy `dag_ssh.py` to the `dags/` folder, set up `ssh_default`, then trigger `produce_coins_to_kafka` | The task finishes green. This replaces running step 3 by hand. |
| 7 | Kibana | Create the `crypto_data` index pattern with the `timestamp` time field, then build the dashboard (Exercise 2.4) | The charts show the prices |
| 8 | Airflow UI | Trigger the DAG a few more times | New points appear on the dashboard |
| 9 | T2, T3 | `Ctrl+C` | Stop the Spark consumers |

> In Part 2, **start the Spark consumers before the producer.** Spark Structured Streaming reads
> Kafka from the `latest` offset by default, so it skips messages that were sent before it started.

---

## Prerequisites

- Docker and Docker Compose
- The course environment: Kafka (`course-kafka:9092`), Spark, Airflow, PostgreSQL
- Python 3.7+

```bash
pip install -r requirements.txt
```

> The Elasticsearch Python client must match the server's major version. The server here
> is 7.13.2, so the client is pinned to `elasticsearch==7.13.2`.

> The scripts connect to the hostnames `elasticsearch` and `course-kafka`. Run them from a
> container that is on the same Docker network as Elasticsearch and Kafka, or change the
> hosts to `localhost`.

---

## 00 - Setup: start Elasticsearch and Kibana

```bash
cd 00_setup
docker compose up -d
```

| Service       | URL                     | Check                                     |
|---------------|-------------------------|-------------------------------------------|
| Elasticsearch | http://localhost:9200   | Returns JSON with `"number" : "7.13.2"`   |
| Kibana        | http://localhost:5601   | Opens the Kibana UI (allow about a minute) |

Useful Kibana pages:
- **Dev Tools** (Menu → Management → Dev Tools): run raw Elasticsearch queries.
- **Stack Management → Index Patterns**: create an index pattern so Discover and Dashboards can use an index.
- **Discover**: browse the documents.

Quick health check in Dev Tools:
```
GET /_cluster/health
GET /_cat/indices?v
```

---

## Part 1 - NYC Taxi → Kafka → Elasticsearch

### Exercise 1.1 - Producer (`01_producer/taxi_producer.py`)
Reads NYC taxi trips from the [NYC Open Data API](https://data.cityofnewyork.us/resource/gi8d-wdg5.json)
and sends each trip as a JSON message to the Kafka topic **`my_trip`**, one message per second.

```bash
python 01_nyc_taxi_to_elasticsearch/01_producer/taxi_producer.py
```

### Exercise 1.2 - Consumer (`02_consumer/taxi_consumer.py`)
Reads from `my_trip` and indexes each message as a document in the index **`nyctaxi`**.
Run it in a second terminal while the producer is running.

```bash
python 01_nyc_taxi_to_elasticsearch/02_consumer/taxi_consumer.py
```

Check the result in Kibana Dev Tools:
```
GET /nyctaxi/_count
GET /nyctaxi/_search
GET /nyctaxi/_mapping
```

**Discussion: dynamic mapping.** The API sends every value as a string, for example
`"fare_amount": "12.5"`. Elasticsearch therefore maps every field as `text` with a `keyword`
sub-field. Look at `_mapping`. This explains why the queries below use `<field>.keyword`, and
why the average fare needs a script.

### Exercise 1.3 - Queries (`03_queries/`)
Each script runs the query from Python. The same query, ready to paste into Kibana Dev Tools,
is in a comment at the bottom of each file.

| # | File | Concept | SQL equivalent |
|---|------|---------|----------------|
| 1 | `01_query_filtering.py` | `bool` → `must` → `term` (exact match, AND) | `WHERE passenger_count='1' AND payment_type='2'` |
| 2 | `02_aggregation_trips_by_vendor.py` | `terms` bucket aggregation, `"size": 0` | `SELECT vendorid, COUNT(*) ... GROUP BY vendorid` |
| 3 | `03_aggregation_avg_fare_by_payment.py` | Sub-aggregation (`avg` inside `terms`) + Painless script | `SELECT payment_type, AVG(fare_amount) ... GROUP BY payment_type` |

```bash
python 01_nyc_taxi_to_elasticsearch/03_queries/01_query_filtering.py
python 01_nyc_taxi_to_elasticsearch/03_queries/02_aggregation_trips_by_vendor.py
python 01_nyc_taxi_to_elasticsearch/03_queries/03_aggregation_avg_fare_by_payment.py
```

**Student challenges**
1. Change the filter to trips with **more than** 2 passengers. Hint: `term` can't do this. Use a `range` query, and think about the field type.
2. Use `should` instead of `must`. How does the result change?
3. Add a `"size": 5` to the vendor aggregation, and add a sub-aggregation that computes `max` of `trip_distance` per vendor.
4. Create the `nyctaxi` index with an explicit mapping (`fare_amount` as `float`) **before** running the consumer, then rewrite query 3 without a script.

---

## Part 2 - Crypto pipeline: API → Kafka → Spark → PostgreSQL / Elasticsearch → Kibana

### Exercise 2.1 - Producer (`01_producer/coins_producer.py`)
Fetches market data for Bitcoin, Ethereum, Solana and Tether from the CoinGecko API, and sends
each coin to its own topic: `crypto_bitcoin`, `crypto_ethereum`, `crypto_solana` and `crypto_tether`.

Get a free demo API key from [CoinGecko](https://www.coingecko.com/en/api) and set it as an
environment variable. Never commit API keys to Git.

```bash
export COINGECKO_API_KEY="<your key>"
python 02_crypto_pipeline/01_producer/coins_producer.py
```

### Exercise 2.2 - Airflow DAG (`02_airflow/dag_ssh.py`)
The DAG `produce_coins_to_kafka` uses an `SSHOperator` to run the producer on the remote
machine where the code lives.

1. Copy the file into Airflow's `dags/` folder.
2. In Airflow UI → Admin → Connections, create an SSH connection named `ssh_default`.
3. Update the script path in `command` so it points to `coins_producer.py` on that machine.
4. Trigger the DAG manually. It has no schedule (`schedule_interval=None`).

### Exercise 2.3 - Spark consumers (`03_consumers/`)
Both consumers use Spark Structured Streaming with the same steps:
read Kafka → cast `value` to string → `from_json` with a schema → select and rename columns → `foreachBatch` write.

| File | Reads | Writes to |
|------|-------|-----------|
| `01_consumer_postgres.py` | `crypto_bitcoin` | PostgreSQL table `crypto_data` (JDBC) |
| `02_consumer_elasticsearch.py` | all 4 crypto topics | Elasticsearch index `crypto_data` |

```bash
spark-submit 02_crypto_pipeline/03_consumers/02_consumer_elasticsearch.py
```

> **Version note:** `02_consumer_elasticsearch.py` uses the connector
> `elasticsearch-spark-30_2.12:8.1.2`, while the server is 7.13.2. If you get version errors,
> switch to a 7.x connector, for example `org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2`.
>
> The checkpoint location is `s3a://elastic-checkpoint/`, the MinIO/S3 bucket of the course
> environment. For a local run, change it to a local folder such as `/tmp/elastic-checkpoint`.

### Exercise 2.4 - Kibana dashboard
1. Stack Management → Index Patterns → create `crypto_data`, with **`timestamp`** as the time field.
2. In Discover, view the incoming documents.
3. Build a dashboard with:
   - a line chart of the average `price` over `timestamp`, split by `currency`
   - a metric with the latest `market_cap` per currency
4. Trigger the Airflow DAG a few times and watch the dashboard update.

**Discussion:** Compare the `crypto_data` mapping with `nyctaxi`. Here Spark sends real
`double` and `timestamp` types, so aggregations work directly without scripts.

---

## Part 3 - Guided Kibana Exercise (`03_kibana_guided/kibana_exercise.md`)

Do this in class right after Part 1, while the `nyctaxi` index contains data.
Open the file and follow it in Kibana. Each question has a hidden answer: click
**Expected answer ✅** to show it.

| Section | Topic |
|---------|-------|
| Part 1 | Getting to know the index: `_count`, `_mapping`, sample documents |
| Part 2 | Filtering (WHERE): `term`, `bool` / `must` |
| Part 3 | Aggregations (GROUP BY): `terms`, nested aggregations |
| Part 4 | Discover and the first visualization |
| Part 5 | Build a 4-panel dashboard with auto refresh, plus discussion questions |
| Bonus | Script query compared with a `range` query on a mapped field |

---

## Homework (`04_homework/`)

After the lesson, students build the whole flow **on their own** with new data, crypto prices
from CoinGecko:

1. **Kafka Producer**: fetch the top 15 coins and send them to the topic `crypto_prices`.
2. **Kafka Consumer**: create the index `cryptoprices` with an **explicit mapping**, then index the records.
3. **3 analysis queries**: average prices, top gainers and losers, market trends.
4. **Kibana dashboard**: line chart, bar chart, heat map and data table. See `kibana_dashboard_guide.md`.

The full assignment is in [`04_homework/README.md`](04_homework/README.md).

How it differs from the class demo in `02_crypto_pipeline`: the consumer is plain Python, not
Spark, and the students define the mapping themselves. That fixes the `.keyword` and string
problems they saw with `nyctaxi`, so aggregations work without scripts.
