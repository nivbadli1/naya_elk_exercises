# Elasticsearch Lesson - Data Engineering Course

Hands-on exercises for the Elasticsearch lesson. Data is streamed from public APIs into
**Kafka**, written into **Elasticsearch**, and then searched and analyzed with the
Python client and **Kibana**.

The folders are numbered. Work through them in order.

| Folder | Where | What |
|--------|-------|------|
| `00_setup` | In class | Start the environment: Kafka, Kafdrop, Elasticsearch, Kibana, dev_env |
| `01_nyc_taxi_to_elasticsearch` | In class | Kafka → Elasticsearch, first queries and aggregations |
| `02_crypto_pipeline` | In class | Same pattern with live data: CoinGecko → Kafka → Elasticsearch → Kibana dashboard |
| `03_kibana_guided` | In class | Guided Kibana exercise on the `nyctaxi` data: Dev Tools, Discover, dashboard |
| `04_homework` | **At home** | Build a crypto pipeline on your own: Kafka → Elasticsearch with a mapping → queries → Kibana dashboard |

```
naya_elk_exercises/
├── 00_setup/
│   └── docker-compose.yml                         # Kafka + Kafdrop + Elasticsearch + Kibana + dev_env
├── 01_nyc_taxi_to_elasticsearch/                  # Part 1 - core Elasticsearch exercises
│   ├── 01_producer/taxi_producer.py               # NYC API  -> Kafka
│   ├── 02_consumer/taxi_consumer.py               # Kafka    -> Elasticsearch
│   └── 03_queries/
│       ├── 01_query_filtering.py                  # bool + term query
│       ├── 02_aggregation_trips_by_vendor.py      # terms aggregation (GROUP BY)
│       └── 03_aggregation_avg_fare_by_payment.py  # nested aggregation + script
├── 02_crypto_pipeline/                            # Part 2 - end-to-end pipeline
│   ├── 01_producer/coins_producer.py              # CoinGecko API -> Kafka (every 60 seconds)
│   └── 02_consumer/coins_consumer.py              # Kafka -> Elasticsearch (crypto_data)
├── 03_kibana_guided/
│   └── kibana_exercise.md                         # Part 3 - guided Kibana exercise (answers included)
└── 04_homework/                                   # Homework - done independently
    ├── README.md                                  # The assignment
    ├── kibana_dashboard_guide.md                  # Step-by-step dashboard guide
    └── solution/                                  # Reference solution + run order
```

---

## Run Order - Step by Step

Producers and consumers run forever, until you stop them with `Ctrl+C`. Give each one its own
terminal, and keep it open while you continue to the next step.

### Part 1 - NYC Taxi

| Step | Terminal | Run | Wait until / Check |
|------|----------|-----|--------------------|
| 1 | T1 | `cd 00_setup && docker compose up -d` | `docker ps` shows 6 containers. http://localhost:9200, http://localhost:5601 (Kibana) and http://localhost:9003 (Kafdrop) open. From here on, run every command inside `dev_env`. |
| 2 | T1 | `pip install -r requirements.txt` | Install finishes without errors |
| 3 | T2 | `python 01_nyc_taxi_to_elasticsearch/01_producer/taxi_producer.py` | Trip records are printed, one per second, and the topic `my_trip` appears in Kafdrop. **Keep it running.** |
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
| 1 | T2 | `python 02_crypto_pipeline/01_producer/coins_producer.py` | `Producing message to crypto_...` lines, then `Published data successfully`, repeated every 60 seconds. **Keep it running.** |
| 2 | Kafdrop | http://localhost:9003 | 4 new topics: `crypto_bitcoin`, `crypto_ethereum`, `crypto_solana`, `crypto_tether` |
| 3 | T3 | `python 02_crypto_pipeline/02_consumer/coins_consumer.py` | `Indexed bitcoin: price=...` lines. **Keep it running.** |
| 4 | Kibana Dev Tools | `GET /crypto_data/_count` | `count` grows by 4 every minute |
| 5 | Kibana Dev Tools | `GET /crypto_data/_mapping` | `price` is `float` and `timestamp` is `date`. Compare with `nyctaxi`. |
| 6 | Kibana | Create the `crypto_data` index pattern with the `timestamp` time field, then build the dashboard (Exercise 2.3) | The charts show the prices, with a new point every minute |
| 7 | T2, T3 | `Ctrl+C` | Stop the producer and the consumer |

> As in Part 1, the order of the producer and the consumer doesn't matter. The consumer uses
> `auto_offset_reset='earliest'`.

---

## Prerequisites

- Docker and Docker Compose. `00_setup/docker-compose.yml` starts Kafka, Elasticsearch, Kibana and the dev container.
- Python 3.7+

```bash
pip install -r requirements.txt
```

> The Elasticsearch Python client must match the server's major version. The server here
> is 7.13.2, so the client is pinned to `elasticsearch==7.13.2`.

> The scripts connect to the hostnames `elasticsearch` and `course-kafka`. Run them from
> **inside the `dev_env` container**, which is on the same Docker network, so these names resolve.
> Connect over SSH (`ssh -p 22022 <user>@localhost`), use VS Code Remote-SSH, or use Jupyter at
> http://localhost:8888.

---

## 00 - Setup: start the environment

```bash
cd 00_setup
docker compose up -d
docker ps          # all 6 containers should be "Up"
```

| Service | Container | URL / Port | What it is for |
|---------|-----------|------------|----------------|
| Zookeeper | `zookeeper` | `2181` | Coordination service required by Kafka |
| Kafka | `course-kafka` | `9092` | Message broker. Topics: `my_trip`, `crypto_*` |
| Kafdrop | `kafdrop` | http://localhost:9003 | Web UI to browse Kafka topics and messages |
| Elasticsearch | `elasticsearch` | http://localhost:9200 | Returns JSON with `"number" : "7.13.2"` |
| Kibana | `kibana` | http://localhost:5601 | UI for Elasticsearch (allow about a minute to start) |
| Dev environment | `dev_env` | SSH `22022`, Jupyter http://localhost:8888, Spark UI http://localhost:4040-4042 | Run the Python scripts from here |

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

## Part 2 - Crypto pipeline: CoinGecko API → Kafka → Elasticsearch → Kibana

### Exercise 2.1 - Producer (`01_producer/coins_producer.py`)
Fetches market data for Bitcoin, Ethereum, Solana and Tether from the CoinGecko API, and sends
each coin to its own topic: `crypto_bitcoin`, `crypto_ethereum`, `crypto_solana` and `crypto_tether`.

The producer runs on its own and fetches new prices every 60 seconds until you stop it with `Ctrl+C`.
The API works without a key. A free demo key from [CoinGecko](https://www.coingecko.com/en/api)
raises the rate limit. Set it as an environment variable, and never commit API keys to Git.

```bash
export COINGECKO_API_KEY="<your key>"   # optional
python 02_crypto_pipeline/01_producer/coins_producer.py
```

### Exercise 2.2 - Consumer (`02_consumer/coins_consumer.py`)
The same pattern as the taxi consumer, with three differences:
- It subscribes to **4 topics** at once.
- It keeps only 5 fields and renames them: `currency`, `timestamp`, `price`, `volume` and `market_cap`.
- It sets its own document `_id` (`<currency>_<timestamp>`). If the producer fetches the same price
  twice, the document is overwritten instead of duplicated.

```bash
python 02_crypto_pipeline/02_consumer/coins_consumer.py
```

### Exercise 2.3 - Kibana dashboard
1. Stack Management → Index Patterns → create `crypto_data`, with **`timestamp`** as the time field.
2. In Discover, view the incoming documents.
3. Build a dashboard with:
   - a line chart of the average `price` over `timestamp`, split by `currency`
   - a metric with the latest `market_cap` per currency
4. Keep the producer running and watch the dashboard update every minute.

**Discussion:** Compare the `crypto_data` mapping with `nyctaxi`. CoinGecko sends real numbers
and ISO dates, so dynamic mapping creates `float` and `date` fields. Aggregations and time
charts then work directly, without `.keyword` or scripts.

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

The full assignment is in [`04_homework/README.md`](04_homework/README.md). The reference
solution and its run order are in [`04_homework/solution/`](04_homework/solution/README.md).

How it differs from `02_crypto_pipeline` in class: the students write all the code
themselves, define an **explicit mapping** before indexing, fetch 15 coins instead of 4, and
add 3 analysis queries.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `SSLError ... CERTIFICATE_VERIFY_FAILED` in a producer | The network inspects HTTPS traffic | Both producers already call the API with `verify=False`. If you write your own code, do the same. |
| `UnsupportedProductError: The client noticed that the server is not Elasticsearch` | The Python client is 8.x or newer, but the server is 7.13.2 | `pip uninstall -y elasticsearch elastic-transport && pip install "elasticsearch==7.13.2"`, then restart the Jupyter kernel |
| `course-kafka` container exits right after `docker compose up` | Kafka restarted before Zookeeper cleared the old session | Wait about 10 seconds, then run `docker start course-kafka` |
| `ElasticsearchWarning: ... security features are not enabled` | Elasticsearch runs without a password (lab setup) | Only a warning. Safe to ignore in class. |
