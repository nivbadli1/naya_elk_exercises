# Kibana Exercise — NYC Taxi Data
### Big Data Engineering Course

> **Before you start:** Make sure `01_nyc_taxi_to_elasticsearch/01_producer/taxi_producer.py` and
> `01_nyc_taxi_to_elasticsearch/02_consumer/taxi_consumer.py` are both running,
> and that data is visible in the `nyctaxi` index.
> Open Kibana → **Dev Tools** (the wrench icon in the left sidebar).

---

## Part 1 — Getting to Know the Index

### Exercise 1.1 — Check the index exists

Run the following and confirm you get a `count > 0`:

```
GET /nyctaxi/_count
```

**Question:** How many documents are currently in the index?

<details>
<summary>Expected answer ✅</summary>

The response looks like this:

```json
{
  "count": 1000,
  "_shards": { "total": 1, "successful": 1, "skipped": 0, "failed": 0 }
}
```

The exact number depends on how long the producer has been running.
Each taxi trip from the API = one document. If the consumer was restarted,
the count may be higher than expected because messages are re-read from offset 0
and re-indexed with a new auto-generated `_id`.
</details>

---

### Exercise 1.2 — Explore the field mapping

```
GET /nyctaxi/_mapping
```

**Questions:**
- What type is the `fare_amount` field? Why is it not a number?
- What is the difference between a `text` field and a `keyword` field?
- Why do we use `fare_amount.keyword` in our queries?

<details>
<summary>Expected answers ✅</summary>

**What type is `fare_amount`?**

```json
"fare_amount": {
  "type": "text",
  "fields": {
    "keyword": { "type": "keyword", "ignore_above": 256 }
  }
}
```

It is `text` — not `float` — because Elasticsearch used **dynamic mapping**.
When no mapping is defined in advance, Elasticsearch guesses the type from the first
document it sees. Since the NYC API sends all values as strings (e.g. `"8.5"`),
Elasticsearch mapped `fare_amount` as text.

**`text` vs `keyword`:**
| | `text` | `keyword` |
|---|---|---|
| Analyzed? | Yes — tokenized, lowercased | No — stored exactly as-is |
| Good for | Full-text search | Exact match, aggregations, sorting |
| Example | Search for "taxi" inside a sentence | Filter where `vendorid = "1"` |

**Why `.keyword` in queries?**
The `text` field is tokenized and can't be used for exact matching or aggregations.
The `.keyword` sub-field is the raw, unanalyzed version — required for `term` queries and `terms` aggregations.
</details>

---

### Exercise 1.3 — Fetch a sample document

```
GET /nyctaxi/_search
{
  "size": 3
}
```

**Question:** What fields does a taxi trip have? Write down at least 5.

<details>
<summary>Expected answer ✅</summary>

A typical document from the NYC Taxi dataset looks like this:

```json
{
  "vendorid": "2",
  "passenger_count": "1",
  "trip_distance": "1.5",
  "payment_type": "1",
  "fare_amount": "8.0",
  "tip_amount": "1.85",
  "total_amount": "11.3",
  "store_and_fwd_flag": "N",
  "ratecodeid": "1",
  "pulocationid": "162",
  "dolocationid": "161"
}
```

Key fields to know:
| Field | Meaning |
|---|---|
| `vendorid` | Taxi company (1 = Creative Mobile, 2 = VeriFone) |
| `passenger_count` | Number of passengers |
| `trip_distance` | Distance in miles |
| `payment_type` | How the trip was paid (see reference table at the bottom) |
| `fare_amount` | Base fare in USD |
| `tip_amount` | Tip in USD |
| `total_amount` | Total charged |
| `store_and_fwd_flag` | Was the trip stored locally before sending? (Y/N) |
</details>

---

## Part 2 — Filtering (WHERE)

### Exercise 2.1 — Filter by vendor

Find all trips from vendor `1`:

```
GET /nyctaxi/_search
{
  "query": {
    "term": {
      "vendorid.keyword": "1"
    }
  }
}
```

**Question:** How many hits did you get?

<details>
<summary>Expected answer ✅</summary>

Look at `hits.total.value` in the response:

```json
{
  "hits": {
    "total": { "value": 432, "relation": "eq" },
    ...
  }
}
```

The exact number depends on your data. Typically vendor `1` represents roughly
30–45% of trips. If you got `0`, check that the consumer has indexed data and
that you are using `.keyword` (not just `vendorid`).
</details>

---

### Exercise 2.2 — Filter with two conditions (AND)

Find all trips where:
- `passenger_count` is `2`
- `payment_type` is `1`

> Hint: use a `bool` query with `must`

<details>
<summary>Show answer ✅</summary>

```
GET /nyctaxi/_search
{
  "query": {
    "bool": {
      "must": [
        { "term": { "passenger_count.keyword": "2" } },
        { "term": { "payment_type.keyword": "1" } }
      ]
    }
  }
}
```

**Why `bool/must` and not two separate `term` queries?**
A single `term` query can only filter on one field. `bool/must` combines multiple
conditions — all of them must be true (equivalent to SQL `AND`).
</details>

---

### Exercise 2.3 — Your turn

Write a query that returns trips where `store_and_fwd_flag` is `"N"`.

<details>
<summary>Show answer ✅</summary>

```
GET /nyctaxi/_search
{
  "query": {
    "term": {
      "store_and_fwd_flag.keyword": "N"
    }
  }
}
```

`store_and_fwd_flag = "N"` means the trip record was sent directly to the server
without being stored locally first. This is the case for the vast majority of trips.
`"Y"` means the cab had no connection at the time and stored the record on board.
</details>

---

## Part 3 — Aggregations (GROUP BY)

### Exercise 3.1 — Count trips per payment type

```
GET /nyctaxi/_search
{
  "size": 0,
  "aggs": {
    "trips_by_payment": {
      "terms": {
        "field": "payment_type.keyword"
      }
    }
  }
}
```

**Questions:**
- Which payment type has the most trips?
- What does `"size": 0` do and why do we use it here?

<details>
<summary>Expected answers ✅</summary>

**Which payment type has the most trips?**

The response buckets look like this:

```json
"buckets": [
  { "key": "1", "doc_count": 680 },
  { "key": "2", "doc_count": 290 },
  { "key": "3", "doc_count": 18 },
  { "key": "4", "doc_count": 12 }
]
```

**Payment type `1` (Credit card)** almost always has the most trips in this dataset.

**What does `"size": 0` do?**

It tells Elasticsearch: "Don't return any individual documents in `hits.hits` — only return the aggregation results."
Without it, Elasticsearch would return 10 documents AND the aggregation, wasting bandwidth.
Since we only care about the counts per bucket, `size: 0` makes the response smaller and faster.

SQL equivalent:
```sql
-- size: 0 is like not doing SELECT * — we only want the GROUP BY result
SELECT payment_type, COUNT(*) FROM nyctaxi GROUP BY payment_type
```
</details>

---

### Exercise 3.2 — Count trips per number of passengers

Write an aggregation that groups trips by `passenger_count`.

> Hint: copy the query above and change the field name.

<details>
<summary>Show answer ✅</summary>

```
GET /nyctaxi/_search
{
  "size": 0,
  "aggs": {
    "trips_by_passengers": {
      "terms": {
        "field": "passenger_count.keyword"
      }
    }
  }
}
```

**Expected result:**

| passenger_count | doc_count |
|---|---|
| 1 | (highest — single passengers dominate) |
| 2 | (second) |
| 5 | (third — group rides) |
| 3 | ... |
| 6 | ... |
| 4 | (least common) |

The most common number of passengers is **1** — solo riders account for the
majority of NYC taxi trips.
</details>

---

### Exercise 3.3 — Nested aggregation

Group trips by vendor, and for each vendor show the breakdown by payment type:

```
GET /nyctaxi/_search
{
  "size": 0,
  "aggs": {
    "by_vendor": {
      "terms": {
        "field": "vendorid.keyword"
      },
      "aggs": {
        "by_payment": {
          "terms": {
            "field": "payment_type.keyword"
          }
        }
      }
    }
  }
}
```

**Question:** For vendor `2`, which payment type is most common?

<details>
<summary>Expected answer ✅</summary>

The response structure looks like this:

```json
"buckets": [
  {
    "key": "2",
    "doc_count": 680,
    "by_payment": {
      "buckets": [
        { "key": "1", "doc_count": 420 },
        { "key": "2", "doc_count": 240 },
        { "key": "3", "doc_count": 20 }
      ]
    }
  },
  {
    "key": "1",
    "doc_count": 320,
    "by_payment": { ... }
  }
]
```

For vendor `2`, **payment type `1` (Credit card)** is the most common.
This pattern holds for both vendors — credit card is dominant across the dataset.

**SQL equivalent of this nested aggregation:**
```sql
SELECT vendorid, payment_type, COUNT(*)
FROM nyctaxi
GROUP BY vendorid, payment_type
ORDER BY vendorid, COUNT(*) DESC
```
</details>

---

## Part 4 — Discover & Visualize (Kibana UI)

> Switch from Dev Tools to the **Discover** tab.

### Exercise 4.1 — Create an index pattern

1. Go to **Stack Management → Index Patterns**
2. Create a new pattern: `nyctaxi`
3. Skip the time field (our data has no timestamp)

### Exercise 4.2 — Explore in Discover

1. Open **Discover** and select the `nyctaxi` index pattern
2. Find the `fare_amount` field in the left panel — pin it to the table
3. Add `vendorid` and `passenger_count` to the table as well

**Question:** Can you spot any trips with unusually high or low fares?

<details>
<summary>Expected answer ✅</summary>

Click the `fare_amount` column header to sort. You may find:
- Fares of `0.0` or even negative values — these are cancelled or disputed trips (payment types 3 and 4)
- Very high fares (above `$50`) — typically long-distance trips or airport routes
- The most common range is `$6–$15` for typical short city rides

This is a good moment to discuss **data quality** — real-world streaming data
often contains outliers, nulls, and unexpected values that need to be handled
before analysis.
</details>

---

### Exercise 4.3 — Build a pie chart

1. Go to **Visualize → Create visualization → Pie**
2. Select the `nyctaxi` index
3. Add a bucket: **Split slices → Terms → `payment_type.keyword`**
4. Click **Update**

**Question:** What percentage of trips used payment type `1`?

<details>
<summary>Expected answer ✅</summary>

Hover over the largest slice to see the exact percentage.
In the NYC Taxi dataset, **payment type 1 (Credit card)** typically accounts
for **60–70%** of all trips.

If the slices are labeled with codes (1, 2, 3) instead of names — that is expected.
Elasticsearch stores the raw values from the API. To show human-readable labels,
you would need to add a scripted field or handle the mapping in the application layer.
</details>

---

## Part 5 — Building a Dashboard

> We will build a dashboard with **4 visualizations** and assemble them into one screen.
> Each visualization is created separately in **Visualize**, then added to a **Dashboard**.
>
> Make sure you already created the `nyctaxi` index pattern in Exercise 4.1.

---

### Visualization 1 — Pie chart: trips by payment type

**Goal:** See the share of each payment method across all trips.

**Steps:**
1. Go to **Visualize → Create visualization → Pie**
2. Select index: `nyctaxi`
3. Under **Buckets** click **Add → Split slices**
4. Aggregation: `Terms`
5. Field: `payment_type.keyword`
6. Size: `6` (to show all payment types)
7. Click **Update ▶**
8. Click **Save** → name it `Trips by Payment Type`

<details>
<summary>Expected result ✅</summary>

You should see a pie divided into slices — typically 2 dominant slices:
- **Type 1** (Credit card) — usually the largest slice (~60–70%)
- **Type 2** (Cash) — second largest (~25–35%)
- Types 3, 4, 5, 6 — small or absent depending on data volume

The legend on the right maps each color to a payment type code.
</details>

---

### Visualization 2 — Vertical bar chart: trips by vendor

**Goal:** Compare how many trips each vendor has.

**Steps:**
1. Go to **Visualize → Create visualization → Vertical Bar**
2. Select index: `nyctaxi`
3. Y-axis is already set to `Count` — leave it as is
4. Under **Buckets** click **Add → X-axis**
5. Aggregation: `Terms`
6. Field: `vendorid.keyword`
7. Order by: `Metric: Count` → Descending
8. Size: `5`
9. Click **Update ▶**
10. Click **Save** → name it `Trips by Vendor`

<details>
<summary>Expected result ✅</summary>

Two bars on the X-axis — one for vendor `1` and one for vendor `2`.
The height of each bar = number of trips for that vendor.
Vendor `2` is usually higher in this dataset.

If you see only one bar, it means all sampled trips belong to one vendor — normal with a small dataset.
</details>

---

### Visualization 3 — Metric: total number of trips

**Goal:** Show a single large number — total documents in the index.

**Steps:**
1. Go to **Visualize → Create visualization → Metric**
2. Select index: `nyctaxi`
3. The default metric is already `Count` — leave it as is
4. Click **Update ▶**
5. (Optional) Under **Options** set the font size to `60`
6. Click **Save** → name it `Total Trips`

<details>
<summary>Expected result ✅</summary>

A large number displayed in the center of the panel — total documents in `nyctaxi`.

The number will grow over time as the producer keeps sending data.
Refresh after a few minutes to see it increase.
</details>

---

### Visualization 4 — Data table: trip count by payment type

**Goal:** Show a table with each payment type and its trip count.

**Steps:**
1. Go to **Visualize → Create visualization → Data Table**
2. Select index: `nyctaxi`
3. Under **Buckets** click **Add → Split rows**
4. Aggregation: `Terms`
5. Field: `payment_type.keyword`
6. Size: `6`
7. Click **Update ▶**
8. Click **Save** → name it `Payment Type Table`

<details>
<summary>Expected result ✅</summary>

A table with two columns:

| payment_type.keyword | Count |
|---|---|
| 1 | (number of credit card trips) |
| 2 | (number of cash trips) |
| 3 | (no charge trips) |

**Teaching moment:** Notice we can't show average fare directly here — because
`fare_amount` was auto-mapped as `keyword` (string), not `float`.
This is a very common real-world mistake when ingesting data without a predefined mapping.

The fix is to define the mapping **before** indexing any data:

```
PUT /nyctaxi
{
  "mappings": {
    "properties": {
      "fare_amount":     { "type": "float" },
      "trip_distance":   { "type": "float" },
      "passenger_count": { "type": "integer" }
    }
  }
}
```

Once the mapping is explicit, the standard `avg` metric works without any Painless script.
</details>

---

### Assemble the Dashboard

**Goal:** Combine all 4 visualizations into one screen.

**Steps:**
1. Go to **Dashboard → Create new dashboard**
2. Click **Add** (top right corner)
3. Add all 4 saved visualizations:
   - `Total Trips`
   - `Trips by Payment Type`
   - `Trips by Vendor`
   - `Payment Type Table`
4. Drag and resize the panels into this layout:

```
┌──────────────┬─────────────────────────┐
│ Total Trips  │   Trips by Payment Type │
│   (Metric)   │        (Pie chart)      │
├──────────────┴─────────────────────────┤
│  Trips by Vendor   │  Payment Type     │
│   (Bar chart)      │  Table            │
└────────────────────┴───────────────────┘
```

5. Set auto-refresh: top right → **Auto refresh → 10 seconds**
6. Click **Save** → name it `NYC Taxi Overview`

<details>
<summary>Expected result ✅</summary>

A live dashboard that updates every 10 seconds.
You can watch the `Total Trips` metric increase in real time as the producer keeps sending messages through Kafka into Elasticsearch.

This is the core of what Kibana is built for — turning a stream of raw events into a live operational view.
</details>

---

### Dashboard — Discussion Questions

After building the dashboard, discuss these with the class:

**1. Why does the pie chart show codes (1, 2, 3) instead of names (Credit card, Cash)?**

> Because Elasticsearch stores exactly what it received from the API — raw codes.
> Translating codes to labels would happen in the application layer, or via a
> Kibana **scripted field** (Stack Management → Index Patterns → Scripted fields).

**2. The Total Trips metric keeps growing after a restart — why?**

> Because `auto_offset_reset='earliest'` in the consumer means every restart
> re-reads all messages from offset 0, creating **duplicate documents** in Elasticsearch.
> Elasticsearch auto-generates a new `_id` for each `es.index()` call, so duplicates are not detected.
>
> Fix: pass a stable `id` to `es.index()` — for example the Kafka offset:
> ```python
> es.index(index=ES_INDEX, id=message.offset, body=document)
> ```
> Now re-indexing the same offset overwrites the same document instead of creating a duplicate.

**3. What would you need to change to show Average Fare in the table?**

> Define an explicit `float` mapping for `fare_amount` before indexing (see Visualization 4 answer).
> Then delete the index, restart the consumer, and the standard `avg` metric will work
> in Kibana without needing a Painless script.

**4. What happens to the dashboard if the producer stops?**

> The data already in Elasticsearch stays — the dashboard keeps showing the last state.
> Nothing is lost. This is one of the key advantages of a Kafka buffer:
> the producer and consumer are fully decoupled.

---

## Bonus Challenge

Write a query that returns only trips where the `trip_distance` is greater than `5`.

> Hint: `trip_distance` is stored as a keyword string, so you'll need a script-based filter.

<details>
<summary>Show answer ✅</summary>

```
GET /nyctaxi/_search
{
  "query": {
    "script": {
      "script": {
        "source": "Double.parseDouble(doc['trip_distance.keyword'].value) > 5"
      }
    }
  }
}
```

This uses a **Painless script** to cast the string value to a float at query time.
It works, but it is slow — the script runs on every document in the index.

The proper solution is to define `trip_distance` as `float` in the mapping upfront,
and then use a standard `range` query:

```
GET /nyctaxi/_search
{
  "query": {
    "range": {
      "trip_distance": { "gt": 5 }
    }
  }
}
```

`range` queries on numeric fields use the inverted index and are much faster than scripts.
</details>

---

## Payment Type Reference

| Code | Meaning |
|------|---------|
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |
