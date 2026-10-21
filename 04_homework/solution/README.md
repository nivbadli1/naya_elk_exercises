# Homework - Solution

The reference solution for the [crypto homework](../README.md). Run every command from inside
the `dev_env` container, in this folder.

| File | Step | What it does |
|------|------|--------------|
| `crypto_producer.py` | 1 | Every 60 seconds: top 15 coins from CoinGecko → Kafka topic `crypto_prices` |
| `crypto_consumer.py` | 2 | Creates the `cryptoprices` index with an explicit mapping, then Kafka → Elasticsearch |
| `crypto_avg_prices.py` | 3.1 | Average price per coin, sorted from highest to lowest |
| `crypto_price_changes.py` | 3.2 | Top 5 gainers and top 5 losers by 24h % change, one document per coin (`collapse`) |
| `crypto_market_trends.py` | 3.3 | Market cap histogram, the overall average change, and the average change per coin |
| `load_historical_data.py` | Optional | Loads 30 days of daily prices, so the Kibana charts have history from the start |

## Run Order

| Step | Terminal | Run | Check |
|------|----------|-----|-------|
| 1 | T1 | `pip install kafka-python "elasticsearch==7.13.2" requests` | Installs without errors |
| 2 | T2 | `python crypto_consumer.py` | `Created Elasticsearch index: cryptoprices with custom mapping`. **Keep it running.** |
| 3 | T1 *(optional)* | `python load_historical_data.py` | `Total records sent to Kafka: ~465`. Takes about a minute. |
| 4 | T3 | `python crypto_producer.py` | `Sent: Bitcoin - $...` × 15, every 60 seconds. **Keep it running.** |
| 5 | Kibana Dev Tools | `GET /cryptoprices/_count` and `GET /cryptoprices/_mapping` | The count grows by 15 every minute. `current_price` is `float` and `timestamp` is `date`. |
| 6 | T1 | `python crypto_avg_prices.py`<br>`python crypto_price_changes.py`<br>`python crypto_market_trends.py` | Each script prints its results, followed by the same query for Kibana Dev Tools |
| 7 | Kibana | Follow [`../kibana_dashboard_guide.md`](../kibana_dashboard_guide.md) | 4 visualizations on one dashboard |

> Create the index **before** any data arrives. The consumer does this on startup, which is why it
> runs first. If an earlier run already created `cryptoprices` with dynamic mapping, delete it with
> `DELETE /cryptoprices` in Dev Tools and restart the consumer.

## Notes

- All API calls use `verify=False` because of the SSL inspection on the class network.
- The consumer uses `<coin id>_<timestamp>` as the document `_id`, so re-reading the same message
  overwrites the document instead of duplicating it.
- The historical loader uses the same names and symbols as the live API, so each coin lands in
  one bucket.
