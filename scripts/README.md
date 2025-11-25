# scripts/ — Quick runbook

Tài liệu ngắn để chạy và debug các producer + streaming jobs nằm trong `scripts/`.

**Overview**
- Producers (thu thập):
  - `scripts/collect_data/StockProducer.py` — lấy OHLCV từ `vnstock` (polling) → gửi Kafka topic `vnstock_stock`.
  - `scripts/collect_data/RealtimeStockProducer.py` — kết nối WebSocket Finnhub → gửi Kafka topic `finnhub_stock`.
- Streaming jobs (consumer & transform):
  - `scripts/streaming/vnstock_kafka_streaming.py` — Kafka → transform → Parquet (local).
  - `scripts/streaming/vnstock_kafka_to_localStorage.py` — Kafka → transform → Cassandra (checkpoint local). 
  - `scripts/streaming/finnhub_kafka_to_localStorage.py` — Kafka → transform → Parquet/Cassandra.
  - `scripts/streaming/finnhub_kafka_to_gcs.py` — Kafka → transform → Google Cloud Storage (GCS) datalake.

**Important Kafka topics**
- `vnstock_stock` — VN OHLCV messages from `StockProducer`.
- `finnhub_stock` — Finnhub realtime trade messages from `RealtimeStockProducer`.

**Env vars / config used by scripts**
- `KAFKA_NODES` — Kafka bootstrap servers (e.g. `localhost:9092`).
- `FINNHUB_API_KEY` — Finnhub API key used by `RealtimeStockProducer`.
- `GCS_JSON_KEY` or `--gcs-key` arg — path to GCS service account JSON key for GCS pipeline.
- Cassandra: if writing to Cassandra, configure `spark.cassandra.connection.host` (default: `localhost`) and provide connector package to `spark-submit`.

**Quick debug: read raw messages from Kafka**
(Assumes local Kafka CLI is available)

PowerShell examples:

```powershell
# vnstock topic
& 'C:\kafka\bin\windows\kafka-console-consumer.bat' --bootstrap-server localhost:9092 --topic vnstock_stock --from-beginning

# finnhub topic
& 'C:\kafka\bin\windows\kafka-console-consumer.bat' --bootstrap-server localhost:9092 --topic finnhub_stock --from-beginning
```

If Kafka is in Docker, replace bootstrap-server with appropriate host/port.

**Run producers (examples)**
- Run VNStock producer (polling, requires vnstock library + `KAFKA_NODES` set):

```powershell
python .\scripts\collect_data\StockProducer.py
```

- Run Finnhub realtime producer (needs `FINNHUB_API_KEY`):

```powershell
$env:FINNHUB_API_KEY = "your_api_key"
python .\scripts\collect_data\RealtimeStockProducer.py
```

**Run Spark streaming jobs (PowerShell examples)**
- Common notes: use `spark-submit` with Kafka connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2`. If writing to Cassandra add `spark-cassandra-connector` package. For GCS pipeline, supply GCS connector JAR and `--conf` pointing to service account key or use env var.

- Run VNSTOCK → Parquet (local):

```powershell
spark-submit --master local[4] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 `
  d:\Project\stock_analysis\scripts\streaming\vnstock_kafka_streaming.py `
  "localhost:9092" "vnstock_stock"
```

- Run VNSTOCK → Cassandra (example with Cassandra connector):

```powershell
spark-submit --master local[4] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 `
  d:\Project\stock_analysis\scripts\streaming\vnstock_kafka_to_localStorage.py
```

- Run Finnhub → GCS (needs GCS connector and service account key):

```powershell
# Example: run with python if PySpark env configured, otherwise use spark-submit with jars
python .\scripts\streaming\finnhub_kafka_to_gcs.py --bucket-name my-bucket --gcs-key "C:\path\to\service-account.json"
```

Or use `spark-submit` and include the GCS connector JAR and kafka package.

**Checkpoint & outputs (where to look)**
- Checkpoints (metadata & offsets) used by Spark streaming in these scripts are under `./checkpoints/` by default (e.g. `./checkpoints/vnstock_raw`, `./checkpoints/vnstock_aggregates`).
- Local Parquet outputs (if running `vnstock_kafka_streaming.py`) default to `./output/vnstock_ohlcv` and `./output/vnstock_aggregates`.
- Cassandra tables for `vnstock_kafka_to_localStorage.py`: `vnstock_ohlcv` and `vnstock_aggregates` in keyspace `stock_market` (default). Verify rows via cqlsh or your Cassandra client.

**Monitoring & troubleshooting**
- Spark UI: usually `http://localhost:4040` when running locally — check jobs, stages, streaming query status.
- Kafka offsets: use Kafka tooling or `kafka-consumer-groups.sh` to inspect consumer groups and lag.
- Cassandra: monitor write latencies and table sizes; use `nodetool` and `cqlsh` for inspection.
- Logs: scripts use Python logging; Spark logs appear in console and Spark UI.

**Non-invasive testing tips**
- Use `kafka-console-consumer` to validate messages before starting Spark jobs.
- Run `vnstock_kafka_streaming.py` writing to Parquet first (no Cassandra) for fast feedback.
- For GCS, validate service account key exists and GCS connector jar is present before running.

**If you want, I can next:**
- Add a small `scripts/bin/run_vnstock_pipeline.ps1` helper (spark-submit wrapper). 
- Or keep only this README. 

(End of `scripts/README.md`)
