# 🚀 Hướng dẫn chạy StreamProcessor.scala

## 📋 Tổng quan

`StreamProcessor.scala` là Spark Structured Streaming application viết bằng Scala, đọc dữ liệu từ Kafka và ghi vào Cassandra.

## 🎯 Architecture

```
┌──────────┐      ┌──────────────┐      ┌───────────┐
│  Kafka   │ ──>  │    Spark     │ ──>  │ Cassandra │
│ (Avro)   │      │ Streaming    │      │ (NoSQL)   │
└──────────┘      └──────────────┘      └───────────┘
```

---

## 📦 Yêu cầu hệ thống

### 1. Software cần thiết:
- **Scala** 2.12.15
- **SBT** (Scala Build Tool) 1.5+
- **Apache Spark** 3.0.0
- **Kafka** 2.x+ đang chạy
- **Cassandra** 3.x+ đang chạy
- **Java** 8 hoặc 11

### 2. Kiểm tra version:
```bash
sbt --version         # SBT version
scala -version        # Scala version
java -version         # Java version
```

---

## 🔧 Cách 1: Chạy trên Local (Không dùng Kubernetes)

### Bước 1: Cài đặt SBT (nếu chưa có)

```bash
# Ubuntu/Debian
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt

# MacOS
brew install sbt
```

### Bước 2: Sửa file cấu hình cho local

Chỉnh sửa `src/main/resources/application.conf`:

```conf
kafka {
    server = "localhost"          # Thay vì kafka-service...
    port = "9092"
    topics {
        market = "finnhub_stock"  # Topic name của bạn
    }
}

cassandra {
    host = "localhost"            # Thay vì cassandra
    keyspace = "market"
    username = "cassandra"
    password = "cassandra"
}

spark {
    master = "local[*]"           # Chạy local thay vì cluster
    appName {
        StreamProcessor = "Local Stream Processor"
    }
}
```

### Bước 3: Build project

```bash
cd /home/pavt1024/bigdata/stock_analysis/references/finnhub-streaming-data-pipeline/StreamProcessor

# Build JAR file
sbt clean assembly

# Kết quả: tạo file target/scala-2.12/streamprocessor-assembly-1.0.jar
```

### Bước 4: Khởi động Cassandra (nếu chưa có)

```bash
# Option 1: Docker
docker run --name cassandra -p 9042:9042 -d cassandra:3.11

# Option 2: Local installation
sudo service cassandra start

# Tạo keyspace và tables
cqlsh
```

Trong cqlsh, chạy:
```cql
-- Tạo keyspace
CREATE KEYSPACE IF NOT EXISTS market 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Tạo table trades
CREATE TABLE IF NOT EXISTS market.trades (
    uuid text PRIMARY KEY,
    trade_conditions list<text>,
    price double,
    symbol text,
    trade_timestamp timestamp,
    volume int,
    ingest_timestamp timestamp
);

-- Tạo table aggregates
CREATE TABLE IF NOT EXISTS market.running_averages_15_sec (
    uuid text PRIMARY KEY,
    symbol text,
    price_volume_multiply double,
    ingest_timestamp timestamp
);

-- Verify
DESCRIBE market.trades;
```

### Bước 5: Chạy với spark-submit

```bash
spark-submit \
  --class StreamProcessor \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,\
org.apache.spark:spark-avro_2.12:3.0.0,\
com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  --conf spark.cassandra.connection.host=localhost \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  --conf spark.sql.shuffle.partitions=4 \
  target/scala-2.12/streamprocessor-assembly-1.0.jar
```

---

## 🐳 Cách 2: Chạy với Docker

### Bước 1: Build Docker image

```bash
cd /home/pavt1024/bigdata/stock_analysis/references/finnhub-streaming-data-pipeline/StreamProcessor

# Build image
docker build -t streamprocessor:latest .
```

### Bước 2: Chạy container

```bash
docker run \
  --network host \
  -e SPARK_MASTER_URL=spark://localhost:7077 \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e CASSANDRA_HOST=localhost \
  streamprocessor:latest
```

---

## ☸️ Cách 3: Chạy trên Kubernetes (Production)

### Yêu cầu:
- Kubernetes cluster đang chạy
- Spark Operator đã cài đặt
- Kafka và Cassandra đã deploy trên K8s

### Deploy:

```bash
# Tạo SparkApplication resource
kubectl apply -f deployment.yaml

# Check logs
kubectl logs -f spark-streamprocessor-driver

# Check status
kubectl get sparkapplications
```

---

## 🧪 Kiểm tra và Debug

### 1. Kiểm tra Kafka có data không:

```bash
# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Consume messages
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic finnhub_stock \
  --from-beginning \
  --max-messages 5
```

### 2. Kiểm tra Cassandra:

```bash
# Vào cqlsh
cqlsh localhost

# Query data
SELECT * FROM market.trades LIMIT 10;
SELECT * FROM market.running_averages_15_sec LIMIT 10;

# Count records
SELECT COUNT(*) FROM market.trades;
```

### 3. Xem Spark UI:

```bash
# Mở browser
http://localhost:4040

# Tabs quan trọng:
# - Streaming: batch processing time, records processed
# - SQL: query execution plans
# - Executors: resource usage
```

### 4. Check logs:

```bash
# Nếu chạy với spark-submit
tail -f /tmp/spark-events/*.log

# Nếu chạy trong Docker
docker logs -f <container_id>

# Nếu chạy trên K8s
kubectl logs -f <pod_name>
```

---

## 🔍 Troubleshooting

### Lỗi: "Connection refused to Kafka"

```bash
# Kiểm tra Kafka đang chạy
netstat -an | grep 9092

# Test connection
telnet localhost 9092

# Sửa server address trong application.conf
kafka.server = "localhost"  # hoặc IP của Kafka broker
```

### Lỗi: "NoHostAvailableException: Cassandra"

```bash
# Kiểm tra Cassandra đang chạy
cqlsh localhost 9042

# Verify connection trong spark-submit
--conf spark.cassandra.connection.host=localhost
--conf spark.cassandra.connection.port=9042
```

### Lỗi: "ClassNotFoundException: AvroDataSourceReader"

```bash
# Thêm package vào spark-submit
--packages org.apache.spark:spark-avro_2.12:3.0.0
```

### Lỗi: "Assembly JAR not found"

```bash
# Rebuild project
cd StreamProcessor
sbt clean assembly

# Verify JAR tồn tại
ls -lh target/scala-2.12/*.jar
```

---

## 📊 Monitoring trong Production

### 1. Metrics quan trọng:

```bash
# Processing rate
spark.streaming.receiver.recordsReceived

# Batch duration
spark.streaming.batchDuration

# Scheduling delay
spark.streaming.totalDelay
```

### 2. Alert conditions:

- Batch processing time > 10 seconds
- Scheduling delay tăng dần (backlog)
- Kafka consumer lag > 1000 records
- Cassandra write failures > 1%

### 3. Resource tuning:

```bash
spark-submit \
  --executor-memory 4G \
  --executor-cores 2 \
  --num-executors 3 \
  --conf spark.sql.shuffle.partitions=12 \
  ...
```

---

## 📚 Files quan trọng

```
StreamProcessor/
├── build.sbt                    # SBT build config
├── src/main/scala/
│   └── StreamProcessor.scala    # Main application
├── src/main/resources/
│   ├── application.conf         # Config cho local
│   ├── deployment.conf          # Config cho production
│   └── schemas/
│       └── trades.avsc          # Avro schema
└── target/
    └── scala-2.12/
        └── streamprocessor-assembly-1.0.jar  # Compiled JAR
```

---

## 🎯 Quick Start Command

Chạy nhanh trên local (tất cả services đã sẵn sàng):

```bash
# 1. Build
cd StreamProcessor && sbt assembly

# 2. Run
spark-submit \
  --class StreamProcessor \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-avro_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  target/scala-2.12/streamprocessor-assembly-1.0.jar

# 3. Monitor
open http://localhost:4040
```

---

## 💡 Tips

1. **Development**: Dùng `local[*]` để test nhanh
2. **Staging**: Deploy trên single-node Spark
3. **Production**: Dùng Spark cluster với HA
4. **Monitoring**: Tích hợp Prometheus + Grafana
5. **Logging**: Centralized logging với ELK stack

---

Cần hỗ trợ thêm về phần nào không? 🚀
