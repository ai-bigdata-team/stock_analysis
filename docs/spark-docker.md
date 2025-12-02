# Chạy Spark bằng Docker cho dự án này

Hướng dẫn chạy các streaming jobs bằng Apache Spark trong Docker, đọc từ Kafka và ghi ra Parquet (local), Cassandra, hoặc GCS.

## Yêu cầu trước khi chạy
- Đã cài và bật Docker Desktop.
- Kafka có thể truy cập từ container (ví dụ trên Windows: `host.docker.internal:9092`; hoặc Kafka chạy cùng mạng Docker).
- Thư mục dự án trên host: `d:\Project\stock_analysis`.
- Tuỳ chọn:
  - Cassandra đang chạy và truy cập được (keyspace `stock_market`).
  - Service Account JSON key cho GCS (nếu dùng GCS sink).

## Image sử dụng
Sử dụng image Bitnami Spark cho đơn giản:
- `bitnami/spark:3.3` hoặc `bitnami/spark:3.5` (chọn phiên bản khớp package Kafka, ví dụ `spark-sql-kafka-0-10_2.12:3.3.2`).

## Lưu ý mạng (Windows)
- Từ container, dùng `host.docker.internal:9092` để truy cập Kafka chạy trên host.
- Nếu Kafka chạy trong Docker, gắn container Spark cùng network và dùng tên service.

## Cách A: Chạy `spark-submit` một lần trong container
Không cần viết compose; mount repo và chạy `spark-submit` trực tiếp.

### Luồng VN → Parquet (local)
```powershell
# Kéo image (một lần)
docker pull bitnami/spark:3.3

# Chạy spark-submit (Windows PowerShell)
docker run --rm -it \
  -v d:\Project\stock_analysis:/app \
  bitnami/spark:3.3 \
  /opt/bitnami/spark/bin/spark-submit \
  --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  /app/scripts/streaming/vnstock_kafka_streaming.py host.docker.internal:9092 vnstock_stock
```
Kết quả sẽ ghi vào `/app/output/...` (map tới `d:\Project\stock_analysis\output\...`).

### Luồng VN → Cassandra
```powershell
docker run --rm -it \
  -v d:\Project\stock_analysis:/app \
  bitnami/spark:3.3 \
  /opt/bitnami/spark/bin/spark-submit \
  --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
  --conf spark.cassandra.connection.host=host.docker.internal \
  /app/scripts/streaming/vnstock_kafka_to_localStorage.py
```

### Luồng Finnhub → Parquet (local)
```powershell
docker run --rm -it \
  -v d:\Project\stock_analysis:/app \
  bitnami/spark:3.3 \
  /opt/bitnami/spark/bin/spark-submit \
  --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  /app/scripts/streaming/finnhub_kafka_to_localStorage.py
```

### Luồng Finnhub → GCS
Dùng Python runner trong container với cấu hình GCS.
```powershell
docker run --rm -it \
  -v d:\Project\stock_analysis:/app \
  -e GCS_JSON_KEY=/app/secrets/gcs-key.json \
  bitnami/spark:3.3 \
  bash -lc "python /app/scripts/streaming/finnhub_kafka_to_gcs.py --bucket-name my-bucket --gcs-key /app/secrets/gcs-key.json"
```
Lưu ý: Production nên dùng `spark-submit` + JAR connector GCS; có thể mount JAR vào `/opt/bitnami/spark/jars/`.

## Cách B: docker-compose cho Spark (chế độ client) rồi tự chạy `spark-submit`
Tạo file `docker-compose.spark.yml` để chạy container Spark client, sau đó exec vào để chạy lệnh.

```yaml
version: "3.8"
services:
  spark:
    image: bitnami/spark:3.3
    container_name: spark-client
    volumes:
      - d:\\Project\\stock_analysis:/app
    environment:
      - SPARK_MODE=client
    networks:
      - sparknet
    stdin_open: true
    tty: true

networks:
  sparknet:
    external: false
```
Khởi động container:
```powershell
docker compose -f docker-compose.spark.yml up -d
```
Vào container và chạy `spark-submit`:
```powershell
docker exec -it spark-client bash
/opt/bitnami/spark/bin/spark-submit --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  /app/scripts/streaming/vnstock_kafka_streaming.py host.docker.internal:9092 vnstock_stock
```

## Chạy producers (Kafka) từ host
Các producer là script Python; chạy trên host để publish dữ liệu vào Kafka:
```powershell
$env:KAFKA_NODES = "localhost:9092"
python .\scripts\collect_data\StockProducer.py

$env:KAFKA_NODES = "localhost:9092"; $env:FINNHUB_API_KEY = "your_api_key"
python .\scripts\collect_data\RealtimeStockProducer.py
```
Nếu Kafka chạy trong Docker, đặt `KAFKA_NODES` theo tên service và cổng phù hợp.

## Kiểm tra nhanh Kafka
```powershell
& 'C:\\kafka\\bin\\windows\\kafka-console-consumer.bat' --bootstrap-server localhost:9092 --topic vnstock_stock --from-beginning
```
Nếu chạy trong container, dùng script Linux tương ứng và broker `host.docker.internal:9092`.

## Ghi chú & mẹo
- Đồng bộ phiên bản Spark/Kafka package (`spark-sql-kafka-0-10_2.12:<phiên bản spark>`).
- Cassandra: đảm bảo kết nối mạng/Firewall; trên Windows ưu tiên `host.docker.internal`.
- GCS: mount service account key vào container và cấu hình connector cho Spark.
- Spark UI: hoạt động trong container (port 4040). Có thể `docker exec -it spark-client bash` để kiểm tra logs, hoặc expose port nếu cần.
- Mọi output/checkpoint ghi vào `/app/output` và `/app/checkpoints` sẽ được lưu trên host qua volume đã mount.


### Sau khi cấu hình xong, cần tạo SparkSession để lưu:
ví dụ: 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VNStockStreaming") \
    .master("local[4]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.cassandra.connection.host", "host.docker.internal") \
    .getOrCreate()

# Đọc Kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
    .option("subscribe", "vnstock_stock") \
    .load()

# Ghi ra Parquet local
df.writeStream.format("parquet") \
    .option("path", "/app/output/vnstock") \
    .option("checkpointLocation", "/app/checkpoints/vnstock") \
    .start() \
    .awaitTermination()
