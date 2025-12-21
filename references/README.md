# Tham khảo: Công nghệ và luồng thực thi của 4 repo dữ liệu chứng khoán

Tài liệu này tóm tắt công nghệ sử dụng và luồng thực thi (execution flow) của 4 repository liên quan tới big data/data engineering và dữ liệu chứng khoán.

---

## 1) Finance-Data-Ingestion-Pipeline-with-Kafka

- Repo: https://github.com/longNguyen010203/Finance-Data-Ingestion-Pipeline-with-Kafka
- Mục tiêu: Xây dựng pipeline realtime thu thập và xử lý dữ liệu tài chính từ Yahoo Finance (yfinance) và Finnhub, streaming qua Kafka, xử lý bằng Spark, lưu Cassandra, orchestration bằng Airflow.

### Công nghệ chính
- Dữ liệu/API: Yahoo Finance (thư viện yfinance, interval 1 phút), Finnhub API (realtime)
- Streaming/Messaging: Apache Kafka
- Xử lý realtime: Apache Spark Streaming (3 worker nodes)
- Lưu trữ: Apache Cassandra
- Orchestration/Packaging: Apache Airflow, Docker, docker-compose
- Phân tích/Trình bày: Jupyter Notebook, Power BI

### Luồng thực thi
1. Ingest: 
   - yfinance (REST) thu thập OHLCV theo phút
   - Finnhub (realtime) thu thập giao dịch/giá
2. Produce: 
   - Ghi vào các Kafka topic theo source (Yahoo/Finnhub)
3. Process (Realtime): 
   - Spark Streaming (Kafka consumer) đọc các topic, làm sạch/biến đổi/aggregate
4. Load: 
   - Ghi kết quả vào Cassandra
5. Orchestrate: 
   - Airflow điều phối các tác vụ (extract → transform → load) trong môi trường Docker
6. Serve/BI: 
   - Phân tích bằng Notebook, lập báo cáo/biểu đồ với Power BI

---

## 2) finnhub-streaming-data-pipeline

- Repo: https://github.com/RSKriegs/finnhub-streaming-data-pipeline
- Mục tiêu: Pipeline streaming realtime từ Finnhub WebSocket, chuẩn hoá Avro, Kafka làm broker, Spark Structured Streaming xử lý, lưu Cassandra, quan sát qua Grafana; triển khai trên Kubernetes (Minikube) và IaC bằng Terraform.

### Công nghệ chính
- Dữ liệu/Feed: Finnhub WebSocket (realtime)
- Định dạng: Avro schema (schemas/trades.avsc)
- Streaming/Messaging: Apache Kafka (+ Zookeeper, Kafdrop)
- Xử lý realtime: Apache Spark Structured Streaming (Scala), Spark Operator trên Kubernetes
- Lưu trữ: Apache Cassandra
- Quan sát/BI: Grafana (dashboard realtime)
- Nền tảng/triển khai: Docker, Kubernetes (Minikube), Helm, Terraform

### Luồng thực thi
1. Ingest (Producer): 
   - Ứng dụng Python “FinnhubProducer” kết nối Finnhub WebSocket, mã hoá Avro
2. Broker: 
   - Gửi message vào Kafka (pod kafka-service); Kafdrop để quan sát topic
3. Process (Spark): 
   - Ứng dụng StreamProcessor (Scala) chạy trên Spark (k8s operator), đọc Kafka, transform/aggregate (trigger 5s), ghi ra Cassandra
4. Store: 
   - Cassandra lưu dữ liệu (tạo keyspace/tables qua script khởi tạo)
5. Visualize: 
   - Grafana kết nối Cassandra, dashboard cập nhật ~500ms
6. Hạ tầng: 
   - Terraform provisioning tài nguyên k8s; Docker hoá toàn bộ ứng dụng

---

## 3) Realtime-Data-Pipeline-for-Stock-Market-Analysis

- Repo: https://github.com/Jimmymugendi/Realtime-Data-Pipeline-for-Stock-Market-Analysis
- Mục tiêu: Ví dụ pipeline realtime đơn giản: Alpha Vantage → Kafka (Confluent Cloud) → Consumer → PostgreSQL (Aiven) để phân tích nhanh.

### Công nghệ chính
- Dữ liệu/API: Alpha Vantage (REST)
- Streaming/Messaging: Apache Kafka (Confluent Cloud)
- Lưu trữ: PostgreSQL (Aiven)
- Ứng dụng: Python producer/consumer

### Luồng thực thi
1. Ingest (Producer): 
   - Python gọi Alpha Vantage (REST) lấy giá cổ phiếu realtime
2. Produce: 
   - Gửi events vào Kafka topic (Confluent Cloud)
3. Consume/Load: 
   - Python consumer đọc Kafka và ghi vào PostgreSQL
4. Phân tích/Trình bày: 
   - Phân tích bằng SQL/BI (định hướng tích hợp Tableau/Power BI trong “Future Work”)

---

## 4) financial-market-data-analysis

- Repo: https://github.com/radoslawkrolikowski/financial-market-data-analysis
- Mục tiêu: Ứng dụng xử lý & dự báo realtime: thu thập đa nguồn (IEX Cloud, Alpha Vantage, các Scrapy spider cho VIX/Economic Indicators/COT), stream vào Kafka, Spark Structured Streaming xử lý, lưu MariaDB/MySQL, suy luận mô hình PyTorch (biGRU) realtime.

### Công nghệ chính
- Dữ liệu/Feed: 
  - IEX Cloud API (REST), Alpha Vantage (REST)
  - Scrapy spiders: VIX (CNBC), Economic Indicators (Investing.com), COT reports (tradingster)
- Streaming/Messaging: Apache Kafka (+ Zookeeper)
- Xử lý realtime: Apache Spark Structured Streaming (PySpark)
- Lưu trữ: MariaDB/MySQL (feature engineering bổ sung qua SQL views)
- ML/Inference: PyTorch biGRU (real-time prediction), dataloader tuỳ biến đọc từ DB
- Môi trường: Hướng dẫn cài đặt thủ công Spark/Kafka/Zookeeper; requirements Python

### Luồng thực thi
1. Ingest (Producer): 
   - `producer.py` gọi IEX/Alpha Vantage và chạy spiders để lấy các chỉ báo kinh tế, VIX, COT theo chu kỳ cấu hình
2. Produce: 
   - Đẩy các stream vào các Kafka topic tương ứng
3. Process (Spark): 
   - `spark_consumer.py` (PySpark Structured Streaming) subscribe Kafka, chuẩn hoá/feature extraction (micro-price, delta, spread, …), join streams, ghi ra DB
4. Store & Feature Engineering bổ sung: 
   - `create_database.py` tạo schema MySQL/MariaDB, view tính MA/Bollinger/Stochastic/ATR/targets…
5. Inference realtime: 
   - `predict.py` đọc datapoint mới nhất theo timestamp từ DB, chuẩn hoá và chạy mô hình biGRU để dự báo

---

## Gợi ý tham khảo nhanh (theo tiêu chí công nghệ)
- WebSocket realtime: finnhub-streaming-data-pipeline (Finnhub WebSocket)
- Orchestration Airflow: Finance-Data-Ingestion-Pipeline-with-Kafka
- Spark Streaming: Finance-Data-Ingestion-Pipeline-with-Kafka, finnhub-streaming-data-pipeline, financial-market-data-analysis
- Flink: (không nằm trong 4 repo này; xem thêm các repo Flink realtime khác nếu cần)
- Iceberg: (không nằm trong 4 repo này; xem thêm các repo dùng Iceberg nếu cần)

Nếu muốn, có thể mở rộng danh mục “references” với các ví dụ bổ sung về Flink và Iceberg để hoàn thiện bức tranh công nghệ lakehouse/stream processing hiện đại.
