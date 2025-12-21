# GCS to BigQuery Data Pipeline Guide

## 📋 Tổng quan

Sau khi đã stream dữ liệu từ Kafka vào GCS, bạn có 2 cách để đưa dữ liệu vào BigQuery:

### 🔄 Phương án 1: Batch Loading (Khuyến nghị cho historical data)
Đọc dữ liệu từ GCS (Parquet) và load vào BigQuery theo batch.

### ⚡ Phương án 2: Direct Streaming (Khuyến nghị cho real-time)
Stream trực tiếp từ Kafka vào BigQuery mà không qua GCS.

---

## 🚀 Bước 1: Tạo BigQuery Dataset và Tables

```bash
# Chạy SQL script để tạo schema
bq query --use_legacy_sql=false < scripts/streaming/bigquery_schema.sql

# Hoặc tạo manual qua Console
```

**Lưu ý:** Đảm bảo BigQuery dataset đã được tạo trước khi chạy pipeline.

---

## 📦 Phương án 1: Batch Loading từ GCS

### Khi nào dùng?
- Load historical data đã có sẵn trong GCS
- Không cần real-time, chạy theo schedule (hourly, daily)
- Muốn kiểm soát tốc độ load để tiết kiệm cost

### Cách chạy:

```bash
# Load tất cả dữ liệu (raw + aggregates)
python scripts/streaming/gcs_to_bigquery_batch.py \
    --project-id your-gcp-project-id \
    --dataset-id vnstock_data \
    --bucket-name stock_data_hehehe

# Chỉ load raw data
python scripts/streaming/gcs_to_bigquery_batch.py \
    --project-id your-gcp-project-id \
    --load-type raw

# Chỉ load aggregates
python scripts/streaming/gcs_to_bigquery_batch.py \
    --project-id your-gcp-project-id \
    --load-type aggregates
```

### Ưu điểm:
✅ Dễ debug và retry  
✅ Có thể schedule với Airflow/Cron  
✅ Kiểm soát được cost tốt hơn  
✅ Có thể dedup/validate trước khi load  

### Nhược điểm:
❌ Không real-time  
❌ Latency cao (phụ thuộc schedule)  

---

## ⚡ Phương án 2: Direct Streaming từ Kafka

### Khi nào dùng?
- Cần data real-time trong BigQuery
- Dashboard/Analytics cần refresh liên tục
- Không quan tâm lưu trữ lâu dài trên GCS

### Cách chạy:

```bash
python scripts/streaming/vnstock_kafka_to_bigquery.py \
    --project-id your-gcp-project-id \
    --dataset-id vnstock_data \
    --bucket-name stock_data_hehehe \
    --kafka-servers localhost:9092 \
    --kafka-topic vnstock_stock \
    --window-duration "1 minutes"
```

### Ưu điểm:
✅ Real-time, latency thấp (~30 seconds)  
✅ Không cần GCS storage cost  
✅ Simple pipeline, ít components  

### Nhược điểm:
❌ Khó debug khi có lỗi  
❌ BigQuery write cost cao hơn  
❌ Phụ thuộc vào Kafka uptime  

---

## 🏗️ Kiến trúc đề xuất (Hybrid)

```
┌─────────┐      ┌───────┐      ┌─────────┐      ┌──────────┐
│  Kafka  │ ───> │ Spark │ ───> │   GCS   │ ───> │ BigQuery │
└─────────┘      └───────┘      └─────────┘      └──────────┘
                     │                                 ▲
                     │                                 │
                     └─────────────────────────────────┘
                           (Optional: Direct stream)
```

### Workflow:
1. **Real-time path**: Kafka → Spark → BigQuery (streaming)
2. **Backup path**: Kafka → Spark → GCS (cho archival)
3. **Batch re-load**: GCS → BigQuery (khi cần reprocess)

---

## 🔧 Troubleshooting

### Lỗi: "Table not found"
```bash
# Tạo tables trước
bq query --use_legacy_sql=false < scripts/streaming/bigquery_schema.sql
```

### Lỗi: "Permission denied"
```bash
# Kiểm tra service account permissions
gcloud projects get-iam-policy your-project-id \
    --flatten="bindings[].members" \
    --filter="bindings.members:serviceAccount:your-sa@project.iam.gserviceaccount.com"

# Cần các roles:
# - BigQuery Data Editor
# - BigQuery Job User
# - Storage Object Admin
```

### Lỗi: "Exceeded rate limits"
```python
# Giảm tốc độ ghi trong code
.trigger(processingTime="60 seconds")  # Tăng từ 30s lên 60s
```

---

## 📊 Verify dữ liệu trong BigQuery

```bash
# Check record counts
bq query --use_legacy_sql=false '
SELECT 
  symbol,
  COUNT(*) as records,
  MIN(trade_timestamp) as first_record,
  MAX(trade_timestamp) as last_record
FROM `vnstock_data.vnstock_raw_ohlcv`
GROUP BY symbol
ORDER BY records DESC
'

# Check latest data
bq query --use_legacy_sql=false '
SELECT *
FROM `vnstock_data.vnstock_raw_ohlcv`
WHERE trade_date = CURRENT_DATE()
ORDER BY trade_timestamp DESC
LIMIT 10
'
```

---

## 💰 Cost Optimization Tips

1. **Sử dụng Partitioning**: Tables đã partition by date → chỉ query data cần thiết
2. **Clustering**: Cluster by `symbol` → queries theo symbol sẽ rất nhanh
3. **Batch size**: Tăng `processingTime` để giảm số lần write
4. **Streaming inserts**: Có phí cao hơn batch, cân nhắc trade-off
5. **GCS as backup**: Luôn lưu GCS để có thể re-load mà không tốn Kafka retention

---

## 🎯 Khuyến nghị cuối cùng

**Cho production:**
- Chạy **cả 2 pipelines** song song:
  - `vnstock_kafka_to_gcs.py` → Backup to GCS
  - `vnstock_kafka_to_bigquery.py` → Real-time to BigQuery
  
**Cho development/testing:**
- Dùng batch loading từ GCS
- Test queries trước khi enable streaming

**Monitoring:**
- Setup alerts cho BigQuery quota limits
- Monitor Spark streaming lag
- Track BigQuery costs daily

---

## 📚 References

- [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
- [BigQuery Partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables)
- [Streaming Inserts Pricing](https://cloud.google.com/bigquery/pricing#streaming_pricing)
