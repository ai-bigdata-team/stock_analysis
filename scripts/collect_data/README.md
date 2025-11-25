# scripts/collect_data — Producers runbook

Ngắn gọn về các script thu thập dữ liệu trong `scripts/collect_data` và cách chạy chúng để gửi message lên Kafka.

Files
- `StockProducer.py` — Polling historical/OHLCV data từ `vnstock` (không realtime chính xác), chuyển bản ghi mới nhất sang JSON và gửi lên Kafka topic `vnstock_stock`.
- `RealtimeStockProducer.py` — Kết nối WebSocket tới Finnhub để nhận trade realtime, loại trùng lặp nhẹ và gửi từng trade lên Kafka topic `finnhub_stock`.

Kafka topics
- `vnstock_stock` — OHLCV records từ VN (StockProducer).
- `finnhub_stock` — Realtime trade messages từ Finnhub (RealtimeStockProducer).

Environment variables / config
- `KAFKA_NODES` — Kafka bootstrap servers (ví dụ `localhost:9092`).
- `FINNHUB_API_KEY` — API key dành cho Finnhub (dùng bởi `RealtimeStockProducer.py`).
- `.env` support: scripts dùng `python-dotenv` (`load_dotenv()`), bạn có thể đặt env vars trong file `.env` ở root.

Dependencies
- `StockProducer.py`: `vnstock`, `kafka-python`, `pandas`, `python-dotenv`.
- `RealtimeStockProducer.py`: `websocket-client` (or `websocket`), `kafka-python`, `python-dotenv`.

Run examples (PowerShell)

- Quick run VNStock producer (polling):
```powershell
$env:KAFKA_NODES = "localhost:9092"
python .\scripts\collect_data\StockProducer.py
```

- Quick run Finnhub realtime producer:
```powershell
$env:KAFKA_NODES = "localhost:9092"
$env:FINNHUB_API_KEY = "your_finnhub_key"
python .\scripts\collect_data\RealtimeStockProducer.py
```

Notes about behaviour
- `StockProducer` polls vnstock for recent historical records (daily freq). Mỗi lần poll nó lấy record mới nhất cho từng mã trong `scripts/constant/stock_code_constant.py` và gọi `producer.send(topic, value)` để publish dict JSON. Có `time.sleep(0.5)` giữa các send và `polling_interval` (mặc định 60s) sau mỗi vòng.
- `RealtimeStockProducer` kết nối websocket tới Finnhub; callback `on_message` parse JSON, lặp qua `data` array và `producer.send(topic, value)` cho từng trade. Có cơ chế `seen_records` để tránh gửi duplicate nhỏ.
- Producer ở cả 2 file khởi tạo `KafkaProducer` với `value_serializer=lambda m: json.dumps(m).encode("utf-8")` => messages là JSON bytes.

Safety & reliability notes
- `producer.send` là bất đồng bộ (kafka-python). Nếu bạn muốn đảm bảo gửi thành công trước khi exit, gọi `producer.flush()` hoặc `producer.close()` hợp lý.
- `StockProducer.get_OHLCV_data_realtime` hiện chuyển `records_latest.to_json(...)` thành dict bằng `json.loads(str(record_data)[1:-1])` — cách này hơi mong manh nếu format thay đổi. Có thể cân nhắc sửa để parse an toàn hơn.

Debugging tips
- Dùng `kafka-console-consumer` hoặc tương đương để xem raw messages trong topic trước khi chạy Spark jobs.
- Kiểm tra log output (Python logging) của mỗi script để xem lỗi kết nối hoặc parse.

Muốn tôi làm gì tiếp? (không thay logic)
- Thêm helper PowerShell trong `scripts/bin/` (ví dụ `run_stock_producer.ps1`, `run_realtime_producer.ps1`).
- Hoặc kiểm tra/cập nhật `requirements.txt` để thêm `vnstock`, `websocket-client`, `kafka-python` nếu cần.

(End of `scripts/collect_data/README.md`)
