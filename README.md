# Stock analysis
Bigdata project for a system designed to collect, store, analyze, and process stock market data

![System Architecture Pipeline](./images/PipeLine.png)

## Project Structure
```
stock_analysis/
├── .env                          # Environment variables (API keys, Kafka configs)
├── requirements.txt              # Python dependencies
├── README.md                     # Project documentation
│
├── images/                       # Documentation images
│   └── PipeLine.png             # System architecture diagram
│
├── scripts/                      # Main application scripts
│   ├── collect_data/            # Data collection modules
│   │   ├── StockProducer.py     # Vnstock producer (polling)
│   │   └── RealtimeStockProducer.py  # Finnhub producer (WebSocket)
│   │
│   ├── constant/                # Configuration constants
│   │   └── stock_code_constant.py   # List of stock symbols
│   │
│   └── streaming/               # Spark streaming jobs (WIP)
│
├── tests/                       # Test scripts
│   ├── test_vnstock_producer.py # Vnstock API tests
│   └── yfinance_test.py         # Yahoo Finance API tests
│
├── references/                  # Reference materials and examples
└── stock_api/                   # Additional API utilities
```

## System

### Components
- **Data Sources**: 
  - Vnstock API (Vietnam stock market - VCI/TCBS/MSN,...)
  - Finnhub API (US stock market - WebSocket real-time)
  
- **Message Broker**: Apache Kafka 3.3.2
  - Topics: `vnstock_stock`, `finnhub_stock`
  
- **Data Processing**: Apache Spark (PySpark 3.3.2) - In development

- **Storage**: TBD (Cassandra/PostgreSQL/HDFS)

### Stock Codes
Currently tracking Vietnamese stocks: `AAA`, `AAM`, `AAT`, `ABS`, `HPG`, `VIC`

## Installation
Create virtual environment  (WSL)
```
python -m venv .venv 
source .venv/bin/activate
``` 
Download libraries 
```
pip install requirements.txt
```
Download Kafka (3.3.2, to be compatible with PySpark in the previous requirements.txt)
```
wget https://archive.apache.org/dist/kafka/3.3.2/kafka_2.13-3.3.2.tgz
tar -xzf kafka_2.13-3.3.2.tgz
``` 
## Collect data
We offer two methods for collecting data:
- Realtime streaming, using Finnhub API and WebSocket
- Polling, using vnstock 

Initialize Zookeeper and then Kafka broker 
```
cd ~/kafka_2.13-3.3.2
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
```

Get Kafka topics 
```
cd ~/kafka_2.13-3.3.2 
bin/kafka-topics.sh --list --bootstrap-server localhost:9092 
```
Get data stored in Kafka topics
```
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic <Kafka_topic_name> \
  --from-beginning
```
## Spark streaming 
Get data from Kafka topics, analyze and save into cassandra
```
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.2,com.datastax.spark:spark-cassandra-connector_2.13:3.3.2 \
  --conf spark.cassandra.connection.host=localhost \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  scripts/streaming/vnstock_kafka_to_cassandra.py
```