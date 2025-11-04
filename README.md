# Stock analysis
Bigdata project for a system designed to collect, store, analyze, and process stock market data

__System Architecture Pipeline:__
![System Architecture Pipeline](./images/PipeLine.png)

__Current Architecture Pipeline:__
![Current Architecture Pipeline](./images/current_pipeline.png)

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

Run python file to collect data by API:
- realtime data by finnhub api:
```
python -m scripts.collect_data.RealtimeStockProducer
```
- batch data by vnstock api (crawl automatically after a time period):
```
python -m scripts.collect_data.StockProducer
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
Get data from Kafka topics, analyze and save into Google Cloud Storage

Download gcs-connector and save to folder `jars`
```
mkdir -p jars && wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.11/gcs-connector-hadoop3-2.2.11-shaded.jar -O jars/gcs-connector-shaded.jar
```

Streaming:
```
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  --jars jars/gcs-connector-shaded.jar \
  scripts/streaming/finnhub_kafka_to_gcs.py \
  --bucket-name stock_data_demo \
  --gcs-key ~/gcs_bigdatastockanalysis.json
```