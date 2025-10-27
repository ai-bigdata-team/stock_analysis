# Start Project 
## Installation
Create virtual environment  (WSL)
```
python -m venv .venv 
source .venv/bin/activate
``` 
Download libraries 
```
pip install /home/pavt1024/bigdata/stock_analysis/references/Finance-Data-Ingestion-Pipeline-with-Kafka/requirements.txt
```
Download Kafka (3.3.2, to be compatible with PySpark in the previous requirements.txt)
```
wget https://archive.apache.org/dist/kafka/3.3.2/kafka_2.13-3.3.2.tgz
tar -xzf kafka_2.13-3.3.2.tgz
``` 
Initialize server 
```
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
```