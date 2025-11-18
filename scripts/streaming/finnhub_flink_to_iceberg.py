import os
import argparse
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.checkpoint_config import CheckpointingMode, ExternalizedCheckpointRetention
from pyflink.table import (
    StreamTableEnvironment, 
    EnvironmentSettings,
    DataTypes,
    TableDescriptor,
    Schema
)
from pyflink.table.expressions import col, lit
from dotenv import load_dotenv

load_dotenv()


class FlinkIcebergPipeline:
    """
    Flink streaming pipeline for Finnhub data
    """
    
    def __init__(self, 
                 kafka_servers="localhost:9092",
                 kafka_topic="finnhub_stock",
                 gcs_warehouse="gs://stock_data_demo/iceberg_warehouse",
                 checkpoint_dir="gs://stock_data_demo/flink_checkpoints"):
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.gcs_warehouse = gcs_warehouse
        self.checkpoint_dir = checkpoint_dir
        self.env = None
        self.table_env = None
        
    def setup_environment(self):
        """Setup Flink execution environment"""
        print("Setting up Flink environment...")
        from pyflink.common import Configuration
        config = Configuration()
        config.set_string("state.checkpoints.dir", self.checkpoint_dir)
        config.set_string("state.backend", "rocksdb")
        config.set_string("execution.checkpointing.interval", "30000")
        config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")

        # Create streaming environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        
        # Enable checkpointing for fault tolerance
        self.env.enable_checkpointing(30000)  # 30 seconds
        checkpoint_config = self.env.get_checkpoint_config()
        checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
        checkpoint_config.set_min_pause_between_checkpoints(10000)
        checkpoint_config.set_checkpoint_timeout(600000)
        checkpoint_config.set_max_concurrent_checkpoints(1)
        checkpoint_config.set_externalized_checkpoint_retention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
        )
        

        
        # Set parallelism
        self.env.set_parallelism(4)
                
        # Create table environment
        settings = EnvironmentSettings.in_streaming_mode()
        self.table_env = StreamTableEnvironment.create(self.env, settings)
        
        print("Flink environment ready")
        
    def create_kafka_source_table(self):
        """
        Create Kafka source table for Finnhub trades
        
        Finnhub message format:
        {
            "c": ["condition1", "condition2"],
            "p": 150.25,
            "s": "AAPL",
            "t": 1699891234567,
            "v": 100.0
        }
        """
        print(f"Creating Kafka source: {self.kafka_topic}")
        
        kafka_ddl = f"""
            CREATE TABLE finnhub_trades_source (
                `c` ARRAY<STRING>,                          -- Trade conditions
                `p` DOUBLE,                                  -- Price
                `s` STRING,                                  -- Symbol
                `t` BIGINT,                                  -- Timestamp (milliseconds)
                `v` DOUBLE,                                  -- Volume
                trade_time AS TO_TIMESTAMP_LTZ(`t`, 3),     -- Convert to timestamp
                WATERMARK FOR trade_time AS trade_time - INTERVAL '10' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.kafka_servers}',
                'properties.group.id' = 'flink-finnhub-consumer',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
        """
        
        self.table_env.execute_sql(kafka_ddl)
        print("Kafka source table created")
        
    def create_iceberg_catalog(self):
        """Create Iceberg catalog for GCS"""
        print(f"Creating Iceberg catalog: {self.gcs_warehouse}")
        
        # Configure Iceberg catalog
        catalog_ddl = f"""
            CREATE CATALOG iceberg_catalog WITH (
                'type' = 'iceberg',
                'catalog-type' = 'hadoop',
                'warehouse' = '{self.gcs_warehouse}',
                'property-version' = '1'
            )
        """
        
        self.table_env.execute_sql(catalog_ddl)
        self.table_env.use_catalog("iceberg_catalog")
        
        # Create database if not exists
        self.table_env.execute_sql("CREATE DATABASE IF NOT EXISTS finnhub")
        self.table_env.use_database("finnhub")
        
        print("Iceberg catalog configured")
        
    def create_iceberg_raw_trades_table(self):
        """Create Iceberg table for raw trades"""
        print("Creating Iceberg raw trades table...")
        
        raw_trades_ddl = """
            CREATE TABLE IF NOT EXISTS raw_trades (
                symbol STRING,
                price DOUBLE,
                volume DOUBLE,
                trade_timestamp TIMESTAMP(3),
                trade_conditions ARRAY<STRING>,
                trade_id STRING,
                ingestion_time TIMESTAMP(3),
                PRIMARY KEY (symbol, trade_timestamp, trade_id) NOT ENFORCED
            ) PARTITIONED BY (symbol)
            WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.metadata.compression-codec' = 'gzip',
                'write.target-file-size-bytes' = '134217728'
            )
        """
        
        self.table_env.execute_sql(raw_trades_ddl)
        print("Raw trades table created")
        
    def create_iceberg_aggregates_table(self):
        """Create Iceberg table for 1-minute OHLCV aggregates"""
        print("Creating Iceberg aggregates table...")
        
        aggregates_ddl = """
            CREATE TABLE IF NOT EXISTS trades_1min_ohlcv (
                symbol STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                total_volume DOUBLE,
                trade_count BIGINT,
                avg_price DOUBLE,
                PRIMARY KEY (symbol, window_start) NOT ENFORCED
            ) PARTITIONED BY (symbol)
            WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        
        self.table_env.execute_sql(aggregates_ddl)
        print("Aggregates table created")
        
    def start_raw_trades_pipeline(self):
        """Pipeline 1: Raw trades to Iceberg"""
        print("\nStarting raw trades pipeline...")
        
        insert_sql = """
            INSERT INTO raw_trades
            SELECT 
                `s` AS symbol,
                `p` AS price,
                `v` AS volume,
                trade_time AS trade_timestamp,
                `c` AS trade_conditions,
                UUID() AS trade_id,
                CURRENT_TIMESTAMP AS ingestion_time
            FROM finnhub_trades_source
        """
        
        # Execute async
        table_result = self.table_env.execute_sql(insert_sql)
        print("Raw trades pipeline started")
        return table_result
        
    def start_aggregates_pipeline(self):
        """Pipeline 2: 1-minute OHLCV aggregates"""
        print("\nStarting aggregates pipeline...")
        
        # Create temporary view for aggregation
        agg_view_sql = """
            CREATE TEMPORARY VIEW trades_windowed AS
            SELECT 
                `s` AS symbol,
                TUMBLE_START(trade_time, INTERVAL '1' MINUTE) AS window_start,
                TUMBLE_END(trade_time, INTERVAL '1' MINUTE) AS window_end,
                `p` AS price,
                `v` AS volume
            FROM finnhub_trades_source
            GROUP BY 
                `s`,
                TUMBLE(trade_time, INTERVAL '1' MINUTE),
                `p`,
                `v`
        """
        self.table_env.execute_sql(agg_view_sql)
        
        # Insert aggregates
        insert_agg_sql = """
            INSERT INTO trades_1min_ohlcv
            SELECT 
                symbol,
                window_start,
                window_end,
                MIN(price) FILTER (WHERE rn = 1) AS open_price,
                MAX(price) AS high_price,
                MIN(price) AS low_price,
                MAX(price) FILTER (WHERE rn = total_count) AS close_price,
                SUM(volume) AS total_volume,
                MAX(total_count) AS trade_count,
                AVG(price) AS avg_price
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY symbol, window_start ORDER BY price) AS rn,
                    COUNT(*) OVER (PARTITION BY symbol, window_start) AS total_count
                FROM trades_windowed
            )
            GROUP BY symbol, window_start, window_end
        """
        
        table_result = self.table_env.execute_sql(insert_agg_sql)
        print("Aggregates pipeline started")
        return table_result
        
    def run(self):
        """Execute the complete pipeline"""
        print("\n" + "="*70)
        print("FLINK STREAMING PIPELINE: Kafka → Iceberg → GCS")
        print("="*70)
        print(f"Kafka: {self.kafka_servers}/{self.kafka_topic}")
        print(f"Iceberg Warehouse: {self.gcs_warehouse}")
        print(f"Checkpoint: {self.checkpoint_dir}")
        print("="*70 + "\n")
        
        # Setup
        self.setup_environment()
        self.create_kafka_source_table()
        self.create_iceberg_catalog()
        self.create_iceberg_raw_trades_table()
        self.create_iceberg_aggregates_table()
        
        # Start pipelines
        raw_result = self.start_raw_trades_pipeline()
        agg_result = self.start_aggregates_pipeline()
        
        print("\n" + "="*70)
        print("ALL PIPELINES STARTED SUCCESSFULLY")
        print("="*70)
        print("\nData Flow:")
        print("  Kafka (finnhub_stock)")
        print("    ↓")
        print("  Flink Processing (<1s latency)")
        print("    ↓")
        print("  Iceberg Tables on GCS:")
        print("    - raw_trades (append)")
        print("    - trades_1min_ohlcv (aggregate)")
        print("\nMonitor:")
        print("  - Flink UI: http://localhost:8081")
        print(f"  - Checkpoints: {self.checkpoint_dir}")
        print(f"  - Data: {self.gcs_warehouse}/finnhub/")
        print("\nPress Ctrl+C to stop\n")
        
        # Wait for completion (or interrupt)
        try:
            # Keep job running
            raw_result.wait()
        except KeyboardInterrupt:
            print("\n\nStopping Flink jobs...")
            print("Gracefully stopped")


def get_args():
    parser = argparse.ArgumentParser(
        description="Flink streaming: Kafka → Iceberg → GCS"
    )
    parser.add_argument(
        "--kafka-servers", 
        default="localhost:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--kafka-topic",
        default="finnhub_stock",
        help="Kafka topic name"
    )
    parser.add_argument(
        "--gcs-warehouse",
        default="gs://stock_data_demo/iceberg_warehouse",
        help="Iceberg warehouse path on GCS"
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="gs://stock_data_demo/flink_checkpoints",
        help="Flink checkpoint directory"
    )
    return parser.parse_args()


def main():
    args = get_args()
    
    pipeline = FlinkIcebergPipeline(
        kafka_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        gcs_warehouse=args.gcs_warehouse,
        checkpoint_dir=args.checkpoint_dir
    )
    
    pipeline.run()


if __name__ == "__main__":
    main()
