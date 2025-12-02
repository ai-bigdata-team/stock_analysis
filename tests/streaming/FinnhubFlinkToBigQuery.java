package streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FinnhubFlinkToBigQuery {
    public static void main(String[] args) {
        // 1. Create environments
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Create Kafka source table
        tableEnv.executeSql(
            "CREATE TABLE finnhub_kafka (" +
            "  `c` ARRAY<STRING>, " +
            "  `p` DOUBLE, " +
            "  `s` STRING, " +
            "  `t` BIGINT, " +
            "  `v` DOUBLE " +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'finnhub_stock'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," + // chỉnh lại nếu cần
            "  'scan.startup.mode' = 'earliest-offset'," +
            "  'format' = 'json'" +
            ")"
        );

        // 3. Create BigQuery sink table
        tableEnv.executeSql(
            "CREATE TABLE finnhub_trades (" +
            "  conditions STRING, " +
            "  price DOUBLE, " +
            "  symbol STRING, " +
            "  timestamp BIGINT, " +
            "  volume DOUBLE " +
            ") WITH (" +
            "  'connector' = 'bigquery'," +
            "  'project' = 'bigdata-project'," + // chỉnh lại nếu cần
            "  'dataset' = 'stock_data'," +
            "  'table' = 'finnhub_trades'," +
            "  'credentials.key.file' = '/home/pavt1024/bigdata/stock_analysis/jars/gcs/gcs-connector-key.json'" +
            ")"
        );

        // 4. Insert data (convert schema)
        tableEnv.executeSql(
            "INSERT INTO finnhub_trades " +
            "SELECT " +
            "  array_to_string(`c`, ','), " +
            "  `p`, " +
            "  `s`, " +
            "  `t`, " +
            "  `v` " +
            "FROM finnhub_kafka"
        );
    }
}