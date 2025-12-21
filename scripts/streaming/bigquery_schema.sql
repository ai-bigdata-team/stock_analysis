-- ============================================
-- BigQuery Schema Setup for VNStock Data
-- ============================================

-- Tạo Dataset
CREATE SCHEMA IF NOT EXISTS `vnstock_data`
OPTIONS(
  location="asia-southeast1",  -- Đặt ở Singapore cho VN data
  description="Vietnamese stock market data from VNStock"
);

-- ============================================
-- 1. Raw OHLCV Table
-- ============================================
CREATE TABLE IF NOT EXISTS `vnstock_data.vnstock_raw_ohlcv` (
  symbol STRING NOT NULL,
  trade_timestamp TIMESTAMP NOT NULL,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  volume INT64,
  price_range FLOAT64,
  price_change FLOAT64,
  price_change_pct FLOAT64,
  record_id STRING,
  ingest_timestamp TIMESTAMP,
  trade_date DATE,
  year INT64,
  month INT64
)
PARTITION BY trade_date
CLUSTER BY symbol
OPTIONS(
  description="Raw OHLCV stock data - tick by tick",
  require_partition_filter=false
);

-- ============================================
-- 2. Aggregates OHLCV Table
-- ============================================
CREATE TABLE IF NOT EXISTS `vnstock_data.vnstock_aggregates_ohlcv` (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  symbol STRING NOT NULL,
  record_count INT64,
  window_open FLOAT64,
  window_close FLOAT64,
  window_high FLOAT64,
  window_low FLOAT64,
  total_volume INT64,
  avg_close_price FLOAT64,
  avg_volume FLOAT64,
  total_turnover FLOAT64,
  window_price_change_pct FLOAT64,
  window_price_range FLOAT64,
  computed_at TIMESTAMP,
  window_date DATE,
  year INT64,
  month INT64
)
PARTITION BY window_date
CLUSTER BY symbol
OPTIONS(
  description="Aggregated OHLCV data with windowed calculations",
  require_partition_filter=false
);

-- ============================================
-- Useful Queries
-- ============================================

-- Query 1: Check latest data per symbol
SELECT 
  symbol,
  MAX(trade_timestamp) as latest_timestamp,
  COUNT(*) as record_count
FROM `vnstock_data.vnstock_raw_ohlcv`
WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY symbol
ORDER BY latest_timestamp DESC;

-- Query 2: Get aggregated stats for today
SELECT 
  symbol,
  window_start,
  window_close,
  window_high,
  window_low,
  total_volume,
  window_price_change_pct
FROM `vnstock_data.vnstock_aggregates_ohlcv`
WHERE window_date = CURRENT_DATE()
ORDER BY window_start DESC, symbol;

-- Query 3: Top movers by price change %
SELECT 
  symbol,
  window_start,
  window_price_change_pct,
  total_volume
FROM `vnstock_data.vnstock_aggregates_ohlcv`
WHERE window_date = CURRENT_DATE()
ORDER BY ABS(window_price_change_pct) DESC
LIMIT 20;

-- Query 4: Volume leaders
SELECT 
  symbol,
  SUM(total_volume) as total_daily_volume,
  AVG(window_price_change_pct) as avg_change_pct
FROM `vnstock_data.vnstock_aggregates_ohlcv`
WHERE window_date = CURRENT_DATE()
GROUP BY symbol
ORDER BY total_daily_volume DESC
LIMIT 20;
