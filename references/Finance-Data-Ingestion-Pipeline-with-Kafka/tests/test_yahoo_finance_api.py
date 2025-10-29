"""
Test Yahoo Finance API to check data availability
"""
import yfinance as yf
from datetime import datetime

def test_yahoo_finance_basic():
    """Test basic Yahoo Finance connection"""
    print("=" * 60)
    print("TEST 1: Basic Yahoo Finance API Test")
    print("=" * 60)
    
    ticker = yf.Ticker("AAPL")
    print(f"✓ Successfully created Ticker object for AAPL")
    print(f"✓ Company name: {ticker.info.get('longName', 'N/A')}")
    print()


def test_realtime_data():
    """Test realtime data (period=1d, interval=1d)"""
    print("=" * 60)
    print("TEST 2: Realtime Data (period=1d, interval=1d)")
    print("=" * 60)
    
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="5d", interval="1d", auto_adjust=False)
    
    print(f"DataFrame shape: {df.shape}")
    print(f"Number of records: {len(df)}")
    print(f"Is empty: {df.empty}")
    
    if not df.empty:
        print(f"\n✓ SUCCESS: Got {len(df)} records")
        print("\nFirst 3 rows:")
        print(df.head(3))
        print("\nLast 3 rows:")
        print(df.tail(3))
    else:
        print("\n✗ FAILED: No data returned (market may be closed)")
    print()


def test_historical_data_5d():
    """Test historical data (period=5d, interval=1d)"""
    print("=" * 60)
    print("TEST 3: Historical Data (period=5d, interval=1d)")
    print("=" * 60)
    
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="5d", interval="1d", auto_adjust=False)
    
    print(f"DataFrame shape: {df.shape}")
    print(f"Number of records: {len(df)}")
    print(f"Is empty: {df.empty}")
    
    if not df.empty:
        print(f"\n✓ SUCCESS: Got {len(df)} records")
        print("\nFirst 3 rows:")
        print(df.head(3))
        print("\nLast 3 rows:")
        print(df.tail(3))
    else:
        print("\n✗ FAILED: No data returned")
    print()


def test_historical_data_5d_1h():
    """Test historical data (period=5d, interval=1h) - More stable"""
    print("=" * 60)
    print("TEST 4: Historical Data (period=5d, interval=1h)")
    print("=" * 60)
    
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="5d", interval="1h", auto_adjust=False)
    
    print(f"DataFrame shape: {df.shape}")
    print(f"Number of records: {len(df)}")
    print(f"Is empty: {df.empty}")
    
    if not df.empty:
        print(f"\n✓ SUCCESS: Got {len(df)} records")
        print("\nFirst 3 rows:")
        print(df.head(3))
        print("\nLast 3 rows:")
        print(df.tail(3))
        
        # Test tail(2) logic like in original code
        print("\n--- Testing tail(2) logic ---")
        records_latest = df.tail(2)
        print(f"tail(2) shape: {records_latest.shape}")
        if records_latest.shape[0] == 2:
            print("✓ Has 2 records (tail logic will work)")
            record = records_latest.head(1)
            print("\nRecord to send to Kafka:")
            print(record)
        else:
            print(f"✗ Only {records_latest.shape[0]} record(s) (tail logic will fail)")
    else:
        print("\n✗ FAILED: No data returned")
    print()


def test_multiple_stocks():
    """Test multiple stock codes"""
    print("=" * 60)
    print("TEST 5: Multiple Stock Codes (period=5d, interval=1h)")
    print("=" * 60)
    
    stock_codes = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    
    for stock_code in stock_codes:
        ticker = yf.Ticker(stock_code)
        df = ticker.history(period="5d", interval="1d", auto_adjust=False)
        
        if not df.empty:
            print(f"✓ {stock_code}: {len(df)} records")
        else:
            print(f"✗ {stock_code}: No data")
    print()


def test_market_status():
    """Check if market is open"""
    print("=" * 60)
    print("TEST 6: Market Status Check")
    print("=" * 60)
    
    now = datetime.now()
    print(f"Current time: {now}")
    print(f"Current hour: {now.hour}")
    
    # NYSE opens at 9:30 EST (14:30 UTC) and closes at 16:00 EST (21:00 UTC)
    # Adjust for your timezone
    print("\nNote: US market hours are 9:30-16:00 EST (14:30-21:00 UTC)")
    print("For Vietnam time (UTC+7): approximately 20:30-03:00 next day")
    print()


if __name__ == "__main__":
    print("\n")
    print("*" * 60)
    print("YAHOO FINANCE API TESTING")
    print("*" * 60)
    print()
    
    try:
        test_yahoo_finance_basic()
        test_realtime_data()
        test_historical_data_5d()
        test_historical_data_5d_1h()
        test_multiple_stocks()
        test_market_status()
        
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print("If TEST 2 failed but TEST 3/4 succeeded:")
        print("  → Market is CLOSED, use period='5d' instead of '1d'")
        print("\nIf all tests failed:")
        print("  → API rate limiting or network issue")
        print("  → Wait a few minutes and try again")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
