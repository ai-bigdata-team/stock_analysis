import yfinance as yf

ticker = yf.Ticker("AAPL")
# Test 1: 5 days, daily data
df = ticker.history(period="5d", interval="1d")
print(f"Test 1 (5d, 1d): {len(df) if not df.empty else 0} records")
if not df.empty: print(df.tail(2))
# Test 2: 1 day, 5min data
df2 = ticker.history(period="1d", interval="5m")
print(f"\nTest 2 (1d, 5m): {len(df2) if not df2.empty else 0} records")
if not df2.empty: print(df2.tail(2))