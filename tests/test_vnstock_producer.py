"""
Test vnstock integration with StockProducer
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from vnstock import Vnstock

# Test basic vnstock functionality
def test_vnstock_connection():
    """Test connecting to vnstock API"""
    print("Testing vnstock connection...")
    
    try:
        # Create stock object for HPG (Hoa Phat Group)
        hpg = Vnstock().stock(symbol="HPG", source="VCI")
        print(f"✓ Successfully created stock object for HPG")
        
        # Get historical data
        print("\nFetching historical data for HPG...")
        df = hpg.quote.history(
            symbol="HPG",
            start="2024-10-20",
            end="2024-10-28"
        )
        
        if df is not None and not df.empty:
            print(f"✓ Successfully retrieved {len(df)} records")
            print("\nLatest data:")
            print(df.tail(2))
            
            # Test JSON conversion
            latest = df.tail(1)
            latest["ticker"] = "HPG"
            json_data = latest.to_json(date_format='iso', orient="records")
            print(f"\n✓ JSON conversion successful")
            print(f"JSON data: {json_data}")
        else:
            print("✗ No data retrieved")
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


def test_multiple_stocks():
    """Test with multiple Vietnam stock codes"""
    print("\n" + "="*50)
    print("Testing multiple stock codes...")
    
    stock_codes = ['HPG', 'VIC', 'AAA']
    
    for code in stock_codes:
        try:
            print(f"\nTesting {code}...")
            stock_obj = Vnstock().stock(symbol=code, source="VCI")
            df = stock_obj.quote.history(
                symbol=code,
                start="2024-10-27",
                end="2024-10-28"
            )
            
            if df is not None and not df.empty:
                print(f"  ✓ {code}: {len(df)} records")
                print(f"  Latest close: {df.iloc[-1]['close'] if 'close' in df.columns else 'N/A'}")
            else:
                print(f"  ✗ {code}: No data")
                
        except Exception as e:
            print(f"  ✗ {code}: Error - {e}")


if __name__ == "__main__":
    print("="*50)
    print("VNSTOCK API TEST")
    print("="*50)
    
    test_vnstock_connection()
    test_multiple_stocks()
    
    print("\n" + "="*50)
    print("Test completed!")
    print("="*50)
