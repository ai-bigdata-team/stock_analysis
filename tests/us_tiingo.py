import finnhub
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor

finnhub_client = finnhub.Client(api_key="API_KEY")

symbols_data = finnhub_client.stock_symbols('US')
symbols = [item['symbol'] for item in symbols_data]
nasdaq_symbols = [item['symbol'] for item in symbols_data if item['mic'] in ['XNAS','XNGS']]
nyse_symbols = [item['symbol'] for item in symbols_data if item['mic'] == 'XNYS']

API_KEYS = [
    "API_KEY_1", 
    "API_KEY_2", 
    "API_KEY_3", 
    "API_KEY_4",
    "API_KEY_5", 
    "API_KEY_6", 
    "API_KEY_7", 
    "API_KEY_8",
    "API_KEY_9", 
    "API_KEY_10", 
    "API_KEY_11", 
    "API_KEY_12",
    "API_KEY_13", 
    "API_KEY_14", 
    "API_KEY_15", 
    "API_KEY_16",
    "API_KEY_17"
]

def chunk_list(lst, size):
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def write_log(message):
    with open("log_tiingo", "a", encoding="utf-8") as logf:
        logf.write(message + "\n")

def process_chunk(symbols, api_key, exchange_name, chunk_id):
    output_file = f"data/tiingo_{exchange_name}_{chunk_id}.jsonl"
    for sym in symbols:
        try:
            url = f"https://api.tiingo.com/tiingo/daily/{sym}/prices?startDate=1970-01-01&endDate=2025-11-25&token={api_key}"
            r = requests.get(url)
            data = r.json()

            if data:
                with open(output_file, "a", encoding="utf-8") as f:
                    for obj in data:
                        # if isinstance(obj, dict):
                        new_obj = {"symbol": sym}
                        new_obj.update(obj)
                        f.write(json.dumps(new_obj) + "\n")
                        # else:
                        #     write_log(f"[{exchange_name}-{chunk_id}] {sym} có phần tử không phải dict: {obj}")

                write_log(f"[{exchange_name}-{chunk_id}] Done {sym}, rows={len(data)}")
            else:
                write_log(f"[{exchange_name}-{chunk_id}] No data for {sym}")

            # Sleep để không vượt quá limit (ví dụ 75 giây để chắc chắn <50 req/h)
            time.sleep(75)
        except Exception as e:
            write_log(f"[{exchange_name}-{chunk_id}] Error {sym}: {e}")

nasdaq_chunks = chunk_list(nasdaq_symbols, 500)
nyse_chunks = chunk_list(nyse_symbols, 500)

tasks = []
for i, chunk in enumerate(nasdaq_chunks):
    tasks.append((chunk, API_KEYS[i % len(API_KEYS)], "nasdaq", i+1))

for i, chunk in enumerate(nyse_chunks):
    tasks.append((chunk, API_KEYS[(i+len(nasdaq_chunks)) % len(API_KEYS)], "nyse", i+1))

# Chạy song song
with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
    futures = [executor.submit(process_chunk, *task) for task in tasks]

    for future in futures:
        future.result()