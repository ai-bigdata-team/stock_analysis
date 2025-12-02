import pandas as pd
import numpy as np

# Sử dụng chung danh sách cổ phiếu
STOCKS = ["AAA", "AAM", "VIC", "VNM", "HPG", "FPT", "VCB"]


def generate_stock_data():
    # ... (giữ nguyên hàm generate_stock_data của bạn)
    # ⚠️ Để hàm generate_realtime_trades hoạt động, bạn cần đảm bảo
    # hàm generate_stock_data đã chạy và lưu file stocks_ohlcv_sample.parquet
    # để lấy được giá close làm điểm neo.
    # Tuy nhiên, trong môi trường này, tôi sẽ mô phỏng việc đó bằng cách tạo
    # một DataFrame close_prices_df giả lập để làm điểm neo cho hàm realtime.
    # ---
    # *Do không thể chạy 2 hàm trong 1 lần gọi, tôi sẽ chỉ tập trung vào hàm realtime
    # và giả lập close price để code chạy độc lập.*
    # (Nếu chạy trên máy, chỉ cần đảm bảo generate_stock_data() chạy trước.)

    # Giả lập giá đóng cửa ngày đầu tiên (dùng làm điểm neo cho hàm realtime)
    np.random.seed(42)
    base_prices = {stock: np.random.uniform(10, 100) for stock in STOCKS}

    # Gọi hàm sinh dữ liệu realtime
    generate_realtime_trades(base_prices)


def generate_realtime_trades(anchor_prices):
    """
    Sinh dữ liệu giao dịch real-time (minute/second resolution)
    cho 6 tháng.
    :param anchor_prices: Dictionary {stock_code: price} làm điểm neo.
    """
    np.random.seed(43)  # Seed khác cho dữ liệu realtime

    # 1. Cấu hình thời gian (6 tháng)
    end_date = pd.Timestamp.now().normalize()
    start_date = end_date - pd.DateOffset(months=6)
    # Lấy các ngày giao dịch (Business Days: T2-T6)
    dates = pd.bdate_range(start=start_date, end=end_date)

    all_trades = []
    trade_id_counter = 1

    print(f"\n🚀 Đang tạo dữ liệu Real-Time cho {len(STOCKS)} mã trong {len(dates)} ngày giao dịch...")

    # Duyệt qua từng cổ phiếu
    for stock in STOCKS:
        # Giá khởi điểm (dùng giá neo ngày đầu tiên)
        current_price = anchor_prices[stock]

        # Duyệt qua từng ngày giao dịch
        for d in dates:
            # 2. Cấu hình phiên giao dịch
            # Bắt đầu 9:15:00, Kết thúc 14:45:00
            start_time = d + pd.Timedelta(hours=9, minutes=15)
            end_time = d + pd.Timedelta(hours=14, minutes=45)

            # --- Biến động giá hàng ngày (Daily Drift) ---
            # Biến động ngẫu nhiên so với ngày trước (-5% đến +5%)
            daily_change = np.random.uniform(-0.05, 0.05)
            # Giá neo phiên (giả định đây là giá Open/Tham chiếu)
            anchor_price = current_price * (1 + daily_change)
            current_price = anchor_price  # Đặt giá khởi điểm cho phiên

            # 3. Số lượng giao dịch trong ngày (Min 20)
            num_trades = np.random.randint(20, 101)

            # Tạo các mốc thời gian ngẫu nhiên trong phiên
            time_deltas = (end_time - start_time) / num_trades
            trade_timestamps = [start_time + time_deltas * i + pd.Timedelta(seconds=np.random.uniform(-30, 30))
                                for i in range(num_trades)]
            trade_timestamps.sort()  # Sắp xếp lại để đảm bảo thứ tự thời gian

            # --- Sinh dữ liệu giao dịch cho phiên ---
            trades_data = []

            for timestamp in trade_timestamps:
                # 4. Biến động giá giữa các giao dịch (Intra-day noise)
                # Biến động nhỏ (-0.5% đến +0.5%) so với giá giao dịch trước đó
                price_noise = np.random.uniform(-0.005, 0.005)
                new_price = current_price * (1 + price_noise)

                # Làm tròn giá (ví dụ: 2 chữ số thập phân)
                new_price = max(0.1, round(new_price, 2))

                # 5. Khối lượng (ngẫu nhiên từ 100 đến 100000)
                vol = int(np.random.uniform(100, 100000) // 100 * 100)  # Lô 100 cổ

                # 6. Conditions (giả lập)
                conditions = np.random.choice([' ', ' ', ' ', 'C', 'X'])  # Dễ ra ' '

                trades_data.append({
                    "stock_code": stock,
                    "trade_timestamp": timestamp,
                    "price": new_price,
                    "volume": vol,
                    "conditions": conditions,
                    "trade_id": trade_id_counter
                })

                # Cập nhật giá hiện tại và Trade ID
                current_price = new_price
                trade_id_counter += 1

            # Cập nhật giá đóng cửa ngày (Close) cho ngày mai
            # Giá đóng cửa là giá giao dịch cuối cùng trong phiên
            if trades_data:
                current_price = trades_data[-1]["price"]

            all_trades.extend(trades_data)

    # 3. Gộp tất cả lại thành DataFrame
    final_df_trades = pd.DataFrame(all_trades)

    # Sắp xếp lại
    final_df_trades = final_df_trades.sort_values(["stock_code", "trade_timestamp"]).reset_index(drop=True)

    print("\nSample dữ liệu Real-Time:")
    print(final_df_trades.head(10))

    # Lưu file
    final_df_trades.to_parquet("stocks_realtime_trades_sample.parquet", index=False)
    print(f"\n✔ Đã lưu file stocks_realtime_trades_sample.parquet với {len(final_df_trades)} dòng dữ liệu.")


if __name__ == "__main__":
    # --- Tách biệt 2 hàm ---
    # *Bạn có thể chạy cả 2 hàm generate_stock_data và generate_realtime_trades
    # trong file của mình. Nếu không có file OHLCV, hàm realtime sẽ dùng giá neo giả lập.*

    # ⚠️ Để chạy đúng, bạn cần chạy lại hàm generate_stock_data() ban đầu
    # để lấy được giá đóng cửa của ngày cuối cùng làm điểm neo.
    # Trong ví dụ này, tôi sẽ gọi generate_realtime_trades với giá neo giả lập
    # để minh họa logic.

    print("--- BẮT ĐẦU SINH DỮ LIỆU REAL-TIME ---")
    np.random.seed(42)
    # Giả lập giá neo cho ngày đầu tiên của dữ liệu real-time
    initial_anchor_prices = {stock: np.random.uniform(10, 100) for stock in STOCKS}

    generate_realtime_trades(initial_anchor_prices)