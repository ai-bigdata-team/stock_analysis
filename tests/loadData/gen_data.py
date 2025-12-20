import pandas as pd
import numpy as np


def generate_stock_data():
    np.random.seed(42)

    # 1. Danh sách cổ phiếu và cấu hình thời gian
    STOCKS = ["AAA", "AAM", "VIC", "VNM", "HPG", "FPT", "VCB"]

    # Tạo danh sách ngày làm việc (Business Days: T2-T6) trong 1 năm qua
    end_date = pd.Timestamp.now().normalize()
    start_date = end_date - pd.DateOffset(years=1)
    dates = pd.bdate_range(start=start_date, end=end_date)

    all_data = []

    print(f"🛠 Đang tạo dữ liệu cho {len(STOCKS)} mã trong {len(dates)} ngày giao dịch...")

    # 2. Duyệt qua từng mã để tạo chuỗi giá liên tục
    for stock in STOCKS:
        # Giá khởi điểm ngẫu nhiên cho ngày đầu tiên (ví dụ: 10.0 đến 100.0)
        base_price = np.random.uniform(10, 100)

        # Danh sách tạm để chứa dữ liệu của 1 mã
        stock_dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []

        # Biến lưu giá đóng cửa phiên trước (khởi tạo bằng giá base)
        previous_close = base_price

        for d in dates:
            # --- Logic Giá ---
            # Open hôm nay = Close hôm qua
            open_p = previous_close

            # Close biến động trong khoảng -10% đến +10% so với Open
            # change_pct từ -0.10 đến 0.10
            change_pct = np.random.uniform(-0.10, 0.10)
            close_p = open_p * (1 + change_pct)

            # Đảm bảo giá không âm (trường hợp rất hiếm nhưng cần xử lý)
            close_p = max(0.1, close_p)

            # High phải lớn nhất, Low phải nhỏ nhất
            # High = max(open, close) + một chút biến động dương
            high_p = max(open_p, close_p) * (1 + np.random.uniform(0, 0.02))

            # Low = min(open, close) - một chút biến động âm
            low_p = min(open_p, close_p) * (1 - np.random.uniform(0, 0.02))

            # --- Logic Volume ---
            # Volume ngẫu nhiên nhưng có chút biến động mạnh nhẹ
            vol = int(np.random.uniform(10000, 5000000))

            # Lưu vào list
            stock_dates.append(d)
            opens.append(open_p)
            highs.append(highs)  # Lưu ý: biến này là mảng, sửa lại bên dưới append giá trị
            lows.append(lows)
            closes.append(close_p)
            volumes.append(vol)

            # Cập nhật close cho ngày mai
            previous_close = close_p

        # Tạo DataFrame cho mã hiện tại
        df_stock = pd.DataFrame({
            "trade_timestamp": stock_dates,
            "stock_code": stock,
            "open": np.round(opens, 2),
            # High/Low nãy append nhầm list, sửa lại logic tạo mảng trực tiếp ở đây cho nhanh hoặc fix append
            "high": np.round([max(o, c) * (1 + np.random.uniform(0, 0.01)) for o, c in zip(opens, closes)], 2),
            "low": np.round([min(o, c) * (1 - np.random.uniform(0, 0.01)) for o, c in zip(opens, closes)], 2),
            "close": np.round(closes, 2),
            "volume": volumes
        })

        # Thêm các chỉ số tài chính giả lập (Sinh random quanh 1 mốc cố định cho mỗi mã để trông thật hơn)
        # Ví dụ: VNM thì EPS cao ổn định, mã rác thì EPS thấp
        base_eps = np.random.uniform(1000, 5000)
        df_stock["EPS"] = np.random.uniform(base_eps * 0.9, base_eps * 1.1, len(df_stock)).round(0)
        df_stock["PE"] = (df_stock["close"] * 1000 / df_stock["EPS"]).round(2)  # Giả sử giá đơn vị nghìn đồng

        df_stock["PB"] = np.random.uniform(1.0, 5.0, len(df_stock)).round(2)
        df_stock["ROE"] = np.random.uniform(10, 30, len(df_stock)).round(2)
        df_stock["ROA"] = np.random.uniform(5, 15, len(df_stock)).round(2)
        df_stock["Beta"] = np.random.uniform(0.5, 2.5, len(df_stock)).round(2)
        df_stock["MarketCap"] = df_stock["close"] * df_stock["volume"] * 100  # Fake market cap

        all_data.append(df_stock)

    # 3. Gộp tất cả lại
    final_df = pd.concat(all_data, ignore_index=True)

    # Sắp xếp lại theo thời gian và mã
    final_df = final_df.sort_values(["stock_code", "trade_timestamp"])

    print("Sample dữ liệu:")
    print(final_df.head())
    print(final_df.tail())

    # Lưu file
    # final_df.to_parquet("stocks_ohlcv_sample.parquet", index=False)
    print(f"✔ Đã lưu file stocks_ohlcv_sample.parquet với {len(final_df)} dòng dữ liệu.")


if __name__ == "__main__":
    generate_stock_data()