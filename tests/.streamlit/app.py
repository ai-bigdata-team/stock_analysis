import streamlit as st
import pandas as pd
import altair as alt
import datetime

# Cấu hình trang chế độ rộng (Wide mode)
st.set_page_config(page_title="Pro Stock Dashboard", layout="wide")

# CSS tùy chỉnh để giao diện gọn gàng hơn
st.markdown("""
<style>
    .metric-label {font-size: 14px; color: #888;}
    .metric-value {font-size: 18px; font-weight: bold;}
    div[data-testid="stHorizontalBlock"] {align-items: center;}

    /* Bảng styling */
    [data-testid="stDataFrame"] {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
    }

    [data-testid="stDataFrame"] thead tr th {
        background-color: #f5f5f5;
        font-weight: 600;
        padding: 10px 8px;
        border-bottom: 2px solid #1976d2;
    }

    [data-testid="stDataFrame"] tbody tr:hover {
        background-color: #f9f9f9;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
# 1. LOAD & PROCESS DATA
# ==========================================
@st.cache_data
def load_data():
    df = pd.read_parquet("stocks_ohlcv_sample.parquet")
    df["trade_timestamp"] = pd.to_datetime(df["trade_timestamp"])
    return df.sort_values(["stock_code", "trade_timestamp"])


@st.cache_data
def load_realtime_trades():
    """Load dữ liệu giao dịch realtime"""
    df = pd.read_parquet("stocks_realtime_trades_sample.parquet")
    df["trade_timestamp"] = pd.to_datetime(df["trade_timestamp"])
    return df.sort_values(["stock_code", "trade_timestamp"])


df = load_data()
df_realtime = load_realtime_trades()

# ==========================================
# 2. HEADER & CONTROLS
# ==========================================
st.title("📈 Stock Market Analysis")

# Hàng công cụ trên cùng: Chọn mã, Time range, Loại biểu đồ
col1, col2, col3, col4 = st.columns([1.5, 2, 1.5, 1.5])

with col1:
    stock_list = df["stock_code"].unique()
    selected_stock = st.selectbox("Mã cổ phiếu:", stock_list, index=0)

# Lọc dữ liệu theo mã đã chọn
df_stock = df[df["stock_code"] == selected_stock].copy()

# Tính toán chỉ báo kỹ thuật (Moving Averages) ngay trên data
df_stock['MA10'] = df_stock['close'].rolling(window=10).mean()
df_stock['MA50'] = df_stock['close'].rolling(window=50).mean()

with col2:
    # Time Range Selector
    time_options = ["1 Tuần", "1 Tháng", "3 Tháng", "6 Tháng", "1 Năm", "Tất cả"]
    selected_time = st.radio("Khung thời gian:", time_options, index=2, horizontal=True)

with col3:
    chart_type = st.selectbox("Loại biểu đồ:", ["Candlestick (Nến)", "Line (Đường)"])

with col4:
    # Multiselect để bật tắt đường MA
    overlays = st.multiselect("Chỉ báo chồng lớp:", ["MA10", "MA50"], default=["MA10"])

# ==========================================
# 3. DATA FILTERING (TIME LOGIC)
# ==========================================
max_date = df_stock["trade_timestamp"].max()
start_date = df_stock["trade_timestamp"].min()

if selected_time == "1 Tuần":
    start_date = max_date - datetime.timedelta(weeks=1)
elif selected_time == "1 Tháng":
    start_date = max_date - datetime.timedelta(days=30)
elif selected_time == "3 Tháng":
    start_date = max_date - datetime.timedelta(days=90)
elif selected_time == "6 Tháng":
    start_date = max_date - datetime.timedelta(days=180)
elif selected_time == "1 Năm":
    start_date = max_date - datetime.timedelta(days=365)

# Lọc DataFrame hiển thị
df_view = df_stock[df_stock["trade_timestamp"] >= start_date]

# ==========================================
# 4. MAIN LAYOUT (CHART LEFT - STATS RIGHT)
# ==========================================
main_col, info_col = st.columns([3, 1])  # Tỷ lệ 3:1

# --- LEFT COLUMN: CHART ---
with main_col:
    st.subheader(f"Diễn biến giá {selected_stock}")

    # Tính độ rộng nến động dựa trên số lượng điểm dữ liệu
    num_points = len(df_view)

    # Công thức: Càng nhiều điểm -> nến càng nhỏ
    if num_points <= 7:  # 1 tuần
        candle_width = 100
    elif num_points <= 30:  # 1 tháng
        candle_width = 50
    elif num_points <= 90:  # 3 tháng
        candle_width = 12
    elif num_points <= 180:  # 6 tháng
        candle_width = 6
    else:  # 1 năm hoặc tất cả
        candle_width = 5

    # 1. Base Chart
    base = alt.Chart(df_view).encode(
        x=alt.X("trade_timestamp:T", axis=alt.Axis(title="Thời gian", format="%d/%m"))
    )

    # 2. Main Price Chart Layer
    if chart_type == "Candlestick (Nến)":
        # Rule (High-Low) - thanh mảnh
        rule = base.mark_rule(size=1).encode(
            y=alt.Y("low:Q", scale=alt.Scale(zero=False), axis=alt.Axis(title="Giá")),
            y2="high:Q",
            color=alt.condition("datum.open <= datum.close", alt.value("#00C853"), alt.value("#FF3D00"))
        )
        # Bar (Open-Close) - thân nến với độ rộng động
        bar = base.mark_bar(size=candle_width).encode(
            y="open:Q",
            y2="close:Q",
            color=alt.condition("datum.open <= datum.close", alt.value("#00C853"), alt.value("#FF3D00")),
            tooltip=["trade_timestamp", "open", "high", "low", "close", "volume"]
        )
        price_chart = rule + bar
    else:
        # Line Chart
        price_chart = base.mark_line(size=2).encode(
            y=alt.Y("close:Q", scale=alt.Scale(zero=False)),
            color=alt.value("#2962FF"),
            tooltip=["trade_timestamp", "close"]
        )

    # 3. Moving Averages Layer
    ma_layers = []
    if "MA10" in overlays:
        ma10 = base.mark_line(color='#FFD600', size=2).encode(
            y='MA10:Q', tooltip=[alt.Tooltip('MA10', format=',.2f')]
        )
        ma_layers.append(ma10)

    if "MA50" in overlays:
        ma50 = base.mark_line(color='#D500F9', size=2).encode(
            y='MA50:Q', tooltip=[alt.Tooltip('MA50', format=',.2f')]
        )
        ma_layers.append(ma50)

    # Combine Price + MA
    final_price_chart = price_chart
    for ma in ma_layers:
        final_price_chart += ma

    # 4. Volume Chart (Bar chart bên dưới) - độ rộng cũng điều chỉnh
    vol_chart = base.mark_bar(opacity=0.6, size=candle_width).encode(
        y=alt.Y("volume:Q", axis=alt.Axis(title="Khối lượng", labels=False, ticks=False)),
        color=alt.condition("datum.open <= datum.close", alt.value("#00C853"), alt.value("#FF3D00"))
    ).properties(height=100)

    # Ghép biểu đồ giá (trên) và volume (dưới)
    combined_chart = alt.vconcat(
        final_price_chart.properties(height=400, width="container"),
        vol_chart.properties(height=100, width="container")
    ).resolve_scale(x='shared')

    st.altair_chart(combined_chart, use_container_width=True)

# --- RIGHT COLUMN: SNAPSHOT STATS ---
with info_col:
    tab = st.radio("Chế độ hiển thị:", ["Tổng hợp", "Sổ lệnh"], horizontal=True)

    if tab == "Sổ lệnh":
        # LỌC DỮ LIỆU REALTIME CHO MÃ ĐÃ CHỌN
        df_trades_stock = df_realtime[df_realtime["stock_code"] == selected_stock].copy()

        # Sắp xếp theo thời gian giảm dần (mới nhất lên đầu)
        df_trades_stock = df_trades_stock.sort_values("trade_timestamp", ascending=False)

        # Lấy 100 lệnh gần nhất
        df_trades_display = df_trades_stock.head(100)

        st.markdown("#### Khớp lệnh thời gian thực")

        # Tạo DataFrame hiển thị với format đẹp
        display_df = df_trades_display[["trade_timestamp", "price", "volume"]].copy()

        # Format thời gian
        display_df["Thời gian"] = display_df["trade_timestamp"].dt.strftime("%H:%M:%S")

        # Format giá với màu sắc
        display_df["Giá"] = display_df["price"].apply(lambda x: f"{x:,.2f}")

        # Format khối lượng
        display_df["Khối lượng"] = display_df["volume"].apply(lambda x: f"{x:,}")

        # Chỉ lấy các cột đã format
        display_df = display_df[["Thời gian", "Giá", "Khối lượng"]]

        # Hiển thị bảng
        st.dataframe(
            display_df,
            use_container_width=True,
            height=450,
            hide_index=True
        )

        # Thống kê tổng quan
        st.markdown("---")
        st.markdown("#### 📊 Thống kê giao dịch")

        total_trades = len(df_trades_display)
        total_volume = df_trades_display["volume"].sum()
        avg_price = df_trades_display["price"].mean()
        max_price = df_trades_display["price"].max()
        min_price = df_trades_display["price"].min()

        col_stat1, col_stat2, col_stat3 = st.columns(3)

        with col_stat1:
            st.metric("📝 Số lệnh", f"{total_trades:,}")
            st.metric("💰 Giá TB", f"{avg_price:,.2f}")

        with col_stat2:
            st.metric("📦 Tổng KL", f"{total_volume:,}")
            st.metric("🔺 Cao nhất", f"{max_price:,.2f}")

        with col_stat3:
            price_range = max_price - min_price
            st.metric("📊 Biên độ", f"{price_range:,.2f}")
            st.metric("🔻 Thấp nhất", f"{min_price:,.2f}")

        st.stop()

    # Lấy dữ liệu ngày cuối cùng (Last Row)
    last_row = df_stock.iloc[-1]
    prev_row = df_stock.iloc[-2] if len(df_stock) > 1 else last_row

    change = last_row['close'] - prev_row['close']
    pct_change = (change / prev_row['close']) * 100

    color_metric = "normal"
    if change > 0: color_metric = "off"

    st.metric(
        label="Giá hiện tại",
        value=f"{last_row['close']:,.0f}",
        delta=f"{change:,.0f} ({pct_change:.2f}%)"
    )

    st.markdown("---")


    # Hàm helper để hiển thị dòng dữ liệu đẹp
    def display_row(label, value, color=None):
        if color:
            val_html = f"<span style='color:{color}; font-weight:bold'>{value}</span>"
        else:
            val_html = f"<b>{value}</b>"

        st.markdown(
            f"<div style='display:flex; justify-content:space-between; margin-bottom:10px; border-bottom:1px solid #333; padding-bottom:4px'>"
            f"<span style='color:#aaa'>{label}</span>"
            f"{val_html}"
            f"</div>",
            unsafe_allow_html=True
        )


    # Hiển thị các thông số giống giao diện bảng bên phải
    display_row("Mở cửa", f"{last_row['open']:,.0f}", "#00E676" if last_row['open'] >= prev_row['close'] else "#FF5252")
    display_row("Cao nhất", f"{last_row['high']:,.0f}", "#00E676")
    display_row("Thấp nhất", f"{last_row['low']:,.0f}", "#FF5252")
    display_row("Khối lượng", f"{last_row['volume']:,.0f}")

    # Kiểm tra nếu có các cột Fundamental
    if 'PE' in last_row:
        display_row("P/E", f"{last_row['PE']:.2f}")
    if 'EPS' in last_row:
        display_row("EPS", f"{last_row['EPS']:,.0f}")
    if 'PB' in last_row:
        display_row("P/B", f"{last_row['PB']:.2f}")
    if 'MarketCap' in last_row:
        display_row("Vốn hóa", f"{last_row['MarketCap'] / 1e9:,.0f} tỷ")

    # Chỉ hiển thị các chỉ số kỹ thuật của ngày cuối
    st.markdown("#### 📊 Chỉ số kỹ thuật (Last Day)")
    if 'RSI' in last_row:
        display_row("RSI", f"{last_row['RSI']:.2f}")

    # Hiển thị MA ngày cuối
    display_row("MA10", f"{last_row['MA10']:.2f}")
    display_row("MA50", f"{last_row['MA50']:.2f}")