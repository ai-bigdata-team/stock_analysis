import streamlit as st
import pandas as pd
import altair as alt
import datetime
import json


# Cấu hình trang
st.set_page_config(page_title="Stock Market Dashboard", layout="wide", initial_sidebar_state="collapsed")

# CSS hiện đại với Dark Theme và màu tím chủ đạo
st.markdown("""
<style>
    /* Import font hiện đại */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Theme tối toàn bộ */
    .stApp {
        background: linear-gradient(135deg, #0a0e27 0%, #1a1d35 100%);
        font-family: 'Inter', sans-serif;
    }

    /* Header styling */
    h1, h2, h3, h4 {
        color: #ffffff !important;
        font-weight: 800 !important;
    }

    /* Main title với gradient */
    .main-title {
        font-size: 32px;
        font-weight: 800;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0px;
    }

    /* Stock code display */
    .stock-header {
        display: flex;
        align-items: center;
        gap: 16px;
        margin: 20px 0;
    }

    .stock-code {
        font-size: 28px;
        font-weight: 800;
        color: #ffffff;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 8px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
    }

    /* Cards với glassmorphism */
    .stat-card {
        background: rgba(26, 29, 53, 0.6);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(102, 126, 234, 0.2);
        border-radius: 16px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }

    /* Metric cards */
    [data-testid="stMetricValue"] {
        font-size: 32px !important;
        font-weight: 800 !important;
        color: #ffffff !important;
    }

    [data-testid="stMetricLabel"] {
        font-size: 13px !important;
        color: #a0aec0 !important;
        font-weight: 650 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    [data-testid="stMetricDelta"] {
        font-size: 16px !important;
        font-weight: 800 !important;
    }

    /* Info row styling */
    .info-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 14px 0;
        border-bottom: 1px solid rgba(102, 126, 234, 0.1);
    }

    .info-label {
        color: #a0aec0;
        font-size: 18px;
        font-weight: 500;
    }

    .info-value {
        color: #ffffff;
        font-size: 22px;
        font-weight: 600;
    }

    .info-value.green { color: #00e676; }
    .info-value.red { color: #ff5252; }
    .info-value.purple { color: #b794f6; }

    /* Buttons & Controls */
    .stSelectbox, .stMultiSelect {
        background: rgba(26, 29, 53, 0.8) !important;
        border-radius: 12px !important;
    }

    .stSelectbox > div > div {
        background: rgba(26, 29, 53, 0.8) !important;
        border: 1px solid rgba(102, 126, 234, 0.3) !important;
        color: #ffffff !important;
        border-radius: 12px !important;
    }

    .stSelectbox label, .stMultiSelect label {
        color: #ffffff !important;
        font-weight: 800 !important;
    }

    /* Radio buttons */
    .stRadio > label {
        color: #ffffff !important;
        font-weight: 800 !important;
    }

    .stRadio > div {
        background: rgba(26, 29, 53, 0.6);
        padding: 8px;
        border-radius: 12px;
        border: 1px solid rgba(102, 126, 234, 0.2);
    }

    .stRadio > div label {
        color: #e2e8f0 !important;
    }

    .stRadio [data-baseweb="radio"] > div {
        color: #ffffff !important;
    }

    /* DataFrame styling */
    [data-testid="stDataFrame"] {
        background: rgba(10, 14, 39, 0.8) !important;
        border: 1px solid rgba(102, 126, 234, 0.2) !important;
        border-radius: 12px !important;
    }

    [data-testid="stDataFrame"] thead tr th {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: #ffffff !important;
        font-weight: 800 !important;
        padding: 12px !important;
        border: none !important;
    }

    [data-testid="stDataFrame"] tbody tr {
        background: rgba(10, 14, 39, 0.6) !important;
        color: #ffffff !important;
        border-bottom: 1px solid rgba(102, 126, 234, 0.1) !important;
    }

    [data-testid="stDataFrame"] tbody tr:hover {
        background: rgba(102, 126, 234, 0.2) !important;
    }

    [data-testid="stDataFrame"] tbody td {
        color: #e2e8f0 !important;
        font-weight: 650 !important;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(26, 29, 53, 0.6);
        padding: 8px;
        border-radius: 12px;
    }

    .stTabs [data-baseweb="tab"] {
        color: #cbd5e0;
        background: transparent;
        border-radius: 8px;
        padding: 8px 16px;
        font-weight: 800;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: #ffffff !important;
    }

    /* Multiselect styling */
    .stMultiSelect label {
        color: #ffffff !important;
        font-weight: 800 !important;
    }

    .stMultiSelect > div > div {
        background: rgba(26, 29, 53, 0.8) !important;
        border: 1px solid rgba(102, 126, 234, 0.3) !important;
        border-radius: 12px !important;
    }

    .stMultiSelect span {
        color: #e2e8f0 !important;
    }

    .stMultiSelect [data-baseweb="tag"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: #ffffff !important;
    }

    /* Section headers */
    .section-header {
        font-size: 18px;
        font-weight: 800;
        color: #ffffff;
        margin: 24px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(102, 126, 234, 0.3);
    }

    /* Price badge */
    .price-badge {
        display: inline-block;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: #ffffff;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 14px;
        font-weight: 800;
        margin-left: 12px;
    }

    /* Trade item styling */
    .trade-item {
        background: rgba(10, 14, 39, 0.6);
        border-left: 3px solid rgba(102, 126, 234, 0.5);
        padding: 12px 16px;
        margin: 8px 0;
        border-radius: 8px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        transition: all 0.3s ease;
    }

    .trade-item:hover {
        background: rgba(102, 126, 234, 0.15);
        border-left-color: #667eea;
        transform: translateX(4px);
    }

    .trade-time {
        color: #a0aec0;
        font-size: 13px;
        font-weight: 650;
    }

    .trade-price {
        color: #ffffff;
        font-size: 16px;
        font-weight: 800;
    }

    .trade-price.up { color: #00e676; }
    .trade-price.down { color: #ff5252; }

    .trade-volume {
        color: #b794f6;
        font-size: 14px;
        font-weight: 800;
    }

    .trade-container {
        max-height: 450px;
        overflow-y: auto;
        padding-right: 8px;
    }

    .trade-container::-webkit-scrollbar {
        width: 6px;
    }

    .trade-container::-webkit-scrollbar-track {
        background: rgba(10, 14, 39, 0.4);
        border-radius: 10px;
    }

    .trade-container::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
    }

    .trade-container::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }

    /* Slider styling */
    .stSlider {
        padding: 10px 0;
    }

    .stSlider > label {
        color: #e2e8f0 !important;
        font-weight: 600 !important;
        font-size: 14px !important;
    }

    .stSlider [data-baseweb="slider"] {
        background: rgba(26, 29, 53, 0.6);
        padding: 10px;
        border-radius: 12px;
    }

    .stSlider [data-baseweb="slider"] > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
    }

    .stSlider [role="slider"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        box-shadow: 0 0 15px rgba(102, 126, 234, 0.5) !important;
    }

    /* Divider */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.3), transparent);
        margin: 24px 0;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
# LOAD DATA
# ==========================================
@st.cache_data
def load_data():
    """
    Đọc dữ liệu từ file tiingo_nasdaq_1.jsonl và chuyển đổi
    sang định dạng tương tự stocks_ohlcv_sample.parquet
    """
    # Đọc file JSONL (mỗi dòng là 1 JSON object)
    data = []
    with open("../.streamlit/tiingo_nasdaq_1.jsonl", "r") as f:
        for line in f:
            data.append(json.loads(line))

    # Chuyển sang DataFrame
    df = pd.DataFrame(data)

    # Chuyển đổi tên cột để khớp với format cũ
    df = df.rename(columns={
        "symbol": "stock_code",
        "date": "trade_timestamp"
    })

    # Chuyển đổi timestamp
    df["trade_timestamp"] = pd.to_datetime(df["trade_timestamp"])

    # Sắp xếp theo mã cổ phiếu và thời gian
    df = df.sort_values(["stock_code", "trade_timestamp"])

    # Tính toán các chỉ số tài chính giả lập cho mỗi mã
    if "EPS" not in df.columns:
        import numpy as np

        # Tạo các chỉ số cho từng mã cổ phiếu
        all_stocks = []
        for stock_code in df["stock_code"].unique():
            df_stock = df[df["stock_code"] == stock_code].copy()

            # Gen EPS với base ngẫu nhiên cho mỗi mã
            base_eps = np.random.uniform(1000, 5000)
            df_stock["EPS"] = np.random.uniform(base_eps * 0.9, base_eps * 1.1, len(df_stock)).round(0)
            df_stock["PE"] = (df_stock["close"] * 1000 / df_stock["EPS"]).round(2)

            df_stock["PB"] = np.random.uniform(1.0, 5.0, len(df_stock)).round(2)
            df_stock["ROE"] = np.random.uniform(10, 30, len(df_stock)).round(2)
            df_stock["ROA"] = np.random.uniform(5, 15, len(df_stock)).round(2)
            df_stock["Beta"] = np.random.uniform(0.5, 2.5, len(df_stock)).round(2)
            df_stock["MarketCap"] = df_stock["close"] * df_stock["volume"] * 100

            all_stocks.append(df_stock)

        df = pd.concat(all_stocks, ignore_index=True)
        df = df.sort_values(["stock_code", "trade_timestamp"])

    # return df
    return df.sort_values(["stock_code", "trade_timestamp"])


@st.cache_data
def load_realtime_trades():
    df = pd.read_parquet("stocks_realtime_trades_sample.parquet")
    df["trade_timestamp"] = pd.to_datetime(df["trade_timestamp"])
    return df.sort_values(["stock_code", "trade_timestamp"])


df = load_data()
df_realtime = load_realtime_trades()

# ==========================================
# HEADER
# ==========================================
st.markdown('<div class="main-title">📈 STOCK MARKET DASHBOARD</div>', unsafe_allow_html=True)

# ==========================================
# CONTROLS - TOP BAR
# ==========================================
st.markdown("<br>", unsafe_allow_html=True)

ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4 = st.columns([2, 3, 2, 2])

with ctrl_col1:
    stock_list = df["stock_code"].unique()
    selected_stock = st.selectbox("🔍 Chọn mã cổ phiếu", stock_list, index=0)

# Lọc dữ liệu
df_stock = df[df["stock_code"] == selected_stock].copy()
df_stock['MA10'] = df_stock['close'].rolling(window=10).mean()
df_stock['MA50'] = df_stock['close'].rolling(window=50).mean()

with ctrl_col2:
    time_options = ["1 Tuần", "1 Tháng", "3 Tháng", "6 Tháng", "1 Năm", "Tất cả"]
    selected_time = st.radio("⏱️ Khung thời gian", time_options, index=2, horizontal=True)

with ctrl_col3:
    chart_type = st.selectbox("📊 Loại biểu đồ", ["Candlestick (Nến)", "Line (Đường)"])

with ctrl_col4:
    overlays = st.multiselect("📈 Chỉ báo", ["MA10", "MA50"], default=["MA10"])

# ==========================================
# TIME FILTERING
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

df_view = df_stock[df_stock["trade_timestamp"] >= start_date]

# ==========================================
# STOCK INFO HEADER
# ==========================================
last_row = df_stock.iloc[-1]
prev_row = df_stock.iloc[-2] if len(df_stock) > 1 else last_row
change = last_row['close'] - prev_row['close']
pct_change = (change / prev_row['close']) * 100

col_header1, col_header2 = st.columns([3, 1])

with col_header1:
    st.markdown(f"""
    <div class="stock-header">
        <div class="stock-code">{selected_stock}</div>
        <div>
            <div style="font-size: 36px; font-weight: 700; color: #ffffff;">{last_row['close']:,.2f}</div>
            <div style="font-size: 18px; font-weight: 600; color: {'#00e676' if change >= 0 else '#ff5252'};">
                {'+' if change >= 0 else ''}{change:,.2f} ({'+' if pct_change >= 0 else ''}{pct_change:.2f}%)
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ==========================================
# MAIN LAYOUT
# ==========================================
main_col, side_col = st.columns([3, 1])

# --- LEFT: CHART ---
with main_col:
    st.markdown('<div class="section-header">📊 Biểu đồ giá</div>', unsafe_allow_html=True)

    # Calculate candle width
    num_points = len(df_view)
    if num_points <= 7:
        candle_width = 150
    elif num_points <= 30:
        candle_width = 35
    elif num_points <= 90:
        candle_width = 12
    elif num_points <= 180:
        candle_width = 5
    else:
        candle_width = 3

    # Base chart
    base = alt.Chart(df_view).encode(
        x=alt.X("trade_timestamp:T",
                axis=alt.Axis(title="", format="%d/%m",
                              labelColor="#a0aec0",
                              gridColor="#2d3748"))
    )

    # Price chart
    if chart_type == "Candlestick (Nến)":
        rule = base.mark_rule(size=1.5).encode(
            y=alt.Y("low:Q",
                    scale=alt.Scale(zero=False),
                    axis=alt.Axis(title="", labelColor="#a0aec0",
                                  gridColor="#2d3748")),
            y2="high:Q",
            color=alt.condition("datum.open <= datum.close",
                                alt.value("#00e676"), alt.value("#ff5252"))
        )
        bar = base.mark_bar(size=candle_width).encode(
            y="open:Q",
            y2="close:Q",
            color=alt.condition("datum.open <= datum.close",
                                alt.value("#00e676"), alt.value("#ff5252")),
            tooltip=[
                alt.Tooltip("trade_timestamp:T", title="Thời gian", format="%d/%m/%Y %H:%M"),
                alt.Tooltip("open:Q", title="Mở cửa", format=",.2f"),
                alt.Tooltip("high:Q", title="Cao nhất", format=",.2f"),
                alt.Tooltip("low:Q", title="Thấp nhất", format=",.2f"),
                alt.Tooltip("close:Q", title="Đóng cửa", format=",.2f"),
                alt.Tooltip("volume:Q", title="Khối lượng", format=",")
            ]
        )
        price_chart = rule + bar
    else:
        price_chart = base.mark_line(size=3).encode(
            y=alt.Y("close:Q",
                    scale=alt.Scale(zero=False),
                    axis=alt.Axis(title="", labelColor="#a0aec0",
                                  gridColor="#2d3748")),
            color=alt.value("#667eea"),
            tooltip=[
                alt.Tooltip("trade_timestamp:T", title="Thời gian", format="%d/%m/%Y"),
                alt.Tooltip("close:Q", title="Giá đóng cửa", format=",.2f")
            ]
        )

    # Moving averages
    ma_layers = []
    if "MA10" in overlays:
        ma10 = base.mark_line(color='#ffd600', size=2.5, strokeDash=[5, 5]).encode(
            y='MA10:Q', tooltip=[alt.Tooltip('MA10', format=',.2f', title='MA10')]
        )
        ma_layers.append(ma10)

    if "MA50" in overlays:
        ma50 = base.mark_line(color='#b794f6', size=2.5, strokeDash=[5, 5]).encode(
            y='MA50:Q', tooltip=[alt.Tooltip('MA50', format=',.2f', title='MA50')]
        )
        ma_layers.append(ma50)

    final_price_chart = price_chart
    for ma in ma_layers:
        final_price_chart += ma

    # Volume chart
    vol_chart = base.mark_bar(opacity=0.7, size=candle_width).encode(
        y=alt.Y("volume:Q", axis=alt.Axis(title="Khối lượng",
                                          labelColor="#a0aec0",
                                          gridColor="#2d3748")),
        color=alt.condition("datum.open <= datum.close",
                            alt.value("#00e676"), alt.value("#ff5252"))
    ).properties(height=120)

    combined_chart = alt.vconcat(
        final_price_chart.properties(height=450, width="container"),
        vol_chart.properties(width="container")
    ).resolve_scale(x='shared').configure_view(
        strokeWidth=0,
        fill='#1a1d35'
    ).configure(
        background='#1a1d35'
    )

    st.altair_chart(combined_chart, use_container_width=True)

# --- RIGHT: INFO PANEL ---
with side_col:
    tab1, tab2 = st.tabs(["📊 Tổng hợp", "📋 Sổ lệnh"])

    with tab1:
        st.markdown('<div class="section-header">Thông tin chi tiết</div>', unsafe_allow_html=True)

        # Price metrics
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.metric("Mở cửa", f"{last_row['open']:,.0f}",
                      delta=f"{last_row['open'] - prev_row['close']:,.0f}")
        with col_m2:
            st.metric("Khối lượng", f"{last_row['volume'] / 1e6:.1f}M")

        st.markdown("<br>", unsafe_allow_html=True)


        # Info rows
        def display_info(label, value, color_class=""):
            st.markdown(f"""
            <div class="info-row">
                <span class="info-label">{label}</span>
                <span class="info-value {color_class}">{value}</span>
            </div>
            """, unsafe_allow_html=True)


        display_info("Cao nhất", f"{last_row['high']:,.0f}", "green")
        display_info("Thấp nhất", f"{last_row['low']:,.0f}", "red")
        display_info("MA10", f"{last_row['MA10']:.2f}", "purple")
        display_info("MA50", f"{last_row['MA50']:.2f}", "purple")

        if 'PE' in last_row:
            display_info("P/E", f"{last_row['PE']:.2f}")
        if 'EPS' in last_row:
            display_info("EPS", f"{last_row['EPS']:,.0f}")
        if 'PB' in last_row:
            display_info("P/B", f"{last_row['PB']:.2f}")
        if 'MarketCap' in last_row:
            display_info("Vốn hóa", f"{last_row['MarketCap'] / 1e9:.1f}B VND")
        if 'RSI' in last_row:
            display_info("RSI", f"{last_row['RSI']:.2f}")

    with tab2:
        st.markdown('<div class="section-header">Khớp lệnh realtime</div>', unsafe_allow_html=True)

        df_trades_stock = df_realtime[df_realtime["stock_code"] == selected_stock].copy()
        df_trades_stock = df_trades_stock.sort_values("trade_timestamp", ascending=False)

        if len(df_trades_stock) > 0:
            # Slider để chọn số lượng lệnh hiển thị
            num_trades = st.slider(
                "Số lệnh hiển thị",
                min_value=5,
                max_value=min(100, len(df_trades_stock)),
                value=5,
                step=1
            )

            df_trades_display = df_trades_stock.head(num_trades)

            st.markdown('<div class="trade-container">', unsafe_allow_html=True)

            prev_price = None
            for idx, row in df_trades_display.iterrows():
                time_str = row["trade_timestamp"].strftime("%H:%M:%S")
                price = row["price"]
                volume = row["volume"]

                # Xác định màu giá
                price_class = ""
                if prev_price is not None:
                    if price > prev_price:
                        price_class = "up"
                    elif price < prev_price:
                        price_class = "down"

                st.markdown(f"""
                <div class="trade-item">
                    <div>
                        <div class="trade-time">{time_str}</div>
                        <div class="trade-price {price_class}">{price:,.2f}</div>
                    </div>
                    <div class="trade-volume">{volume:,}</div>
                </div>
                """, unsafe_allow_html=True)

                prev_price = price

            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            col_s1, col_s2 = st.columns(2)
            with col_s1:
                st.metric("📝 Số lệnh", f"{len(df_trades_display):,}")
                st.metric("🔺 Cao nhất", f"{df_trades_display['price'].max():,.2f}")
            with col_s2:
                st.metric("📦 Tổng KL", f"{df_trades_display['volume'].sum() / 1e6:.1f}M")
                st.metric("🔻 Thấp nhất", f"{df_trades_display['price'].min():,.2f}")
        else:
            st.info("Không có dữ liệu giao dịch realtime cho mã này")