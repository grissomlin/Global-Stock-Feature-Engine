import streamlit as st
import sqlite3
import pandas as pd

# --- 側邊欄：篩選條件 ---
st.sidebar.header("🔍 選股策略條件")

# 1. 年份與月份篩選
year = st.sidebar.selectbox("選擇年份", [2024, 2025], index=1)
if year == 2025:
    month = st.sidebar.selectbox("選擇月份", list(range(1, 12))) # 2025 只提供 1-11 月
else:
    month = st.sidebar.selectbox("選擇月份", list(range(1, 13)))

# 2. 技術指標交叉策略
strategy_type = st.sidebar.selectbox(
    "技術指標策略", 
    ["無", "KD 黃金交叉", "MACD 柱狀圖轉正", "均線多頭排列(MA20>MA60)"]
)

# 3. 未來報酬目標選單
reward_target = st.sidebar.selectbox(
    "未來報酬評估區間", 
    ["up_1-5", "up_6-10", "up_11-20"]
)

# 4. 背離條件 (可選)
use_divergence = st.sidebar.checkbox("開啟背離過濾 (Divergence)")
div_type = "無"
if use_divergence:
    div_type = st.sidebar.radio("選擇背離指標", ["MACD 底部背離", "KD 底部背離"])

# --- 資料處理與查詢 ---
def fetch_filtered_data(db_path, y, m, strat, reward, div):
    conn = sqlite3.connect(db_path)
    # 格式化日期範圍
    start_dt = f"{y}-{m:02d}-01"
    # 簡單計算月底 (可用 pd.offsets 但此處手寫簡化)
    end_dt = f"{y}-{m:02d}-31"
    
    query = f"SELECT * FROM stock_analysis WHERE date BETWEEN '{start_dt}' AND '{end_dt}'"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # 執行過濾邏輯
    if strat == "KD 黃金交叉":
        df = df[df['kd_gold'] == 1]
    elif strat == "MACD 柱狀圖轉正":
        df = df[df['macdh_slope'] > 0]
    elif strat == "均線多頭排列(MA20>MA60)":
        df = df[df['ma20'] > df['ma60']]
        
    if div == "MACD 底部背離":
        df = df[df['macd_bottom_div'] == 1]
    elif div == "KD 底部背離":
        df = df[df['kd_bottom_div'] == 1]
        
    return df

# --- 顯示結果 ---
st.header(f"🚀 篩選結果: {year}年{month}月")
filtered_df = fetch_filtered_data(TARGET_DB, year, month, strategy_type, reward_target, div_type)

if filtered_df.empty:
    st.info("💡 目前條件下沒有符合的股票，請嘗試放寬篩選條件。")
else:
    # 整理顯示表格
    display_df = filtered_df[['date', 'symbol', 'close', 'ma20_slope', reward_target]].copy()
    
    # 💡 建立玩股網超連結 (WantGoo)
    # 台灣股票代號通常是 2330.TW，需要去掉 .TW
    def make_wantgoo_link(symbol):
        clean_symbol = str(symbol).split('.')[0]
        url = f"https://www.wantgoo.com/stock/{clean_symbol}/technical-chart"
        return url

    display_df['分析連結'] = display_df['symbol'].apply(make_wantgoo_link)
    
    # 使用 Streamlit 的 link column 功能
    st.data_editor(
        display_df,
        column_config={
            "分析連結": st.column_config.LinkColumn(
                "玩股網圖表",
                help="點擊前往技術線圖",
                validate=r"^https://.*",
                max_chars=100,
            )
        },
        hide_index=True,
        use_container_width=True
    )

    st.success(f"✅ 找到 {len(display_df)} 筆符合訊號的資料")
