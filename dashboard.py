import streamlit as st
import os, json, sqlite3, io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="全球股市特徵引擎", layout="wide")

# --- 1. 固定變數定義 ---
TARGET_DB = "tw_stock_warehouse.db"

# --- 2. Google Drive 服務初始化 ---
def get_gdrive_service():
    if "GDRIVE_SERVICE_ACCOUNT" not in st.secrets:
        st.error("❌ Secrets 中缺少 GDRIVE_SERVICE_ACCOUNT")
        return None
    try:
        info = json.loads(st.secrets["GDRIVE_SERVICE_ACCOUNT"])
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"❌ 服務初始化失敗: {e}")
        return None

def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    with st.spinner(f'🚀 正在從雲端同步 {file_name}...'):
        while done is False:
            _, done = downloader.next_chunk()
    return True

# --- 3. 側邊欄：策略篩選條件 ---
st.sidebar.header("📊 選股策略條件")

year = st.sidebar.selectbox("選擇年份", [2024, 2025], index=1)
if year == 2025:
    month = st.sidebar.selectbox("選擇月份", list(range(1, 12)), index=0)
else:
    month = st.sidebar.selectbox("選擇月份", list(range(1, 13)), index=0)

strategy_type = st.sidebar.selectbox(
    "技術指標策略", 
    ["無", "KD 黃金交叉", "MACD 柱狀圖轉正", "均線多頭排列(MA20>MA60)"]
)

reward_period = st.sidebar.selectbox(
    "評估未來報酬區間", 
    ["1-5", "6-10", "11-20"]
)
up_col = f"up_{reward_period}"
down_col = f"down_{reward_period}"

use_divergence = st.sidebar.checkbox("開啟底部背離過濾")
div_type = "無"
if use_divergence:
    div_type = st.sidebar.radio("選擇背離指標", ["MACD 底部背離", "KD 底部背離"])

# --- 4. 主程式邏輯 ---
st.title("🌐 全球股市特徵引擎 - 策略篩選中心")

service = get_gdrive_service()

if service:
    if not os.path.exists(TARGET_DB):
        folder_id = st.secrets["GDRIVE_FOLDER_ID"]
        query = f"'{folder_id}' in parents and name = '{TARGET_DB}' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files:
            download_file(service, files[0]['id'], TARGET_DB)
        else:
            st.error(f"❌ 雲端找不到 {TARGET_DB}")

    if os.path.exists(TARGET_DB):
        try:
            conn = sqlite3.connect(TARGET_DB)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"
            
            # 讀取資料
            query = f"SELECT * FROM stock_analysis WHERE date BETWEEN '{start_date}' AND '{end_date}'"
            df = pd.read_sql(query, conn)
            conn.close()

            # --- 執行 Python 層級過濾 ---
            if strategy_type == "KD 黃金交叉":
                df = df[df['kd_gold'] == 1]
            elif strategy_type == "MACD 柱狀圖轉正":
                df = df[df['macdh_slope'] > 0]
            elif strategy_type == "均線多頭排列(MA20>MA60)":
                df = df[df['ma20'] > df['ma60']]

            if div_type == "MACD 底部背離":
                df = df[df['macd_bottom_div'] == 1]
            elif div_type == "KD 底部背離":
                df = df[df['kd_bottom_div'] == 1]

            # --- 顯示結果表格 ---
            st.subheader(f"🚀 {year}年{month}月 符合訊號標的 (共 {len(df)} 筆)")
            
            if not df.empty:
                def make_wantgoo_link(symbol):
                    clean_id = str(symbol).split('.')[0]
                    return f"https://www.wantgoo.com/stock/{clean_id}/technical-chart"

                # 💡 欄位定義與排序：左側(結果) -> 右側(特徵分析)
                # 我們將斜率放最後，這不是主要用來看的，是用來做特徵研究
                feature_cols = ['ma20_slope', 'ma60_slope', 'macdh_slope']
                core_cols = ['date', 'symbol', 'close', 'ytd_ret', up_col, down_col]
                
                # 自動過濾掉資料庫中不存在的欄位
                available_cols = [c for c in core_cols + feature_cols if c in df.columns]
                
                res_df = df[available_cols].copy()
                res_df['分析'] = res_df['symbol'].apply(make_wantgoo_link)

                # 使用 Data Editor 顯示 (自定義中文化欄位)
                st.data_editor(
                    res_df,
                    column_config={
                        "date": "訊號日期",
                        "symbol": "股票代號",
                        "close": st.column_config.NumberColumn("收盤價", format="%.2f"),
                        "ytd_ret": st.column_config.NumberColumn("YTD 實測漲幅 (%)", format="%.2f%%"),
                        up_col: st.column_config.NumberColumn(f"未來最大漲幅(%)", format="%.2f%%"),
                        down_col: st.column_config.NumberColumn(f"未來最大跌幅(%)", format="%.2f%%"),
                        "ma20_slope": st.column_config.NumberColumn("MA20斜率", format="%.4f"),
                        "ma60_slope": st.column_config.NumberColumn("MA60斜率", format="%.4f"),
                        "macdh_slope": st.column_config.NumberColumn("MACD動能速度", format="%.4f"),
                        "分析": st.column_config.LinkColumn("玩股網", display_text="開圖"),
                    },
                    hide_index=True,
                    use_container_width=True
                )
                
                # 平均統計
                avg_up = res_df[up_col].mean()
                avg_down = res_df[down_col].mean()
                st.info(f"💡 本次篩選平均表現：最大潛在漲幅 {avg_up:.2f}% | 最大潛在跌幅 {avg_down:.2f}%")

            else:
                st.info("💡 此條件下查無資料，請放寬篩選標準。")

        except Exception as e:
            st.error(f"❌ 數據讀取失敗: {e}")

# --- 5. 報酬分布分箱統計 ---
if not df.empty and (up_col in df.columns):
    st.divider()
    st.header(f"📊 策略報酬分布統計 (未來 {reward_period} 天)")

    # 這裡的統計直接使用原始過濾後的 res_df
    bins = list(range(-100, 105, 5)) + [float('inf')]
    labels = [f"{i}%~{i+5}%" for i in range(-100, 100, 5)] + [">100%"]

    def get_bin_stats(data, column_name):
        if column_name not in data.columns: return pd.DataFrame()
        counts = pd.cut(data[column_name], bins=bins, labels=labels, right=False).value_counts().sort_index()
        stats_df = counts.reset_index()
        stats_df.columns = ['區間', '家數']
        total = stats_df['家數'].sum()
        stats_df['比例'] = stats_df['家數'].apply(lambda x: f"{(x/total*100):.2f}%" if total > 0 else "0.00%")
        return stats_df[stats_df['家數'] > 0]

    col_stats_up, col_stats_down = st.columns(2)

    with col_stats_up:
        st.subheader("📈 最大漲幅分布")
        up_stats = get_bin_stats(res_df, up_col)
        if not up_stats.empty:
            st.bar_chart(up_stats.set_index('區間')['家數'], color="#2ecc71")
            st.table(up_stats)

    with col_stats_down:
        st.subheader("📉 最大跌幅分布")
        down_stats = get_bin_stats(res_df, down_col)
        if not down_stats.empty:
            st.bar_chart(down_stats.set_index('區間')['家數'], color="#e74c3c")
            st.table(down_stats)

with st.expander("💡 什麼是「特徵欄位分析」？"):
    st.write("""
    特徵分析是將價格轉化為可量化的數學狀態。在本表格右側：
    1. **MA20/60 斜率**：描述趨勢的慣性，正值代表動能向上。
    2. **MACD 動能速度**：描述能量的變化，由負轉正代表空方力道衰竭。
    透過觀察這些特徵與左側「未來報酬」的關聯，讀者可以學習歸納出高勝率的選股模式。
    """)
