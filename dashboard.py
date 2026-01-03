import streamlit as st
import os, json, sqlite3, io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="全球股市特徵引擎", layout="wide")

# --- 1. 固定變數定義 (防止 NameError) ---
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

# A. 年份與月份 (限定 2025 到 11月)
year = st.sidebar.selectbox("選擇年份", [2024, 2025], index=1)
if year == 2025:
    month = st.sidebar.selectbox("選擇月份", list(range(1, 12)), index=0)
else:
    month = st.sidebar.selectbox("選擇月份", list(range(1, 13)), index=0)

# B. 技術指標策略
strategy_type = st.sidebar.selectbox(
    "技術指標策略", 
    ["無", "KD 黃金交叉", "MACD 柱狀圖轉正", "均線多頭排列(MA20>MA60)"]
)

# C. 未來報酬目標
reward_target = st.sidebar.selectbox(
    "評估未來報酬區間", 
    ["up_1-5", "up_6-10", "up_11-20"]
)

# D. 背離條件 (可選)
use_divergence = st.sidebar.checkbox("開啟底部背離過濾")
div_type = "無"
if use_divergence:
    div_type = st.sidebar.radio("選擇背離指標", ["MACD 底部背離", "KD 底部背離"])

# --- 4. 主程式邏輯 ---
st.title("🌐 全球股市特徵引擎 - 策略篩選中心")

service = get_gdrive_service()

if service:
    # 確保資料庫存在
    if not os.path.exists(TARGET_DB):
        folder_id = st.secrets["GDRIVE_FOLDER_ID"]
        query = f"'{folder_id}' in parents and name = '{TARGET_DB}' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files:
            download_file(service, files[0]['id'], TARGET_DB)
        else:
            st.error(f"❌ 雲端找不到 {TARGET_DB}")

    # 開始查詢數據
    if os.path.exists(TARGET_DB):
        try:
            conn = sqlite3.connect(TARGET_DB)
            
            # 建立 SQL 查詢 (動態日期)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"
            
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
                # 建立玩股網超連結
                def make_wantgoo_link(symbol):
                    clean_id = str(symbol).split('.')[0]
                    return f"https://www.wantgoo.com/stock/{clean_id}/technical-chart"

                # 整理顯示欄位
                res_df = df[['date', 'symbol', 'close', 'ma20_slope', reward_target]].copy()
                res_df['玩股網'] = res_df['symbol'].apply(make_wantgoo_link)

                # 使用 Data Editor 顯示超連結
                st.data_editor(
                    res_df,
                    column_config={
                        "玩股網": st.column_config.LinkColumn("查看線圖", display_text="點我開圖"),
                        "close": st.column_config.NumberColumn("收盤價", format="%.2f"),
                        "ma20_slope": st.column_config.NumberColumn("MA20斜率", format="%.4f"),
                        reward_target: st.column_config.ProgressColumn("未來報酬", format="%.2f", min_value=-0.2, max_value=0.2)
                    },
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("💡 此條件下查無資料，請放寬篩選標準。")

        except Exception as e:
            st.error(f"❌ 數據讀取失敗: {e}")
