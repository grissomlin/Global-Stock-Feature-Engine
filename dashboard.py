import streamlit as st
import os, json, sqlite3, io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="全球股市特徵引擎 - 數據中心", layout="wide")

# --- 1. 配置與服務初始化 ---
def get_gdrive_service():
    if "GDRIVE_SERVICE_ACCOUNT" not in st.secrets:
        st.error("❌ Secrets 中缺少 GDRIVE_SERVICE_ACCOUNT 設定")
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

# --- 2. 下載邏輯 ---
def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    with st.spinner(f'🚀 正在從雲端同步 {file_name}...'):
        while done is False:
            status, done = downloader.next_chunk()
    return True

# --- 3. 主程式介面 ---
st.title("🇹🇼 台灣市場數據掃描 (自動同步)")

service = get_gdrive_service()

if service:
    folder_id = st.secrets["GDRIVE_FOLDER_ID"]
    try:
        # 💡 核心修正：在這裡定義 online_db_list
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        online_db_list = results.get('files', [])
        
        # 設定預設下載目標
        TARGET_DB = "tw_stock_warehouse.db"
        
        # 如果本地沒有檔案，自動下載
        if not os.path.exists(TARGET_DB):
            tw_file = next((f for f in online_db_list if f['name'] == TARGET_DB), None)
            if tw_file:
                download_file(service, tw_file['id'], TARGET_DB)
                st.success(f"✅ {TARGET_DB} 已成功同步至本地")
            else:
                st.warning(f"⚠️ 雲端資料夾中找不到 {TARGET_DB}")

        # --- 4. 數據表結構檢查 ---
        if os.path.exists(TARGET_DB):
            conn = sqlite3.connect(TARGET_DB)
            # 獲取所有資料表名稱
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
            
            # 優先讀取加工後的分析表
            target_table = 'stock_analysis' if 'stock_analysis' in tables else 'stock_prices'
            
            st.divider()
            st.header(f"📊 目前資料表：`{target_table}`")
            
            # 讀取數據樣例
            df_sample = pd.read_sql(f"SELECT * FROM {target_table} LIMIT 20", conn)
            
            col1, col2 = st.columns([1, 3])
            
            with col1:
                st.subheader("📌 欄位清單")
                # 顯示所有欄位名稱及其資料型態
                schema_info = pd.read_sql(f"PRAGMA table_info({target_table})", conn)
                st.dataframe(schema_info[['name', 'type']], height=400)
            
            with col2:
                st.subheader("💡 數據內容預覽 (Top 20)")
                st.dataframe(df_sample, use_container_width=True)
            
            # 5. 快速特徵檢查指標
            st.subheader("🛠️ 特徵工程檢查點")
            cols = df_sample.columns.tolist()
            indicators = {
                "均線與斜率": ["ma20", "ma20_slope"],
                "MACD 背離": ["macd", "macdh", "macd_bottom_div"],
                "KD 訊號": ["k", "d", "kd_gold"],
                "預測標籤": ["up_1-5", "up_6-10"]
            }
            
            metrics_cols = st.columns(len(indicators))
            for i, (name, fields) in enumerate(indicators.items()):
                found = [f for f in fields if f in cols]
                if len(found) == len(fields):
                    metrics_cols[i].success(f"{name}: OK")
                elif len(found) > 0:
                    metrics_cols[i].warning(f"{name}: 部分遺漏")
                else:
                    metrics_cols[i].error(f"{name}: 未發現")

            conn.close()

    except Exception as e:
        st.error(f"❌ 存取雲端資料夾失敗: {e}")
