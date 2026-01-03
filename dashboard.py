import streamlit as st
import os, json, sqlite3, io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="全球股市特徵引擎", layout="wide")

# --- 1. 初始化 Google Drive 服務 ---
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

# --- 2. 下載函式 ---
def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    with st.spinner(f'正在從雲端同步 {file_name}...'):
        while done is False:
            status, done = downloader.next_chunk()
    return True

# --- 3. 核心邏輯 ---
st.title("🌐 全球股市特徵引擎 - 數據中心")

service = get_gdrive_service()

if service:
    folder_id = st.secrets["GDRIVE_FOLDER_ID"]
    try:
        # 💡 這行定義了 online_db_list
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        online_db_list = results.get('files', [])
        
        # 🎯 預設台灣市場
        TARGET_DB = "tw_stock_warehouse.db"
        
        if not os.path.exists(TARGET_DB):
            tw_file = next((f for f in online_db_list if f['name'] == TARGET_DB), None)
            if tw_file:
                download_file(service, tw_file['id'], TARGET_DB)
                st.success(f"✅ {TARGET_DB} 下載完成")
            else:
                st.warning(f"⚠️ 雲端暫無 {TARGET_DB}")

        # 4. 讀取與顯示資料
        if os.path.exists(TARGET_DB):
            conn = sqlite3.connect(TARGET_DB)
            # 檢查表格
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
            target_table = 'stock_analysis' if 'stock_analysis' in tables else 'stock_prices'
            
            st.header(f"🇹🇼 台灣市場數據掃描：`{target_table}`")
            
            # 抓取 Schema
            df_sample = pd.read_sql(f"SELECT * FROM {target_table} LIMIT 10", conn)
            
            c1, c2 = st.columns([1, 2])
            with c1:
                st.subheader("📌 欄位 (Features)")
                st.write(df_sample.columns.tolist())
            with c2:
                st.subheader("💡 數據預覽")
                st.dataframe(df_sample, use_container_width=True)
            
            conn.close()

    except Exception as e:
        st.error(f"❌ 讀取雲端清單失敗: {e}")
