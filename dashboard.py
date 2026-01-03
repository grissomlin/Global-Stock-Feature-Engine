import streamlit as st
import os
import json
import sqlite3
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

st.title("🌐 全球股市特徵引擎 - 系統診斷")

# --- 診斷步驟 ---
def run_diagnostics():
    st.header("🔍 系統連線診斷")
    
    # 步驟 1: 檢查 Secrets 是否存在
    if "GDRIVE_SERVICE_ACCOUNT" not in st.secrets or "GDRIVE_FOLDER_ID" not in st.secrets:
        st.error("❌ 診斷失敗: Streamlit Secrets 中缺少必要變數 (GDRIVE_SERVICE_ACCOUNT 或 GDRIVE_FOLDER_ID)")
        return None, None

    # 步驟 2: 嘗試初始化 Google Drive 服務
    try:
        info = json.loads(st.secrets["GDRIVE_SERVICE_ACCOUNT"])
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        service = build('drive', 'v3', credentials=creds)
        st.success("✅ Google Drive 服務初始化成功 (金鑰有效)")
    except Exception as e:
        st.error(f"❌ 診斷失敗: 無法驗證 Google 憑證。原因: {e}")
        return None, None

    # 步驟 3: 嘗試列出資料夾內容
    folder_id = st.secrets["GDRIVE_FOLDER_ID"]
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        
        if not files:
            st.warning(f"⚠️ 警告: 連線成功，但該資料夾 (ID: {folder_id}) 是空的，或者裡面沒有任何檔案。")
            return service, []
        
        # 過濾出資料庫檔案
        db_files = [f for f in files if f['name'].endswith('_stock_warehouse.db')]
        if not db_files:
            st.warning(f"⚠️ 警告: 資料夾內有 {len(files)} 個檔案，但沒有任何以 '_stock_warehouse.db' 結尾的資料庫檔案。")
            st.write("資料夾內的檔案清單：", [f['name'] for f in files])
        else:
            st.success(f"✅ 成功找到 {len(db_files)}個資料庫檔案！")
            
        return service, db_files

    except Exception as e:
        st.error(f"❌ 診斷失敗: 無法存取資料夾。請檢查 Folder ID 是否正確，以及該資料夾是否有分享給 Service Account。")
        st.info(f"您的 Service Account Email 為: {info.get('client_email')}")
        return None, None

# 執行診斷並取得檔案清單
service, online_db_list = run_diagnostics()

# --- 如果有檔案，提供下載按鈕 ---
if online_db_list:
    st.divider()
    st.subheader("📥 雲端檔案同步")
    selected_to_download = st.multiselect("選擇要下載到儀表板環境的檔案", [f['name'] for f in online_db_list])
    
    if st.button("開始下載檔案"):
        # 這裡放入你之前的 download_db_from_drive 邏輯
        st.info("下載功能執行中...")
