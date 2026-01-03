# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from dotenv import load_dotenv

# 💡 載入環境變數
load_dotenv() 
socket.setdefaulttimeout(600)

# 💡 環境變數讀取
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')

# 💡 導入特徵加工模組
try:
    from processor import process_market_data
except ImportError:
    print("⚠️ 系統提示：找不到 processor.py，將跳過特徵工程。")
    process_market_data = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== Google Drive 服務函式 (純環境變數版) ==========

def get_drive_service():
    """從環境變數讀取 JSON 憑證並初始化 Google Drive 服務"""
    # 優先從環境變數 GDRIVE_SERVICE_ACCOUNT 讀取
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=['https://www.googleapis.com/auth/drive']
            )
            return build('drive', 'v3', credentials=creds, cache_discovery=False)
        else:
            print("❌ 錯誤：找不到環境變數 GDRIVE_SERVICE_ACCOUNT")
            return None
    except Exception as e:
        print(f"❌ Drive 服務初始化失敗: {e}")
        return None

def download_db_from_drive(service, file_name):
    """從雲端下載資料庫檔案"""
    if not GDRIVE_FOLDER_ID: return False
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if not items: return False
        
        file_id = items[0]['id']
        print(f"📡 從雲端同步舊檔案: {file_name}")
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(file_name, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            done = False
            while not done: _, done = downloader.next_chunk()
        return True
    except: return False

def upload_db_to_drive(service, file_path):
    """將更新後的資料庫同步回雲端"""
    if not GDRIVE_FOLDER_ID or not os.path.exists(file_path): return False
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='application/x-sqlite3', resumable=True)
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if items:
            service.files().update(fileId=items[0]['id'], media_body=media).execute()
        else:
            meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=meta, media_body=media).execute()
        print(f"✅ 雲端同步完成: {file_name}")
        return True
    except Exception as e:
        print(f"⚠️ {file_name} 同步失敗: {e}")
        return False

# ========== 主程式邏輯 ==========

def main():
    # 支援參數：all, tw, us, cn, hk, jp, kr
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else 'all'
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn, 
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    
    markets_to_run = [target_market] if target_market in module_map else list(module_map.keys())
    
    # 初始化 Drive 服務
    service = get_drive_service()

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        print(f"\n--- 🚀 市場啟動: {m.upper()} ---")

        # 1. 嘗試下載雲端現有資料庫 (加速更新)
        if service:
            download_db_from_drive(service, db_file)
            # 💡 針對韓股下載額外清單 (如有需要)
            if m == 'kr':
                download_db_from_drive(service, "kr_list_all.csv")

        # 2. 執行各國下載器
        target_module = module_map.get(m)
        if target_module:
            print(f"📡 抓取最新行情數據...")
            target_module.run_sync(start_date="2024-01-01", end_date="2025-12-31")
        
        # 3. 執行特徵工程加工 (MA斜率, MACD背離, 未來報酬等)
        if process_market_data and os.path.exists(db_file):
            print(f"🧪 執行特徵工程加工...")
            process_market_data(db_file)
        
        # 4. 優化資料庫並回傳雲端
        if service and os.path.exists(db_file):
            print(f"🧹 優化資料庫並同步至雲端...")
            try:
                conn = sqlite3.connect(db_file)
                conn.execute("VACUUM")
                conn.close()
                upload_db_to_drive(service, db_file)
            except Exception as e:
                print(f"❌ 雲端同步失敗: {e}")

    print("\n✅ 所有選定市場處理完畢。")

if __name__ == "__main__":
    main()
